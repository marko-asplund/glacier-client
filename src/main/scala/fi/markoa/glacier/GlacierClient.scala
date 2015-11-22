package fi.markoa.glacier

import java.time.{ZonedDateTime}
//import java.time.{ZonedDateTime, LocalDate}
//import java.time.format.DateTimeFormatter
import java.io.{InputStream, File, FileWriter}
import java.util.concurrent.atomic.{AtomicReference, AtomicInteger, AtomicBoolean}
import com.amazonaws.event.ProgressEventType._

import scala.concurrent.{Future, Promise, Await}
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}
import scala.collection.JavaConversions._
import scala.io.Source
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.event.{ProgressListener => AWSProgressListener, ProgressEvent, ProgressEventType}
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.sns.AmazonSNSClient
import com.amazonaws.services.sqs.AmazonSQSClient
import com.amazonaws.services.glacier.{AmazonGlacierClient, TreeHashGenerator}
import com.amazonaws.services.glacier.model._
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager
import argonaut._, Argonaut._


case class Vault(vaultARN: String, vaultName: String,
                 creationDate: ZonedDateTime, lastInventoryDate: Option[ZonedDateTime],
                 numberOfArchives: Long, sizeInBytes: Long)

case class Job(id: String, vaultARN: String, action: String, description: String, creationDate: ZonedDateTime,
               statusCode: String, statusMessage: String, completionDate: Option[ZonedDateTime],
               archiveId: Option[String])

case class JobOutput(archiveDescription: String, contentType: String, checksum: String, status: Int, output: InputStream)

case class Archive(id: String, location: Option[String], sha256TreeHash: String, size: Long, description: Option[String])

case class AWSInventory(vaultARN: String, inventoryDate: ZonedDateTime, archives: List[AWSArchive])
case class AWSArchive(id: String, description: String, created: ZonedDateTime, size: Long, sha256TreeHash: String)

object DateTimeOrdering extends Ordering[ZonedDateTime] {
  def compare(a: ZonedDateTime, b: ZonedDateTime) = a compareTo b
}

object DataTransferEventType extends Enumeration {
  type DataTransferEventType = Value
  val TransferStarted, TransferProgress, TransferCompleted, TransferCanceled, TransferFailed = Value
}

object Converters {
  def parseOptDate(ds: String) = Option(ds).map(ZonedDateTime.parse)

  def asVault(v: DescribeVaultOutput) = Vault(v.getVaultARN, v.getVaultName, ZonedDateTime.parse(v.getCreationDate),
    parseOptDate(v.getLastInventoryDate), v.getNumberOfArchives, v.getSizeInBytes)

  def vaultResultasVault(v: DescribeVaultResult) = Vault(v.getVaultARN, v.getVaultName,
    ZonedDateTime.parse(v.getCreationDate), parseOptDate(v.getLastInventoryDate), v.getNumberOfArchives, v.getSizeInBytes)

  def asJob(j: GlacierJobDescription) = Job(j.getJobId, j.getVaultARN, j.getAction, j.getJobDescription,
    ZonedDateTime.parse(j.getCreationDate), j.getStatusCode, j.getStatusMessage, parseOptDate(j.getCompletionDate),
    Option(j.getArchiveId))

  def asJobOutput(o: GetJobOutputResult) = JobOutput(o.getArchiveDescription, o.getContentType, o.getChecksum,
    o.getStatus, o.getBody)

  def fromAWSArchive(am: AWSArchive) = Archive(am.id, None, am.sha256TreeHash, am.size, Some(am.description))

  implicit def ArchiveCodecJson = casecodec5(Archive.apply, Archive.unapply)("archiveId", "location", "sha256TreeHash",
    "size", "description")

  implicit def DateTimeCodecJson: CodecJson[ZonedDateTime] = CodecJson(
    (d: ZonedDateTime) => jString(d.toString),
    c => for (s <- c.as[String]) yield ZonedDateTime.parse(s)
  )
  implicit def AWSArchiveCodecJson = casecodec5(AWSArchive.apply, AWSArchive.unapply)("ArchiveId", "ArchiveDescription",
    "CreationDate", "Size", "SHA256TreeHash")
  implicit def AWSInventoryCodecJson = casecodec3(AWSInventory.apply, AWSInventory.unapply)("VaultARN", "InventoryDate",
    "ArchiveList")
}

class GlacierClient(regionName: Regions, credentials: AWSCredentialsProvider) {
  import Converters._

  val logger = Logger(LoggerFactory.getLogger(classOf[GlacierClient]))

  val region = Region.getRegion(regionName)
  val client = new AmazonGlacierClient(credentials)
  val sqs = new AmazonSQSClient(credentials)
  val sns = new AmazonSNSClient(credentials)
  client.setRegion(region)
  sqs.setRegion(region)
  sns.setRegion(region)

  val b64enc = java.util.Base64.getEncoder
  val b64dec = java.util.Base64.getDecoder
  implicit val dataTimeOrder = DateTimeOrdering

  val homeDir = new File(sys.props("user.home"), ".glacier-backup")


  // ========= vault management =========

  def describeVault(vaultName: String): Vault = vaultResultasVault(client.describeVault(new DescribeVaultRequest(vaultName)))

  def createVault(vaultName: String): String = {
    val location = client.createVault(new CreateVaultRequest(vaultName)).getLocation
    logger.info(s"vault created: $vaultName, $location")
    location
  }

  def listVaults: Seq[Vault] = client.listVaults(new ListVaultsRequest).getVaultList.map(asVault)

  def deleteVault(vaultName: String): Unit = {
    client.deleteVault(new DeleteVaultRequest(vaultName))
    logger.info(s"vault deleted: $vaultName")
  }

  // ========= local archive catalog management =========

  def catListArchives(vaultName: String): Seq[Archive] = {
    val d = getVaultDir(region, vaultName)
    Option(d.list).getOrElse(Array.empty).flatMap { i =>
      Parse.decodeOption[Archive](Source.fromFile(new File(d, i)).mkString)
    }
  }

  private def getVaultDir(cRegion: Region, vaultName: String) =
    new File(new File(homeDir, cRegion.toString), b64enc.encodeToString(vaultName.getBytes))
  private def getArchiveFile(vaultDir: File, archiveId: String) = new File(vaultDir, archiveId.substring(0, 20))

  def catAddArchive(vaultName: String, arch: Archive): Boolean = {
    val d = getVaultDir(region, vaultName)
    d.mkdirs
    val f = getArchiveFile(d, arch.id)
    Try {
      val w = new FileWriter(f)
      w.write(arch.asJson.spaces2)
      w.close
    } match {
      case Success(_) => true
      case Failure(ex) => false
    }
  }

  def catDeleteArchive(vaultName: String, archiveId: String): Boolean =
    getArchiveFile(getVaultDir(region, vaultName), archiveId).delete

  // ========= Glacier archive inventory management =========
  //https://docs.aws.amazon.com/amazonglacier/latest/dev/retrieving-vault-inventory-java.html

  def requestInventory(vaultName: String): Option[String] =
    initiateJob(vaultName, new JobParameters().withType("inventory-retrieval"))

  private def initiateJob(vaultName: String, jobParams: JobParameters): Option[String] =
    Try(client.initiateJob(new InitiateJobRequest(vaultName, jobParams)).getJobId) match {
      case Success(jobId: String) => Some(jobId)
      case Failure(ex) => { logger.error(s"failed to initiate job ${jobParams.getType} for vault ${vaultName}", ex); None }
    }

  def listJobs(vaultName: String): Seq[Job] =
    client.listJobs(new ListJobsRequest(vaultName)).getJobList.map(asJob)

  def retrieveInventory(vaultName: String, jobId: String): Option[AWSInventory] = {
    val o = getJobOutput(vaultName, jobId)
    val json = Source.fromInputStream(o.output).mkString
    val i = Parse.decodeOption[AWSInventory](json)
    o.output.close
    i
  }

  def getLatestVaultInventory(vaultName: String): Option[AWSInventory] =
    listJobs(vaultName).filter(j => j.action == "InventoryRetrieval" &&
                                 j.completionDate.isDefined && j.statusCode == "Succeeded").
      sortBy(_.completionDate.get) match {
        case h +: t => retrieveInventory(vaultName, h.id)
        case _ => None
      }

  def getJobOutputToFile(vaultName: String, jobId: String, targetFile: String): JobOutput = {
    val o = getJobOutput(vaultName, jobId)
    val w = new FileWriter(targetFile)
    w.write(Source.fromInputStream(o.output).mkString)
    w.close
    o.output.close
    o
  }

  def synchronizeLocalCatalog(vaultName: String) = getLatestVaultInventory(vaultName) match {
    case Some(i) =>
      i.archives.map(a => fromAWSArchive(a)).foreach(a => catAddArchive(vaultName, a))
      logger.info(s"local $vaultName catalog synchronized, inventory date: date: ${i.inventoryDate}")
    case _ =>
  }

  private def getJobOutput(vaultName: String, jobId: String): JobOutput =
    asJobOutput(client.getJobOutput(new GetJobOutputRequest().withVaultName(vaultName).withJobId(jobId)))


  // ========= Glacier archive management =========

  object Progress {

    import DataTransferEventType._

    case class ArchiveTransferState(bytesToTransfer: Option[Long], transferring: Boolean = false,
                                    bytesTransferred: Long = 0)

    type TransferStateMachine = (ArchiveTransferState, ProgressEvent) => ArchiveTransferState
    type TransferProgressReporter = (ArchiveTransferState, ProgressEvent, TransferEventListener) => Unit
    type TransferEventListener = (DataTransferEventType, ArchiveTransferState, String) => Unit

    def uploadStateMachine(state: ArchiveTransferState, event: ProgressEvent): ArchiveTransferState = {
      event.getEventType match {
        case HTTP_REQUEST_STARTED_EVENT => state.copy(transferring = true)
        case HTTP_RESPONSE_COMPLETED_EVENT => state.copy(transferring = false)
        case HTTP_REQUEST_CONTENT_RESET_EVENT =>
          state.copy(bytesTransferred = state.bytesTransferred + event.getBytesTransferred)
        case REQUEST_BYTE_TRANSFER_EVENT => state.copy(bytesTransferred =
          state.bytesTransferred + event.getBytesTransferred)
        case _ => state
      }
    }

    def nowTime =
      new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date)

    def downloadStateMachine(state: ArchiveTransferState, event: ProgressEvent): ArchiveTransferState =
      event.getEventType match {
        case TRANSFER_STARTED_EVENT => state.copy(transferring = true)
        case RESPONSE_BYTE_TRANSFER_EVENT => state.copy(bytesTransferred =
          state.bytesTransferred + event.getBytesTransferred)
        case _ => state
      }

    def bytesBasedProgressReporter(tickInterval: Int): TransferProgressReporter = {
      val prevPctMark = new AtomicInteger
      val warned = new AtomicBoolean

      (state: ArchiveTransferState, event: ProgressEvent, listener: TransferEventListener) => state match {
        case ArchiveTransferState(Some(bytesToTransfer), true, bytesTransferred) =>
          val pct = ((bytesTransferred.toDouble / bytesToTransfer) * 100).toInt
          if ((pct / tickInterval) > prevPctMark.get) {
            prevPctMark.set(pct / tickInterval)
            listener(TransferProgress, state, s"transfer progress: $pct% (bytes: ${bytesTransferred})")
          }
        case ArchiveTransferState(None, _, _) if (!warned.compareAndSet(false, true)) =>
          logger.warn("transfer size not known, relative transfer progress will not be reported")
      }
    }

    //val IsoDateFmt = DateTimeFormatter.ISO_LOCAL_DATE_TIME

    def timeBasedProgressReporter(): TransferProgressReporter = ???

    def awsTransferProgressListenerAdapter(initialState: ArchiveTransferState, sm: TransferStateMachine,
                                                   reporter: TransferProgressReporter,
                                                   listener: TransferEventListener): AWSProgressListener =
      new AWSProgressListener {
        val state = new AtomicReference(initialState)

        def progressChanged(ev: ProgressEvent): Unit = ev.getEventType match {
          case CLIENT_REQUEST_STARTED_EVENT => listener(TransferStarted, state.get, s"transfer started")
          case TRANSFER_COMPLETED_EVENT => listener(TransferCompleted, state.get, s"transfer completed")
          case TRANSFER_CANCELED_EVENT => listener(TransferCanceled, state.get, s"transfer canceled")
          case TRANSFER_FAILED_EVENT => listener(TransferFailed, state.get, s"transfer failed")
          case _ => val currState = state.get
            val st = sm(currState, ev)
            reporter(st, ev, listener)
            state.compareAndSet(currState, st)
        }
      }

    val printProgressListener = (evType: DataTransferEventType, state: ArchiveTransferState, msg: String) =>
      println(s"$evType: $msg")
  }

  import Progress._

  def uploadArchive(vaultName: String, archiveDescription: String, sourceFile: String): Archive =
    Await.result(uploadArchive(vaultName, archiveDescription, sourceFile, Progress.printProgressListener), Duration.Inf)

  def uploadArchive(vaultName: String, archiveDescription: String, sourceFileName: String,
                    transferListener: Progress.TransferEventListener): Future[Archive] = {
    val sourceFile = new File(sourceFileName)
    val hash = TreeHashGenerator.calculateTreeHash(sourceFile)
    val progressListener = awsTransferProgressListenerAdapter(ArchiveTransferState(Some(sourceFile.length)),
      uploadStateMachine, bytesBasedProgressReporter(5), printProgressListener)
    val atm = new ArchiveTransferManager(client, credentials)
    val archivePromise = Promise[Archive]()
    Try(atm.upload("-", vaultName, archiveDescription, sourceFile, progressListener)) match {
      case Success(r) =>
        logger.info(s"archive uploaded: $vaultName, ${r.getArchiveId}")
        val archive = Archive(r.getArchiveId, Some(sourceFileName), hash, sourceFile.length, Some(archiveDescription))
        catAddArchive(vaultName, archive)
        archivePromise.success(archive)
      case Failure(ex) =>
        logger.error("archive upload failed: $vaultName: ${ex}", ex)
        archivePromise.failure(ex)
    }
    archivePromise.future
  }

  def deleteArchive(vaultName: String, archiveId: String): Boolean = {
    client.deleteArchive(new DeleteArchiveRequest(vaultName, archiveId))
    logger.info(s"archive deleted: $vaultName, $archiveId")
    catDeleteArchive(vaultName, archiveId)
  }

  private val DefaultTickInterval = 5
  def downloadArchive(vaultName: String, archiveId: String, targetFile: String): Unit = {
    val archiveSize = catListArchives(vaultName).find(_.id == archiveId).map(_.size)
    val progressListener = awsTransferProgressListenerAdapter(ArchiveTransferState(archiveSize), downloadStateMachine,
      bytesBasedProgressReporter(DefaultTickInterval), printProgressListener)
    logger.info(s"preparing archive $vaultName, $archiveId. this will take several hours")
    new ArchiveTransferManager(client, sqs, sns).
      download("-", vaultName, archiveId, new File(targetFile), progressListener)
  }

  def prepareArchiveRetrieval(vaultName: String, archiveId: String): Option[String] =
    initiateJob(vaultName, new JobParameters().withType("archive-retrieval").withArchiveId(archiveId))

  def downloadPreparedArchive(vaultName: String, jobId: String, targetFile: String): Unit = {
    val archive = listJobs(vaultName).filter(_.action == "ArchiveRetrieval").find(_.id == jobId) flatMap ( j =>
      catListArchives(vaultName).find(_.id == j.archiveId.get)
    )
    val progressListener = awsTransferProgressListenerAdapter(ArchiveTransferState(archive.map(_.size)), downloadStateMachine,
      bytesBasedProgressReporter(DefaultTickInterval), printProgressListener)
    new ArchiveTransferManager(client, sqs, sns).
      downloadJobOutput("-", vaultName, jobId, new File(targetFile), progressListener)
  }

  // ========= other =========

  def shutdown = client.shutdown

}

object GlacierClient {
  def regions = Regions.values
  def apply(region: Regions, credentials: AWSCredentialsProvider) = new GlacierClient(region, credentials)
  def apply(region: Regions): GlacierClient = apply(region, new ProfileCredentialsProvider)
  def apply(region: String): GlacierClient = apply(Regions.fromName(region))
}
