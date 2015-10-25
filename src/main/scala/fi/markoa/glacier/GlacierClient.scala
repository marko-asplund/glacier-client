package fi.markoa.glacier

import java.time.ZonedDateTime
import java.io.{InputStream, File, FileWriter}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicBoolean}
import scala.concurrent.{Future, Promise, Await}
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}
import scala.collection.JavaConversions._
import scala.io.Source
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.glacier.AmazonGlacierClient
import com.amazonaws.services.glacier.model._
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager
import com.amazonaws.services.sns.AmazonSNSClient
import com.amazonaws.services.sqs.AmazonSQSClient
import com.amazonaws.event.{ProgressListener => AWSProgressListener, ProgressEvent, ProgressEventType}
import argonaut._, Argonaut._

case class Vault(creationDate: ZonedDateTime, lastInventoryDate: Option[ZonedDateTime],
                 numberOfArchives: Long, sizeInBytes: Long, vaultARN: String,
                 vaultName: String)

case class Job(id: String, vaultARN: String, action: String, description: String, creationDate: ZonedDateTime, statusCode: String, statusMessage: String, completionDate: Option[ZonedDateTime], archiveId: Option[String])

case class JobOutput(archiveDescription: String, contentType: String, checksum: String, status: Int, output: InputStream)

case class Archive(id: String, location: Option[String], checksum: Option[String], description: Option[String])

case class AWSInventory(vaultARN: String, inventoryDate: ZonedDateTime, archives: List[AWSArchive])
case class AWSArchive(id: String, description: String, created: ZonedDateTime, size: Long, treeHash: String)

object DateTimeOrdering extends Ordering[ZonedDateTime] {
  def compare(a: ZonedDateTime, b: ZonedDateTime) = a compareTo b
}

object DataTransferEventType extends Enumeration {
  type DataTransferEventType = Value
  val TransferProgress, TransferCompleted, TransferCanceled, TransferFailed = Value
}

object Converters {
  def parseOptDate(ds: String) = Option(ds).map(ZonedDateTime.parse(_))

  def asVault(v: DescribeVaultOutput) =
    Vault(ZonedDateTime.parse(v.getCreationDate), parseOptDate(v.getLastInventoryDate), v.getNumberOfArchives, v.getSizeInBytes, v.getVaultARN, v.getVaultName)

  def vaultResultasVault(v: DescribeVaultResult) =
    Vault(ZonedDateTime.parse(v.getCreationDate), parseOptDate(v.getLastInventoryDate), v.getNumberOfArchives, v.getSizeInBytes, v.getVaultARN, v.getVaultName)

  def asJob(j: GlacierJobDescription) =
    Job(j.getJobId, j.getVaultARN, j.getAction, j.getJobDescription, ZonedDateTime.parse(j.getCreationDate), j.getStatusCode, j.getStatusMessage, parseOptDate(j.getCompletionDate), Option(j.getArchiveId))

  def asJobOutput(o: GetJobOutputResult) =
    JobOutput(o.getArchiveDescription, o.getContentType, o.getChecksum, o.getStatus, o.getBody)

  def asArchive(a: UploadArchiveResult) =
    Archive(a.getArchiveId, Some(a.getLocation), Some(a.getChecksum), None)

  def fromAWSArchive(am: AWSArchive) = Archive(am.id, None, Some(am.treeHash), Some(am.description))

  implicit def ArchiveCodecJson = 
    casecodec4(Archive.apply, Archive.unapply)("archiveId", "location", "checksum", "description")

  implicit def DateTimeCodecJson: CodecJson[ZonedDateTime] = CodecJson(
    (d: ZonedDateTime) => jString(d.toString),
    c => for (s <- c.as[String]) yield ZonedDateTime.parse(s)
  )
  implicit def AWSArchiveCodecJson =
    casecodec5(AWSArchive.apply, AWSArchive.unapply)("ArchiveId", "ArchiveDescription", "CreationDate",
      "Size", "SHA256TreeHash")
  implicit def AWSInventoryCodecJson =
    casecodec3(AWSInventory.apply, AWSInventory.unapply)("VaultARN", "InventoryDate", "ArchiveList")
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
    val i = Parse.decodeOption[AWSInventory](Source.fromInputStream(o.output).mkString)
    o.output.close
    i
  }

  def getLatestVaultInventory(vaultName: String): Option[AWSInventory] =
    listJobs(vaultName).filter(j => j.completionDate.isDefined && j.statusCode == "Succeeded").
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

  import DataTransferEventType._
  private def awsUploadProgressListener(bytesToTransfer: Double, tickInterval: Int,
                 listener: (DataTransferEventType, String, Option[Int]) => Unit): AWSProgressListener = {
    import ProgressEventType._
    new AWSProgressListener {
      val bytesTransferred = new AtomicLong
      val prevPctMark = new AtomicInteger
      val transferring = new AtomicBoolean
      def progressChanged(ev: ProgressEvent): Unit = ev.getEventType match {
        case REQUEST_CONTENT_LENGTH_EVENT =>
        case HTTP_REQUEST_STARTED_EVENT => transferring.compareAndSet(false, true)
        case HTTP_RESPONSE_COMPLETED_EVENT => transferring.compareAndSet(true, false)
        case HTTP_REQUEST_CONTENT_RESET_EVENT => bytesTransferred.addAndGet(ev.getBytesTransferred)
        case REQUEST_BYTE_TRANSFER_EVENT =>
          val s = bytesTransferred.addAndGet(ev.getBytesTransferred)
          if (transferring.get) {
            val pct = ((s/bytesToTransfer)*100).toInt
            if ( (pct/tickInterval) > prevPctMark.get) {
              prevPctMark.set(pct/tickInterval)
              listener(TransferProgress, s"transfer progress: $pct% (bytes: ${bytesTransferred.get})", Some(pct))
            }
          }
        case TRANSFER_COMPLETED_EVENT => listener(TransferCompleted, s"transfer completed", None)
        case TRANSFER_CANCELED_EVENT => listener(TransferCanceled, s"transfer canceled", None)
        case TRANSFER_FAILED_EVENT => listener(TransferFailed, s"transfer failed", None)
        case _ =>
      }
    }
  }

  def uploadArchive(vaultName: String, archiveDescription: String, sourceFile: String): Archive =
    Await.result(uploadArchive(vaultName, archiveDescription, sourceFile,
                  (evType: DataTransferEventType, msg: String, pct: Option[Int]) => println(s"$msg")), Duration.Inf)

  def uploadArchive(vaultName: String, archiveDescription: String, sourceFile: String,
                    transferListener: (DataTransferEventType, String, Option[Int]) => Unit): Future[Archive] = {
    val awsListener = awsUploadProgressListener((new File(sourceFile)).length.toDouble, 5, transferListener)
    val atm = new ArchiveTransferManager(client, credentials)
    val archivePromise = Promise[Archive]()
    Try(atm.upload("-", vaultName, archiveDescription, new File(sourceFile), awsListener)) match {
      case Success(r) =>
        logger.info(s"archive uploaded: $vaultName, ${r.getArchiveId}")
        val archive = Archive(r.getArchiveId, Some(sourceFile), None, Some(archiveDescription))
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

  def downloadArchive(vaultName: String, archiveId: String, targetFile: String): Unit =
    new ArchiveTransferManager(client, sqs, sns).
      download("-", vaultName, archiveId, new File(targetFile), awsDownloadProgressListener("archive", archiveId))

  // merge with awsUploadProgressListener ?
  private def awsDownloadProgressListener(objType: String, objId: String): AWSProgressListener = {
    import ProgressEventType._
    new AWSProgressListener {
      def progressChanged(ev: ProgressEvent): Unit = ev.getEventType match {
        case REQUEST_BYTE_TRANSFER_EVENT =>
        case TRANSFER_COMPLETED_EVENT =>
          println(s"$objType download completed: $objId")
        case TRANSFER_CANCELED_EVENT =>
          println(s"$objType download canceled: $objId")
        case TRANSFER_FAILED_EVENT =>
          println(s"$objType download failed: $objId")
        case _ => println(s"ev: $ev") // debugging
      }
    }
  }

  def requestArchiveRetrieval(vaultName: String, archiveId: String): Option[String] =
    initiateJob(vaultName, new JobParameters().withType("archive-retrieval").withArchiveId(archiveId))

  def downloadJobOutput(vaultName: String, jobId: String, targetFile: String): Unit =
    new ArchiveTransferManager(client, sqs, sns).
      downloadJobOutput("-", vaultName, jobId, new File(targetFile), awsDownloadProgressListener("jobOutput", jobId))

  // ========= other =========

  def shutdown = client.shutdown

}

object GlacierClient {
  def apply(region: Regions, credentials: AWSCredentialsProvider) = new GlacierClient(region, credentials)
  def apply(region: Regions): GlacierClient = apply(region, new ProfileCredentialsProvider)
  def apply(region: String): GlacierClient = apply(Regions.valueOf(region))
  def apply(): GlacierClient = apply(Regions.EU_WEST_1)
}
