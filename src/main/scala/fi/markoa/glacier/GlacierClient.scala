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

case class AmInventory(vaultARN: String, inventoryDate: ZonedDateTime, archives: List[AmArchive])
case class AmArchive(id: String, description: String, created: ZonedDateTime, size: Long, treeHash: String)

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

  def fromAmArchive(am: AmArchive) = Archive(am.id, None, Some(am.treeHash), Some(am.description))

  implicit def ArchiveCodecJson = 
    casecodec4(Archive.apply, Archive.unapply)("archiveId", "location", "checksum", "description")

  implicit def DateTimeCodecJson: CodecJson[ZonedDateTime] = CodecJson(
    (d: ZonedDateTime) => jString(d.toString),
    c => for (s <- c.as[String]) yield ZonedDateTime.parse(s)
  )
  implicit def AmArchiveCodecJson =
    casecodec5(AmArchive.apply, AmArchive.unapply)("ArchiveId", "ArchiveDescription", "CreationDate",
      "Size", "SHA256TreeHash")
  implicit def AmInventoryCodecJson =
    casecodec3(AmInventory.apply, AmInventory.unapply)("VaultARN", "InventoryDate", "ArchiveList")
}

class GlacierClient(regionName: Regions, credentials: AWSCredentialsProvider) {
  import Converters._

  val logger = Logger(LoggerFactory.getLogger(classOf[GlacierClient]))

  val region = Region.getRegion(regionName)
  val client = new AmazonGlacierClient(credentials)
  client.setRegion(region)

  val b64enc = java.util.Base64.getEncoder
  val b64dec = java.util.Base64.getDecoder
  implicit val dataTimeOrder = DateTimeOrdering

  val homeDir = new File(sys.props("user.home"), ".glacier-backup")


  // ========= vault management =========

  def describeVault(vaultName: String) = vaultResultasVault(client.describeVault(new DescribeVaultRequest(vaultName)))

  def createVault(vaultName: String) = client.createVault(new CreateVaultRequest(vaultName)).getLocation

  def listVaults: Seq[Vault] = client.listVaults(new ListVaultsRequest).getVaultList.map(asVault)

  def deleteVault(vaultName: String) = client.deleteVault(new DeleteVaultRequest(vaultName))


  // ========= local archive catalog management =========

  def catListArchives(vaultName: String): List[Archive] = {
    val d = new File(homeDir, b64enc.encodeToString(vaultName.getBytes))
    d.list.flatMap { i =>
      Parse.decodeOption[Archive](Source.fromFile(new File(d, i)).mkString)
    }.toList
  }

  private def getVaultDir(vaultName: String) = new File(homeDir, b64enc.encodeToString(vaultName.getBytes))
  private def getArchiveFile(vaultDir: File, archiveId: String) = new File(vaultDir, archiveId.substring(0, 20))

  def catAddArchive(vaultName: String, arch: Archive): Boolean = {
    val d = getVaultDir(vaultName)
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

  def catDeleteArchive(vaultName: String, archiveId: String): Boolean = {
    val f = new File(new File(homeDir, b64enc.encodeToString(vaultName.getBytes)), archiveId.substring(0, 20))
    f.delete
  }

  // ========= Glacier archive inventory management =========
  //https://docs.aws.amazon.com/amazonglacier/latest/dev/retrieving-vault-inventory-java.html

  def requestInventory(vaultName: String): Option[String] =
    Try(client.initiateJob(new InitiateJobRequest(vaultName, new JobParameters().withType("inventory-retrieval"))).getJobId) match {
      case Success(jobId: String) => Some(jobId)
      case Failure(ex) => { logger.error(s"failed to get inventory for vault ${vaultName}", ex); None }
    }

  def listJobs(vaultName: String) =
    client.listJobs(new ListJobsRequest(vaultName)).getJobList.map(asJob)

  def retrieveInventory(vaultName: String, jobId: String) = {
    val o = getJobOutput(vaultName, jobId)
    val i = Parse.decodeOption[AmInventory](Source.fromInputStream(o.output).mkString)
    o.output.close
    i
  }

  def getLatestVaultInventory(vaultName: String) =
    listJobs(vaultName).filter(j => j.completionDate.isDefined && j.statusCode == "Succeeded").
      sortBy(_.completionDate.get).toList match {
        case h :: t => retrieveInventory(vaultName, h.id)
        case _ => None
      }

  def getJobOutputToFile(vaultName: String, jobId: String, targetFile: String) = {
    val o = getJobOutput(vaultName, jobId)
    val w = new FileWriter(targetFile)
    w.write(Source.fromInputStream(o.output).mkString)
    w.close
    o.output.close
    o
  }

  def synchronizeLocalCatalog(vaultName: String) = getLatestVaultInventory(vaultName) match {
    case Some(i) => i.archives.map(a => fromAmArchive(a)).foreach(a => catAddArchive(vaultName, a))
    case _ =>
  }

  private def getJobOutput(vaultName: String, jobId: String) =
    asJobOutput(client.getJobOutput(new GetJobOutputRequest().withVaultName(vaultName).withJobId(jobId)))


  // ========= Glacier archive management =========

  import DataTransferEventType._
  def awsUploadProgressListener(bytesToTransfer: Double, tickInterval: Int,
                                  listener: (DataTransferEventType, String, Option[Int]) => Unit) = {
    import ProgressEventType._
    new AWSProgressListener {
      val bytesTransferred = new AtomicLong
      val prevPctMark = new AtomicInteger
      val transferStarted = new AtomicBoolean
      def progressChanged(ev: ProgressEvent): Unit = ev.getEventType match {
        case HTTP_REQUEST_CONTENT_RESET_EVENT => transferStarted.compareAndSet(false, true)
        case REQUEST_BYTE_TRANSFER_EVENT => if (transferStarted.get) {
          val s = bytesTransferred.addAndGet(ev.getBytesTransferred)
          val pct = ((s/bytesToTransfer)*100).toInt
          if ( (pct/tickInterval) > prevPctMark.get) {
            prevPctMark.set(pct/tickInterval)
            listener(TransferProgress, s"transfer progress: $pct%", Some(pct))
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
        val archive = Archive(r.getArchiveId, Some(sourceFile), None, Some(archiveDescription))
        catAddArchive(vaultName, archive)
        archivePromise.success(archive)
      case Failure(ex) =>
        archivePromise.failure(ex)
    }
    archivePromise.future
  }

  def deleteArchive(vaultName: String, archiveId: String) = {
    client.deleteArchive(new DeleteArchiveRequest(vaultName, archiveId))
    catDeleteArchive(vaultName, archiveId)
  }

  def downloadArchive(vaultName: String, archiveId: String, targetFile: String) = {
    import ProgressEventType._
    val sqs = new AmazonSQSClient(credentials)
    sqs.setRegion(region)
    val sns = new AmazonSNSClient(credentials)
    sns.setRegion(region)

    val atm = new ArchiveTransferManager(client, sqs, sns)
    val lsnr = new AWSProgressListener {
      def progressChanged(ev: ProgressEvent): Unit = ev.getEventType match {
        case TRANSFER_COMPLETED_EVENT =>
          println(s"archive download completed: $archiveId")
        case TRANSFER_CANCELED_EVENT =>
          println(s"archive download canceled: $archiveId")
        case TRANSFER_FAILED_EVENT =>
          println(s"archive download failed: $archiveId")
        case _ =>
      }
    }
    atm.download("-", vaultName, archiveId, new File(targetFile), lsnr)
  }

  // ========= other =========

  def shutdown = client.shutdown

}

object GlacierClient {
  def apply(region: Regions, credentials: AWSCredentialsProvider) = new GlacierClient(region, credentials)
  def apply() = new GlacierClient(Regions.EU_CENTRAL_1, new ProfileCredentialsProvider)
}
