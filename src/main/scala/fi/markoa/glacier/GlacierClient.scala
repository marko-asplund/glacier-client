package fi.markoa.glacier

import java.time.ZonedDateTime
import java.io.{InputStream, File, FileWriter}
import scala.concurrent.{Promise, Await}
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}
import scala.collection.JavaConversions._
import scala.io.Source
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.glacier.AmazonGlacierClient
import com.amazonaws.services.glacier.model._
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager
import com.amazonaws.event.{ProgressListener, ProgressEvent, ProgressEventType}
import argonaut._, Argonaut._

case class Vault(creationDate: ZonedDateTime, lastInventoryDate: Option[ZonedDateTime],
                 numberOfArchives: Long, sizeInBytes: Long, vaultARN: String,
                 vaultName: String)

case class Job(id: String, vaultARN: String, action: String, description: String, creationDate: ZonedDateTime, statusCode: String, statusMessage: String, completionDate: Option[ZonedDateTime], archiveId: Option[String])

case class JobOutput(archiveDescription: String, contentType: String, checksum: String, status: Int, output: InputStream)

case class Archive(id: String, location: String, checksum: Option[String], description: Option[String])

case class AmInventory(vaultARN: String, inventoryDate: ZonedDateTime, archives: List[AmArchive])
case class AmArchive(id: String, description: String, created: ZonedDateTime, size: Long, treeHash: String)

object DateTimeOrdering extends Ordering[ZonedDateTime] {
  def compare(a: ZonedDateTime, b: ZonedDateTime) = a compareTo b
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
    Archive(a.getArchiveId, a.getLocation, Some(a.getChecksum), None)

  implicit def ArchiveCodecJson = 
    casecodec4(Archive.apply, Archive.unapply)("archiveId", "location", "checksum", "description")

  implicit def DateTimeEncodeJson: EncodeJson[ZonedDateTime] = EncodeJson(d => jString(d.toString))
  implicit def DateTimeDecodeJson: DecodeJson[ZonedDateTime] =
    optionDecoder(_.string flatMap (s => tryTo(ZonedDateTime.parse(s))), "java.time.ZonedDateTime")
  implicit def AmArchiveCodecJson =
    casecodec5(AmArchive.apply, AmArchive.unapply)("ArchiveId", "ArchiveDescription", "CreationDate",
      "Size", "SHA256TreeHash")
  implicit def AmInventoryCodecJson =
    casecodec3(AmInventory.apply, AmInventory.unapply)("VaultARN", "InventoryDate", "ArchiveList")
}

class GlacierClient(endPoint: String, credentials: AWSCredentialsProvider) {
  import Converters._

  val logger = Logger(LoggerFactory.getLogger(classOf[GlacierClient]))

  val client = new AmazonGlacierClient(credentials)
  client.setEndpoint(endPoint)

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

  private def getJobOutput(vaultName: String, jobId: String) =
    asJobOutput(client.getJobOutput(new GetJobOutputRequest().withVaultName(vaultName).withJobId(jobId)))


  // ========= Glacier archive management =========

  def uploadArchive(vaultName: String, archiveDescription: String, sourceFile: String) = {
    import ProgressEventType._
    val atm = new ArchiveTransferManager(client, credentials)
    val archive = Promise[Archive]()
    val lsnr = new ProgressListener {
      def getArchive = Await.result(archive.future, Duration(2, "seconds"))
      def progressChanged(ev: ProgressEvent): Unit = ev.getEventType match {
        case REQUEST_BYTE_TRANSFER_EVENT => print("*")
        case TRANSFER_COMPLETED_EVENT =>
          catAddArchive(vaultName, getArchive)
          println(s"\narchive upload completed: ${getArchive.id}")
        case TRANSFER_CANCELED_EVENT =>
          println(s"\ntransfer canceled: ${getArchive.id}")
        case TRANSFER_FAILED_EVENT =>
          println(s"\ntransfer failed: ${getArchive.id}")
        case _ =>
      }
    }
    val r = atm.upload("-", vaultName, archiveDescription, new File(sourceFile), lsnr)
    val a = Archive(r.getArchiveId, sourceFile, None, Some(archiveDescription))
    archive.success(a)
    a
  }

  def deleteArchive(vaultName: String, archiveId: String) = {
    client.deleteArchive(new DeleteArchiveRequest(vaultName, archiveId))
    catDeleteArchive(vaultName, archiveId)
  }

  def downloadArchive(vaultName: String, archiveId: String, targetFile: String) = {
    import ProgressEventType._
    val atm = new ArchiveTransferManager(client, credentials)
    // TODO: implement progressmeter
    val lsnr = new ProgressListener {
      def progressChanged(ev: ProgressEvent): Unit = ev.getEventType match {
        case TRANSFER_COMPLETED_EVENT =>
        case TRANSFER_CANCELED_EVENT =>
        case TRANSFER_FAILED_EVENT =>
        case _ =>
      }
    }
    atm.download("-", vaultName, archiveId, new File(targetFile), lsnr)
  }

  // ========= other =========

  def shutdown = client.shutdown

}

object GlacierClient {
  def apply(endPoint: String, credentials: AWSCredentialsProvider) = new GlacierClient(endPoint, credentials)
  def apply() = new GlacierClient("https://glacier.eu-central-1.amazonaws.com/", new ProfileCredentialsProvider)
}
