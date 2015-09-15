package fi.markoa.glacier

import java.time.ZonedDateTime
import java.io.{InputStream, File, FileWriter}
import scala.concurrent.{Promise, Await}
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}
import scala.collection.JavaConversions._
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
}

class GlacierClient(endPoint: String, credentials: AWSCredentialsProvider) {
  import Converters._

  val logger = Logger(LoggerFactory.getLogger(classOf[GlacierClient]))

  val client = new AmazonGlacierClient(credentials)
  client.setEndpoint(endPoint)

  val b64enc = java.util.Base64.getEncoder
  val b64dec = java.util.Base64.getDecoder

  val homeDir = new File(sys.props("user.home"), ".glacier-backup")

  // TODO: vault tags management
  // notifications?

  def describeVault(vaultName: String) = vaultResultasVault(client.describeVault(new DescribeVaultRequest(vaultName)))

  def createVault(vaultName: String) = client.createVault(new CreateVaultRequest(vaultName)).getLocation

  def listVaults: Seq[Vault] = client.listVaults(new ListVaultsRequest).getVaultList.map(asVault)

  def deleteVault(vaultName: String) = client.deleteVault(new DeleteVaultRequest(vaultName))

  def listArchives(vaultName: String) = ???
  def addArchive(vaultName: String, arch: Archive) = {
    val d = new File(homeDir, b64enc.encodeToString(vaultName.getBytes))
    d.mkdirs
    val f = new File(d, arch.id.substring(0, 20))
    val w = new FileWriter(f)
    w.write(arch.asJson.spaces2)
    w.close
  }
  def deleteArchive(vaultName: String, archiveId: String) = ???

  //https://docs.aws.amazon.com/amazonglacier/latest/dev/retrieving-vault-inventory-java.html

  def requestInventory(vaultName: String): Option[String] =
    Try(client.initiateJob(new InitiateJobRequest(vaultName, new JobParameters().withType("inventory-retrieval"))).getJobId) match {
      case Success(jobId: String) => Some(jobId)
      case Failure(ex) => { logger.error(s"failed to get inventory for vault ${vaultName}", ex); None }
    }

  def getLatestVaultInventory(vaultName: String) = ???

  def listJobs(vaultName: String) =
    client.listJobs(new ListJobsRequest(vaultName)).getJobList.map(asJob)

  def getJobOutput(vaultName: String, jobId: String) =
    asJobOutput(client.getJobOutput(new GetJobOutputRequest().withVaultName(vaultName).withJobId(jobId)))

  def uploadArchive(vaultName: String, archiveDescription: String, path: String) = {
    import ProgressEventType._
    val atm = new ArchiveTransferManager(client, credentials)
    val archiveId = Promise[Archive]()
    // use time based progress indicator?
    val lsnr = new ProgressListener {
      def progressChanged(ev: ProgressEvent): Unit = ev.getEventType match {
        case REQUEST_BYTE_TRANSFER_EVENT =>
        case TRANSFER_CANCELED_EVENT => println("")
        case TRANSFER_COMPLETED_EVENT =>
          val ar = Await.result(archiveId.future, Duration(2, "seconds"))
          println(s"archive upload completed: ${ar.id}")
        case TRANSFER_FAILED_EVENT => println("archive upload failed: "+ev)
        case _ => println(ev+", "+ev.getBytes)
      }
    }
    val r = atm.upload("-", vaultName, archiveDescription, new File(path), lsnr)
    val a = Archive(r.getArchiveId, path, None, Some(archiveDescription))
    archiveId.success(a)
    a
  }

  def downloadArchive = ???

  def shutdown = client.shutdown

}

object GlacierClient {
  def apply(endPoint: String, credentials: AWSCredentialsProvider) = new GlacierClient(endPoint, credentials)
  def apply() = new GlacierClient("https://glacier.eu-central-1.amazonaws.com/", new ProfileCredentialsProvider)
}
