package fi.markoa.glacier

import java.time.ZonedDateTime
import java.io.{InputStream, File}
import scala.util.{Try, Success, Failure}
import scala.collection.JavaConversions._
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.glacier.AmazonGlacierClient
import com.amazonaws.services.glacier.model._
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager
import com.amazonaws.event.{ProgressListener, ProgressEvent, ProgressEventType}

case class Vault(creationDate: ZonedDateTime, lastInventoryDate: Option[ZonedDateTime],
                 numberOfArchives: Long, sizeInBytes: Long, vaultARN: String,
                 vaultName: String)

case class Job(id: String, vaultARN: String, action: String, description: String, creationDate: ZonedDateTime, statusCode: String, statusMessage: String, completionDate: Option[ZonedDateTime], archiveId: Option[String])

case class JobOutput(archiveDescription: String, contentType: String, checksum: String, status: Int, output: InputStream)

case class Archive(id: String, checksum: String, location: String)

object Converters {
  def parseOptDate(ds: String) = Option(ds).map(ZonedDateTime.parse(_))

  def asVault(v: DescribeVaultOutput) =
    Vault(ZonedDateTime.parse(v.getCreationDate), parseOptDate(v.getLastInventoryDate), v.getNumberOfArchives, v.getSizeInBytes, v.getVaultARN, v.getVaultName)

  def asJob(j: GlacierJobDescription) =
    Job(j.getJobId, j.getVaultARN, j.getAction, j.getJobDescription, ZonedDateTime.parse(j.getCreationDate), j.getStatusCode, j.getStatusMessage, parseOptDate(j.getCompletionDate), Option(j.getArchiveId))

  def asJobOutput(o: GetJobOutputResult) =
    JobOutput(o.getArchiveDescription, o.getContentType, o.getChecksum, o.getStatus, o.getBody)

  def asArchive(a: UploadArchiveResult) =
    Archive(a.getArchiveId, a.getChecksum, a.getLocation)
}

class GlacierClient(endPoint: String, credentials: AWSCredentialsProvider) {
  import Converters._

  val client = new AmazonGlacierClient(credentials)
  client.setEndpoint(endPoint)

  def createVault = ???

  def describeVault = ???

  def listVaults: Seq[Vault] = client.listVaults(new ListVaultsRequest).getVaultList.map(asVault)

  def deleteVault = ???

  //https://docs.aws.amazon.com/amazonglacier/latest/dev/retrieving-vault-inventory-java.html

  def requestInventory(vaultName: String): Option[String] =
    Try(client.initiateJob(new InitiateJobRequest(vaultName, new JobParameters().withType("inventory-retrieval"))).getJobId) match {
      case Success(jobId: String) => Some(jobId)
      case Failure(ex) => { println(ex); None }
    }

  def getLatestVaultInventory(vaultName: String) = ???

  def listJobs(vaultName: String) =
    client.listJobs(new ListJobsRequest(vaultName)).getJobList.map(asJob)

  def getJobOutput(vaultName: String, jobId: String) =
    asJobOutput(client.getJobOutput(new GetJobOutputRequest().withVaultName(vaultName).withJobId(jobId)))

  def uploadArchive(vaultName: String, archiveDescription: String, path: String) = {
    val atm = new ArchiveTransferManager(client, credentials)
    val lsnr = new ProgressListener {
      def progressChanged(ev: ProgressEvent): Unit = {
        ev.getEventType match {
          case ProgressEventType.BYTE_TRANSFER_EVENT => 
          case ProgressEventType.REQUEST_BYTE_TRANSFER_EVENT => 
          case _ => println("event: "+ev.getEventType)
        }
      }
    }
    atm.upload("-", vaultName, archiveDescription, new File(path), lsnr).getArchiveId
  }

}

object GlacierClient {
  def apply(endPoint: String, credentials: AWSCredentialsProvider) = new GlacierClient(endPoint, credentials)
  def apply() = new GlacierClient("https://glacier.eu-central-1.amazonaws.com/", new ProfileCredentialsProvider)
}
