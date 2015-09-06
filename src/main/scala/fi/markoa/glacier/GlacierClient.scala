package fi.markoa.glacier

import java.time._
import scala.collection.JavaConversions._
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.glacier.AmazonGlacierClient
import com.amazonaws.services.glacier.model._

case class Vault(creationDate: ZonedDateTime, lastInventoryDate: Option[ZonedDateTime],
                 numberOfArchives: Long, sizeInBytes: Long, vaultARN: String,
                 vaultName: String)

object Converters {
  implicit def vaultConverter(v: DescribeVaultOutput) =
    Vault(ZonedDateTime.parse(v.getCreationDate), Option(v.getLastInventoryDate).map(d => ZonedDateTime.parse(d)), v.getNumberOfArchives, v.getSizeInBytes, v.getVaultARN, v.getVaultName)
  implicit def vaultSeqConverter(s: Seq[DescribeVaultOutput]) =
    s.map(i => vaultConverter(i))
}

class GlacierClient(endPoint: String) {
  import Converters._

  val client = new AmazonGlacierClient(new ProfileCredentialsProvider)
  client.setEndpoint(endPoint)

  def createVault = {
  }

  def listVaults: Seq[Vault] = {
    val s: Seq[DescribeVaultOutput] = client.listVaults(new ListVaultsRequest).getVaultList
    s
  }

}

object GlacierClient {
  def apply(endPoint: String) = new GlacierClient(endPoint)
  def apply() = new GlacierClient("https://glacier.eu-central-1.amazonaws.com/")
}
