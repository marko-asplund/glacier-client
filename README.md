# glacier-client

[![Build Status](https://travis-ci.org/marko-asplund/glacier-client.svg?branch=master)](https://travis-ci.org/marko-asplund/glacier-client)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.practicingtechie/glacier-backup-cli_2.11/badge.svg?style=plastic)](https://maven-badges.herokuapp.com/maven-central/com.practicingtechie/glacier-backup-cli_2.11)

Amazon Glacier backup tool with the following features
- vault management: create, describe, list, delete
- archive management: upload, delete, download
- local catalog management: add, list, delete, synchronize with Glacier inventory

glacier-client can be used through its API in Scala programs or as an interactive backup CLI (via REPL).

# basic operations

Start up Scala REPL

```
~/glacier-backup-cli (master ✘)✚✭ ᐅ sbt console

[info] Loading settings from plugins.sbt ...
[info] Loading project definition from /Users/marko/glacier-backup-cli/project
[info] Loading settings from build.sbt ...
[info] Set current project to glacier-backup-cli (in build file:/Users/marko/glacier-backup-cli/)
[info] Starting scala interpreter...
Welcome to Scala 2.11.11 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_151).
Type in expressions for evaluation. Or try :help.
```

List the names of available AWS regions

```
scala> fi.markoa.glacier.GlacierClient.regions
res0: Array[String] = Array(us-gov-west-1, us-east-1, us-east-2, us-west-1, us-west-2, eu-west-1, eu-west-2, eu-central-1, ap-south-1, ap-southeast-1, ap-southeast-2, ap-northeast-1, ap-northeast-2, sa-east-1, cn-north-1, ca-central-1)
```

Create a Glacier client that connects to the `us-west-2` region

```
scala> val c = fi.markoa.glacier.GlacierClient("us-west-2")
c: fi.markoa.glacier.GlacierClient = fi.markoa.glacier.GlacierClient@11b6e34a
```

Create new vault. The ID (or ARN) for the newly created vault is returned.

```
scala> c.createVault("test-vault-1")
res1: String = /429963740182/vaults/test-vault-1
```

List all vaults in the region. A sequence of vault objects is returned, in this case it includes just the vault we
created above. Please note that with vault operations the results are visible immediately.

```
scala> c.listVaults
res2: Seq[fi.markoa.glacier.Vault] = ArrayBuffer(Vault(arn:aws:glacier:us-west-2:429963740182:vaults/test-vault-1,test-vault-1,2017-11-19T08:18:38.990Z,None,0,0))
```

Now we’re ready to upload an archive into the vault:

```
scala> c.uploadArchive("test-vault-1", "my backup archive", "my-backup.zip")
TransferStarted: transfer started
TransferProgress: transfer progress: 5% (bytes: 516096)
TransferProgress: transfer progress: 10% (bytes: 1024000)
TransferProgress: transfer progress: 15% (bytes: 1540096)
TransferProgress: transfer progress: 20% (bytes: 2048000)
TransferProgress: transfer progress: 25% (bytes: 2564096)
TransferProgress: transfer progress: 30% (bytes: 3072000)
...
TransferProgress: transfer progress: 90% (bytes: 9216000)
TransferProgress: transfer progress: 95% (bytes: 9732096)
TransferProgress: transfer progress: 100% (bytes: 10240000)
TransferCompleted: transfer completed
res3: fi.markoa.glacier.Archive = Archive(WREjqj2BItYhI5BGV7mdJGsDl3oztPvpvVh_hngm5SWqJkOd5jnLipLyYy2KkM74-3mkt85nUjI4a_hcQZhtLnQF03K0sv2Bc97BYEwYQ7M4O_lmtgrCTuGCyAEEiuQmCFfRSnBkTw,Some(my-backup.zip),0c5dc86251d157e29cfadb04ac615426600a4e1177a8ac2c1134d895378b3acd,10240000,Some(my backup archive))
```

Note that Glacier doesn’t maintain an up-to-date list vault contents - a list of contents needs to be requested
explicitly and preparing it can take a very long time.  For this reason Glacier client stores a local catalogue
of archives per vault. Vault contents can be listed as follows:

```
scala> c.catListArchives("test-vault-1")
res4: Seq[fi.markoa.glacier.Archive] = ArraySeq(Archive(WREjqj2BItYhI5BGV7mdJGsDl3oztPvpvVh_hngm5SWqJkOd5jnLipLyYy2KkM74-3mkt85nUjI4a_hcQZhtLnQF03K0sv2Bc97BYEwYQ7M4O_lmtgrCTuGCyAEEiuQmCFfRSnBkTw,Some(my-backup.zip),0c5dc86251d157e29cfadb04ac615426600a4e1177a8ac2c1134d895378b3acd,10240000,Some(my backup archive)))
```

Archives need to be prepared prior to their retrieval and preparation can take several hours. For this reason it’s
often more convenient to retrieve them asynchronously: 1) you request archive retrieval and after Glacier has finished
preparing archive you can 2) download it.

```
scala> c.prepareArchiveRetrieval("test-vault-1", "WREjqj2BItYhI5BGV7mdJGsDl3oztPvpvVh_hngm5SWqJkOd5jnLipLyYy2KkM74-3mkt85nUjI4a_hcQZhtLnQF03K0sv2Bc97BYEwYQ7M4O_lmtgrCTuGCyAEEiuQmCFfRSnBkTw")
res1: Option[String] = Some(h479o4kxdawFsho0POzQAznw6e6beampFAIBYuI7s41O_HmzqqWsg2qk2vL2Lw_4MOsI1VFarvokz7NXczBq0CrwPKzv)
```

Archive retrieval is added in the vault’s list of jobs. You can list unfinished jobs as follows:

```
scala> c.listJobs("test-vault-1")
res4: Seq[fi.markoa.glacier.Job] = ArrayBuffer(Job(h479o4kxdawFsho0POzQAznw6e6beampFAIBYuI7s41O_HmzqqWsg2qk2vL2Lw_4MOsI1VFarvokz7NXczBq0CrwPKzv,arn:aws:glacier:us-west-2:429963740182:vaults/test-vault-1,ArchiveRetrieval,null,2017-11-19T09:00:34.339Z,InProgress,null,None,Some(WREjqj2BItYhI5BGV7mdJGsDl3oztPvpvVh_hngm5SWqJkOd5jnLipLyYy2KkM74-3mkt85nUjI4a_hcQZhtLnQF03K0sv2Bc97BYEwYQ7M4O_lmtgrCTuGCyAEEiuQmCFfRSnBkTw)))
```

Notice the `InProgress` status. Once archive preparation has been finished the job list will look something like this:

```
scala> c.listJobs("test-vault-1")
res8: Seq[fi.markoa.glacier.Job] = ArrayBuffer(Job(h479o4kxdawFsho0POzQAznw6e6beampFAIBYuI7s41O_HmzqqWsg2qk2vL2Lw_4MOsI1VFarvokz7NXczBq0CrwPKzv,arn:aws:glacier:us-west-2:429963740182:vaults/test-vault-1,ArchiveRetrieval,null,2017-11-19T09:00:34.339Z,Succeeded,Succeeded,Some(2017-11-19T12:52:38.363Z),Some(WREjqj2BItYhI5BGV7mdJGsDl3oztPvpvVh_hngm5SWqJkOd5jnLipLyYy2KkM74-3mkt85nUjI4a_hcQZhtLnQF03K0sv2Bc97BYEwYQ7M4O_lmtgrCTuGCyAEEiuQmCFfRSnBkTw)))
```

Setting up notifications relieves you from having to periodically poll job completion status and instead receive
notifications. Notifications can be set up via AWS Console.

A prepared archive can then be downloaded from Glacier using the retrieval job ID:

```
scala> c.downloadPreparedArchive("test-vault-1", "h479o4kxdawFsho0POzQAznw6e6beampFAIBYuI7s41O_HmzqqWsg2qk2vL2Lw_4MOsI1VFarvokz7NXczBq0CrwPKzv", "my-backup-restored.zip")
TransferStarted: transfer started
TransferProgress: transfer progress: 5% (bytes: 520869)
TransferProgress: transfer progress: 10% (bytes: 1025701)
TransferProgress: transfer progress: 15% (bytes: 1547941)
TransferProgress: transfer progress: 20% (bytes: 2052773)
TransferProgress: transfer progress: 25% (bytes: 2575013)
TransferProgress: transfer progress: 30% (bytes: 3079845)
...
TransferProgress: transfer progress: 90% (bytes: 9228965)
TransferProgress: transfer progress: 95% (bytes: 9736869)
TransferProgress: transfer progress: 100% (bytes: 10240000)
TransferCompleted: transfer completed
```

That’s it for the basic operations!

Some other tasks Glacier client lets you do include delete vault, request a vault inventories (list of archives a
vault contains), download inventories and delete archives.
