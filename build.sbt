organization  := "com.practicingtechie"

version       := "0.4.4"

scalaVersion  := "2.12.10"

scalacOptions := Seq("-feature", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "com.amazonaws" % "aws-java-sdk-glacier" % "1.11.641",
  "io.argonaut" %% "argonaut" % "6.2"
)

publishMavenStyle := true

publishTo := Some("Central repo releases" at "https://oss.sonatype.org/service/local/staging/deploy/maven2/")

licenses := Seq("Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

pomExtra := (
  <url>https://github.com/marko-asplund/glacier-client</url>
    <scm>
      <url>git@github.com:marko-asplund/glacier-client.git</url>
      <connection>scm:git:git@github.com:marko-asplund/glacier-client.git</connection>
    </scm>
    <developers>
      <developer>
        <id>marko-asplund</id>
        <name>marko asplund</name>
        <url>https://practicingtechie.com/</url>
      </developer>
    </developers>)
