organization  := "com.practicingtechie"

version       := "0.5.0"

scalaVersion  := "2.13.6"

scalacOptions := Seq("-feature", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.3",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.scalatest" %% "scalatest" % "3.2.9" % "test",
  "software.amazon.awssdk" % "glacier" % "2.16.83",
  "software.amazon.awssdk" % "auth" % "2.16.83",
  //"io.argonaut" %% "argonaut" % "6.2.2"
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
