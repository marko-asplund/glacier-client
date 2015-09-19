organization  := "fi.markoa"

version       := "0.1"

scalaVersion  := "2.11.6"

scalacOptions := Seq("-feature", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "org.scalatest" %% "scalatest" % "2.2.5" % "test",
  "com.amazonaws" % "aws-java-sdk-glacier" % "1.10.20",
  "io.argonaut" %% "argonaut" % "6.0.4"
)

