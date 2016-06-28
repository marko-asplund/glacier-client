organization  := "fi.markoa"

version       := "0.4.1"

scalaVersion  := "2.11.8"

scalacOptions := Seq("-feature", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.4.0",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "com.amazonaws" % "aws-java-sdk-glacier" % "1.11.11",
  "io.argonaut" %% "argonaut" % "6.1"
)

