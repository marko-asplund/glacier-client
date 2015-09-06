organization  := "fi.markoa"

version       := "0.1"

scalaVersion  := "2.11.6"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= {
  Seq(
    "com.amazonaws" % "aws-java-sdk" % "1.10.16"
  )
}
