name := "to_date"

version := "1.0"

scalaVersion := "2.12.7"

libraryDependencies ++= Seq(
  "com.facebook.presto" % "presto-spi" % "0.203" % "provided",
  "com.google.guava" % "guava" % "26.0-jre"
)