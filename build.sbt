name := "to_date"

version := "1.0"

scalaVersion := "2.12.7"

libraryDependencies ++= Seq(
  "com.facebook.presto" % "presto-spi" % "0.203" % "provided",
  "com.google.guava" % "guava" % "26.0-jre",
  "joda-time" % "joda-time" % "2.9.9",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)