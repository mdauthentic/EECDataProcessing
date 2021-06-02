name := "EECDataProcessing"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"

val sparkVersion = "3.0.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "0.21.3" % "test"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case x => MergeStrategy.first
}