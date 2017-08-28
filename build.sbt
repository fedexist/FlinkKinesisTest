name := "FlinkKinesisTest"

version := "0.1"

scalaVersion := "2.10.6"


mainClass in assembly := Some("lab.vardata.KinesisFlink")

val flinkVersion = "1.3.2"

libraryDependencies += "org.apache.flink" %% "flink-scala" % flinkVersion % "provided"
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided"
libraryDependencies += "org.apache.flink" %% "flink-connector-kinesis" % flinkVersion

assemblyMergeStrategy in assembly := {
  //case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", ps @ _*) => MergeStrategy.first
  case x => MergeStrategy.first
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

