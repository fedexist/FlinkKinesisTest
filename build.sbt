name := "FlinkKinesisTest"

version := "0.1"

scalaVersion := "2.10.6"


mainClass in assembly := Some("lab.vardata.KinesisFlink")

val flinkVersion = "1.3.2"

libraryDependencies += "org.apache.flink" %% "flink-scala" % flinkVersion % "provided"
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided"
libraryDependencies += "org.apache.flink" %% "flink-connector-kinesis" % flinkVersion
libraryDependencies += "org.joda" % "joda-convert" % "1.8.3"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.6.2"
libraryDependencies += "org.apache.flink" %% "flink-hbase" % flinkVersion
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.3.0"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.3.0"


assemblyMergeStrategy in assembly := {
  //case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", ps @ _*) => MergeStrategy.first
  case x => MergeStrategy.first
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

