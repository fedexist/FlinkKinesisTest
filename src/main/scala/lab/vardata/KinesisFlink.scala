package lab.vardata

import java.io.FileNotFoundException
import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.InitialPosition
import org.apache.flink.streaming.connectors.kinesis.config.{AWSConfigConstants, ConsumerConfigConstants}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import play.api.libs.json.Json

object KinesisFlink extends App {

  override def main(args: Array[String]): Unit = {

    val aws_config : String = try {
      ParameterTool.fromArgs(args).getRequired("config")
    } catch {
      case e: Exception =>
        println(e.getMessage)
        println("No properties file path specified. Please run 'ProjectSwiftr --config /path/to/file.properties'")
        return
    }
    val properties = try{
      ParameterTool.fromPropertiesFile(aws_config)
    } catch {
      case e: FileNotFoundException =>
        println(e.getMessage)
        return
    }

    val stream_name: String = try {
      properties.getRequired("stream_name")
    } catch {
      case e: Exception =>
        println(s"No stream name specified. Please add the 'stream_name' property in $aws_config")
        return
    }

    val aws_region: String = try {
      properties.getRequired("aws_region")
    } catch {
      case e: Exception =>
        println(s"No aws region specified. Please add the 'aws_region' property in $aws_config")
        return
    }

    val aws_access_id: String = try {
      properties.getRequired("aws_access_id")
    } catch {
      case e: Exception =>
        println(s"No aws access id specified. Please add the 'aws_access_id' property in $aws_config")
        return
    }

    val aws_secret_key: String = try {
      properties.getRequired("aws_secret_key")
    } catch {
      case e: Exception =>
        println(s"No aws secret key specified. Please add the 'aws_secret_key' property in $aws_config")
        return
    }

    val consumerConfig = new Properties()
    consumerConfig.put(AWSConfigConstants.AWS_REGION, aws_region)
    consumerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, aws_access_id)
    consumerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, aws_secret_key)
    consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST")
    consumerConfig.put(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS, "1000")

    // get the execution environment
    println("Initializing Stream Execution Environment")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // TODO: classe StreamEvent(event : JsValue)
    println(s"Adding Kinesis source $stream_name")
    val kinesis = env.addSource(
      new FlinkKinesisConsumer[String](stream_name, new SimpleStringSchema(), consumerConfig)).map{
      r => Json.parse(r)
    }.name("json-parser").uid("json-parser")

    val banknoteEvents = kinesis
      .filter(j => j("deviceType").toString() == "M3").name("filter")
      .map { j => StreamEvent(j, 1) }.name("map").uid("map")
      .split(
        (event: StreamEvent) =>
          event.IsRecognized match {
            case 1 => List("NotRecognizedEvent")
            case 0 => List("RecognizedEvent")
          })

    val notRecognizedStream = banknoteEvents select "NotRecognizedEvent"

    val currentDay = new HTableDescriptor(TableName.valueOf("currentDay"))
      .addFamily(
        new HColumnDescriptor(EventFamily.BANKNOTE_EVENT.toString)
          .setTimeToLive(86400)
          .setMaxVersions(100)
          .setCompressionType(Compression.Algorithm.SNAPPY)
      ).toByteArray

    val hbase_conf = new Path("/mnt/nfs/nfsshare/hbase-site.xml")

    notRecognizedStream
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
      .apply(HBaseBatchWindow()).name("hbase-window-1").uid("hbase-window-1")
      .writeUsingOutputFormat(new HBaseBatchFormat(currentDay, hbase_conf))
      .setParallelism(1)
      .name("output-hbase-1").uid("output-hbase-1")



    env.execute("Kinesis Stream Test")

  }

  object EventFamily extends Enumeration {
    type EventFamily = Value

    val BANKNOTE_EVENT: EventFamily.Value = Value("banknoteEvents")
  }

  object EventType extends Enumeration {
    type EventType = Value

    val BANKNOTE_RECOGNIZED: KinesisFlink.EventType.Value = Value("OK")
    val BANKNOTE_NOT_RECOGNIZED: KinesisFlink.EventType.Value = Value("KO")

  }


}
