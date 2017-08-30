package lab.vardata

import java.io.IOException
import java.util

import org.apache.flink.core.fs.Path
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

@SerialVersionUID(1L)
class HBaseBatchFormat(byteTableDesc : Array[Byte], confPath : Path)
  extends HBaseOutputFormat[BatchContainer](byteTableDesc, confPath) {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  @throws[IOException]
  def writeRecord(record: BatchContainer): Unit = {

    val list = new util.ArrayList[Put](record.batch.map {
      tuple4: (String, String, String, Int) =>
        new Put(Bytes.toBytes(tuple4._1)).addColumn(Bytes.toBytes(tuple4._2), Bytes.toBytes(tuple4._3), Bytes.toBytes(tuple4._4)
        )}.asJavaCollection)

    val results : Array[AnyRef] = new Array[AnyRef](list.size())
    LOG.info(s"Task $taskNumber: Preparing to write batch on ${table.getName} a list of ${list.size()}")

    table.batch(list, results)
    LOG.info(s"Task $taskNumber: Batch write on ${table.getName} executed successfully")

  }
}
