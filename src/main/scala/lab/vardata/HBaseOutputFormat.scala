package lab.vardata

import java.io.IOException

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HBaseAdmin, Table}
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.slf4j.LoggerFactory

abstract class HBaseOutputFormat[T](byteTableDesc : Array[Byte], confPath : Path) extends OutputFormat[T]{

  private val LOG = LoggerFactory.getLogger(this.getClass)


  @transient var conf : org.apache.hadoop.conf.Configuration = _
  @transient var connection : Connection = _
  @transient var table : Table = _
  var taskNumber : String = _

  @throws[IOException]
  def configure(parameters: Configuration): Unit = {
    conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "master-1.localdomain,master-2.localdomain")
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    HBaseAdmin.checkHBaseAvailable(conf)
    connection = ConnectionFactory.createConnection(conf)
  }


  @throws[IOException]
  def close(): Unit = {
    table.close()
    LOG.debug(s"Task $taskNumber: Closed connection to ${table.getName}")
  }


  @throws[IOException]
  def open(taskNumber: Int, numTasks: Int): Unit = {
    this.taskNumber = String.valueOf(taskNumber)
    @transient lazy val admin = connection.getAdmin
    @transient lazy val tableDescriptor = HTableDescriptor.parseFrom(byteTableDesc)

    if(!admin.tableExists(tableDescriptor.getTableName))
      admin.createTable(tableDescriptor)

    table = connection.getTable(tableDescriptor.getTableName)
    LOG.debug(s"Task $taskNumber: Opening connection to ${table.getName} to execute $numTasks tasks on Single Put job")

  }
}
