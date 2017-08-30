package lab.vardata

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.scala.function.RichAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

case class HBaseBatchWindow() extends RichAllWindowFunction[StreamEvent, BatchContainer, TimeWindow]{

  private val LOG = LoggerFactory.getLogger(classOf[HBaseBatchWindow])

  def apply(window: TimeWindow, input: Iterable[StreamEvent], out: Collector[BatchContainer]): Unit = {

    val format = new SimpleDateFormat("yyyy-MM-ddTHH:mm:ss")

    val in_batch =
      BatchContainer(
        input.map( in => (in.organizationId.reduce( (a, b) => a+b) + in.siteId + format.format(in.sentTimestamp),
          in.eventType._1.toString, in.deviceId + in.eventType.toString, in.count)
        )
      )

    LOG.info(s"Collecting and preparing to write batch: ${in_batch.batch.size} elements")

    out.collect(in_batch)

  }
}
