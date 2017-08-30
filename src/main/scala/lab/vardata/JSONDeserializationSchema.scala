package lab.vardata

import java.io.IOException

import org.apache.flink.streaming.util.serialization.AbstractDeserializationSchema
import org.slf4j.LoggerFactory
import play.api.libs.json._


/**
  * DeserializationSchema that deserializes a JSON String into a JsValue.
  *
  * <p>Fields can be accessed by calling objectNode.get(&lt;name>).as(&lt;type>)
  */
class JSONDeserializationSchema extends AbstractDeserializationSchema[JsValue] {

  private val LOG = LoggerFactory.getLogger(this.getClass)


  @throws[IOException]
  override def deserialize(message: Array[Byte]): JsValue = {

    LOG.info(s"JSON byte representation: $message")
    val obj = Json.parse(message)
    LOG.info(s"JSON string representation after Json.parse: $obj")
    obj
  }

  override def isEndOfStream(nextElement: JsValue) = false
}