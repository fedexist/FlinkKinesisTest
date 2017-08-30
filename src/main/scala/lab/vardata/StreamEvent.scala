package lab.vardata

import lab.vardata.KinesisFlink.EventFamily.EventFamily
import lab.vardata.KinesisFlink.EventType.EventType
import lab.vardata.KinesisFlink.{EventFamily, EventType}
import org.joda.time.DateTime
import play.api.libs.json.{JsArray, JsValue}

case class StreamEvent(json: JsValue, count: Int) extends Serializable{

  var eventType : (EventFamily, EventType) = _

  val deviceId: String = json("deviceId").toString()
  val organizationId : Seq[String] = json("organizationId").as[JsArray].value.map(j => j.validate[String].get)
  val siteId: String = json("siteId").toString()
  val sentTimestamp : DateTime = DateTime.parse(json("sentTimestamp").validate[String].get)

  val payload = new EventPayload(json("payload"))

  def IsRecognized : Int = {

    if(payload.component == "bna" & payload.stateExt == "0x01" & payload.error == "0x00" & payload.banknoteRead != "0x00"){
      eventType = (EventFamily.BANKNOTE_EVENT, EventType.BANKNOTE_RECOGNIZED)
      0
    }
    else if(payload.component == "bna" & payload.stateExt == "0x01" & payload.error == "0x02" & payload.banknoteRead == "0x00"){
      eventType = (EventFamily.BANKNOTE_EVENT, EventType.BANKNOTE_NOT_RECOGNIZED)
      1
    }
    else
      -1
  }

}
