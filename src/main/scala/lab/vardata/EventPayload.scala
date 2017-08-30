package lab.vardata

import play.api.libs.json.JsValue

class EventPayload(payload: JsValue) extends Serializable{

  val component: String = payload("component").toString()
  val stateExt: String = payload("attribute")("stateExt").toString()
  val error: String = payload("attribute")("error").toString()
  val banknoteRead: String = payload("attribute")("bna")("banknoteRead").toString()

}
