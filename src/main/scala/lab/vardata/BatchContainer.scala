package lab.vardata

case class BatchContainer(batch: Iterable[(String, String, String, Int)]) extends Serializable
