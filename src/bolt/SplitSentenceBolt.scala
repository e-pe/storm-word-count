package bolt

import org.apache.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.tuple.{Fields, Tuple, Values}

case class SplitSentenceBolt() extends BaseBasicBolt {
  override def execute(tuple: Tuple, basicOutputCollector: BasicOutputCollector): Unit = {
    val sentence = tuple.getString(0)

    for {
      word <- sentence.split(" ")
    } {
      basicOutputCollector.emit(new Values(word))
    }
  }

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
    outputFieldsDeclarer.declare(new Fields("word"))
  }
}
