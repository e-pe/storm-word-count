package bolt

import org.apache.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.tuple.{Fields, Tuple, Values}

case class WordCountBolt() extends BaseBasicBolt {
  var counts = Map.empty[String, Int]

  override def execute(tuple: Tuple, basicOutputCollector: BasicOutputCollector): Unit = {
      val word = tuple.getString(0)
      var count = this.counts.getOrElse(word, 0)

      count += 1

      this.counts = this.counts + (word -> count)

      println(word + " " + count)

      basicOutputCollector.emit(new Values(word, count.toString))
  }

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
    outputFieldsDeclarer.declare(new Fields("word", "count"))
  }
}
