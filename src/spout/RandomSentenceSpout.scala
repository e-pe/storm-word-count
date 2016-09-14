package spout

import java.util
import java.util.Random

import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.tuple.{Fields, Values}
import org.apache.storm.utils.Utils

case class RandomSentenceSpout() extends BaseRichSpout {
  var spoutCollector: SpoutOutputCollector = null
  var random: Random = null

  override def open(map: util.Map[_, _], topologyContext: TopologyContext, spoutOutputCollector: SpoutOutputCollector): Unit = {
      this.spoutCollector = spoutOutputCollector
      this.random = new Random
  }

  override def nextTuple(): Unit = {
      Thread.sleep(100)

      val sentences = Vector(
        "the cow jumped over the moon",
        "an apple a day keeps the doctor away",
        "four score and seven years ago",
        "snow white and the seven dwarfs",
        "i am at two with nature")

      val sentence = sentences(this.random.nextInt(sentences length))

      this.spoutCollector.emit(new Values(sentence))
  }

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
    outputFieldsDeclarer.declare(new Fields("sentence"))
  }
}
