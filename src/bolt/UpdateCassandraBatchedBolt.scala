package bolt

import java.util

import data.{WordStorage, WordStorageFactory, WordStorageOptions}
import org.apache.storm.{Config, Constants}
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.Tuple

case class UpdateCassandraBatchedBolt() extends BaseRichBolt {
    var wordStorage: WordStorage = null
    var outputCollector: OutputCollector = null
    var tuples = Vector.empty[Tuple]

    override def prepare(map: util.Map[_, _], topologyContext: TopologyContext, outputCollector: OutputCollector): Unit = {
        this.outputCollector = outputCollector

        this.wordStorage = WordStorageFactory.createStorage(
            WordStorageOptions("wordcluster", "127.0.0.1"))
    }

    override def execute(tuple: Tuple): Unit = {
        var flush = false

        if (tuple.getSourceStreamId == Constants.SYSTEM_TICK_STREAM_ID)  {
            flush = true
        } else {
            this.tuples :+= tuple

            flush = if (this.tuples.size >= 3) true else false
        }

        if (flush) {
            for (tuple <- this.tuples) {
                val word = tuple.getString(0)
                val count = tuple.getString(1)

                this.wordStorage.updateWordCount(
                    word = word, count = count)

                this.outputCollector.ack(tuple)
            }

            this.tuples = Vector.empty[Tuple]
        }
    }

    override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {

    }

    override def getComponentConfiguration: util.Map[String, AnyRef] = {
      val conf = new Config
      conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, Int.box(1))

      conf
    }
}
