import bolt.{SplitSentenceBolt, UpdateCassandraBatchedBolt, WordCountBolt}
import org.apache.storm.{Config, LocalCluster, StormSubmitter}
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.tuple.Fields
import spout.RandomSentenceSpout

object Main {
  def main(args:Array[String]) = {
      val conf = new Config
      val topologyBuilder = new TopologyBuilder
      val localCluster = new LocalCluster

      topologyBuilder.setSpout("sentence-spout", RandomSentenceSpout(), 8)
      topologyBuilder.setBolt("splitter", SplitSentenceBolt(), 12)
          .shuffleGrouping("sentence-spout")

      topologyBuilder.setBolt("count", WordCountBolt(), 12)
            .fieldsGrouping("splitter", new Fields("word"))

      topologyBuilder.setBolt("cassandra", UpdateCassandraBatchedBolt(), 8)
                .fieldsGrouping("count", new Fields("word"))

      conf.setNumWorkers(4)
      conf.setDebug(true)

      conf.setMaxSpoutPending(100)

      localCluster.submitTopology("word-count-topology", conf, topologyBuilder.createTopology)
  }
}
