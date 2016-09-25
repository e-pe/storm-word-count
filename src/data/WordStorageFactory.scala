package data

import me.prettyprint.cassandra.serializers.{IntegerSerializer, StringSerializer}
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate
import me.prettyprint.hector.api.Cluster
import me.prettyprint.hector.api.factory.HFactory

object WordStorageFactory {
    def createStorage(options: WordStorageOptions): WordStorage = {
        val cluster = HFactory.getOrCreateCluster(
          options.clusterName, options.hostAddress)

        val keyspace = HFactory.createKeyspace("wordcounts", cluster)

        val columnFamilyTemplate = new ThriftColumnFamilyTemplate[String, String](
          keyspace, "words", StringSerializer.get(), StringSerializer.get())

        WordStorage(columnFamilyTemplate)
    }
}
