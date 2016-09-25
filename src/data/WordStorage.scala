package data

import me.prettyprint.cassandra.service.template.{ColumnFamilyTemplate, ColumnFamilyUpdater}

case class WordStorage(columnFamilyTemplate: ColumnFamilyTemplate[String, String]) {
    def updateWordCount(word: String, count: String): Unit = {
        val updater = columnFamilyTemplate.createUpdater(word)

        updater.setString("count", count)

        columnFamilyTemplate.update(updater)
    }
}
