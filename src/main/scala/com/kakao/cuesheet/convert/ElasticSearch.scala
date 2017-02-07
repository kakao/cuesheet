package com.kakao.cuesheet.convert

import com.kakao.mango.concurrent._
import com.kakao.mango.elasticsearch.{ElasticSearch => ES}
import com.kakao.mango.logging.Logging
import org.apache.spark.rdd.RDD

/** a utility to bulk-insert ElasticSearch records */
case class ElasticSearch(url: String, index: String, `type`: String, bulk: Int = 1000) extends Logging {
  def bulkIndex(rdd: RDD[_], maxRate: Double = 1e7): Unit = {
    // rate per executor
    val rate = maxRate / rdd.sparkContext.getExecutorMemoryStatus.size

    rdd.foreachPartition{ partition =>
      for (group <- Throttled(partition.grouped(bulk), rate / bulk)) {
        ES(url).bulk(index, `type`, group).sync()
      }
    }
  }
}

/** an extension method to RDD, to export data to a ElasticSearch storage */
abstract class SaveToES(rdd: RDD[_]) {
  def saveToES(url: String, index: String, `type`: String, bulk: Int = 1000, maxRate: Double = 1e7): Unit = {
    ElasticSearch(url, index, `type`, bulk).bulkIndex(rdd, maxRate)
  }
}

