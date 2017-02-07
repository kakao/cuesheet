package com.kakao.cuesheet.convert

import java.nio.charset.StandardCharsets.UTF_8

import com.kakao.mango.concurrent._
import com.kakao.mango.couchbase.Couchbase
import com.kakao.mango.hbase.HBase
import com.kakao.mango.json._
import com.kakao.mango.util.Retry
import org.apache.spark.rdd.RDD

import scala.concurrent.duration._

/** Extension methods for pair RDDs with String keys.
  * Supports saving records to key-value stores such as Couchbase and HBase,
  * and the values are converted to JSON under the hood.
  */
class StringKeyRDD[T](rdd: RDD[(String, T)]) extends SaveToES(rdd) {

  def saveToCouchbase(nodes: Seq[String], bucket: String, expiry: Int = 0, maxRate: Double = 1e7, password: String = null): Unit = {
    // rate per executor
    val rate = maxRate / rdd.sparkContext.getExecutorMemoryStatus.size

    rdd.foreachPartition { partition =>
      // BackPressureException may happen, so retry 10 times
      // if that fails, Spark task scheduler may retry again.
      val cluster = Couchbase(nodes: _*)
      val client = cluster.bucket(bucket, password)

      val converted = partition.map {
        case (key, value: Array[Byte]) => (key, new String(value, UTF_8))
        case (key, value: String) => (key, value)
        case (key, value) => (key, toJson(value))
      }

      for (group <- converted.grouped(1000)) {
        Retry(10, 100.millis) {
          client.putAll(group, rate, expiry).sync()
        }
      }

      cluster.disconnect()
    }
  }

  def saveToHBase(quorum: String, table: String, family: String, qualifier: String, maxRate: Double = 1e7): Unit = {
    // rate per executor
    val rate = maxRate / rdd.sparkContext.getExecutorMemoryStatus.size

    rdd.foreachPartition { partition =>
      val hbase = HBase(quorum)
      val column = hbase.column(table, family, qualifier)

      val converted = partition.map {
        case (key, value: Array[Byte]) => (key.getBytes(UTF_8), value)
        case (key, value: String) => (key.getBytes(UTF_8), value.getBytes(UTF_8))
        case (key, value) => (key.getBytes(UTF_8), serialize(value))
      }

      for (group <- converted.grouped(1000)) {
        Retry(10, 100.millis) {
          column.putAllBytes(group, rate).sync()
        }
      }
    }
  }
}
