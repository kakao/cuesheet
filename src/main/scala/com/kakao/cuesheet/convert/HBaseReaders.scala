package com.kakao.cuesheet.convert

import com.kakao.mango.util.Conversions._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

trait HBaseReaders {
  val sc: SparkContext

  /** Read an entire HBase table, and make and RDD of (row key, ((column family, column qualifier), (timestamp, value)))
    *
    * @param quorum  The zookeeper connectString to use HBase
    * @param table   The HBase table
    */
  def hbaseTableBinary(quorum: String, table: String): RDD[(Array[Byte], ((Array[Byte], Array[Byte]), (Long, Array[Byte])))] = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", quorum)
    conf.set(TableInputFormat.INPUT_TABLE, table)

    sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).flatMap {
      case (bytes, result) =>
        val rowkey = bytes.get()
        result.getMap.toSeq.flatMap {
          case (family, map) =>
            map.toSeq.map {
              case (qualifier, cell) =>
                cell.last match {
                  case (timestamp, value) => (rowkey, ((family, qualifier), (timestamp.longValue(), value)))
                }
            }
        }
    }
  }

  /** Read an entire HBase table, and make and RDD of (row key, ((column family, column qualifier), (timestamp, value)))
    *
    * @param quorum  The zookeeper connectString to use HBase
    * @param table   The HBase table
    */
  def hbaseTable(quorum: String, table: String): RDD[(String, ((String, String), (Long, String)))] = {
    hbaseTableBinary(quorum, table).map {
      case (rowkey, ((family, qualifier), (timestamp, value))) =>
        (rowkey.string, ((family.string, qualifier.string), (timestamp, value.string)))
    }
  }

  def hbaseColumnBinary(quorum: String, table: String, family: Array[Byte], qualifier: Array[Byte]): RDD[(Array[Byte], (Long, Array[Byte]))] = {
    hbaseTableBinary(quorum, table).collect {
      case (rowkey, ((f, q), cell)) if family.sameElements(f) && qualifier.sameElements(q) => (rowkey, cell)
    }
  }

  def hbaseColumn(quorum: String, table: String, family: String, qualifier: String): RDD[(String, (Long, String))] = {
    hbaseTable(quorum, table).collect {
      case (rowkey, ((f, q), cell)) if family == f && qualifier == q => (rowkey, cell)
    }
  }
}
