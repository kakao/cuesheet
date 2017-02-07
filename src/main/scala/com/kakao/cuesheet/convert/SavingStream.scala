package com.kakao.cuesheet.convert

import com.kakao.mango.concurrent.{NamedExecutors, RichExecutorService}
import com.kakao.mango.text.ThreadSafeDateFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream

import java.util.concurrent.{Future => JFuture}
import scala.reflect.runtime.universe.TypeTag

object SavingStream {
  val yyyyMMdd = ThreadSafeDateFormat("yyyy-MM-dd")
  val hh = ThreadSafeDateFormat("HH")
  val mm = ThreadSafeDateFormat("mm")
  val m0 = (ms: Long) => mm(ms).charAt(0) + "0"
}

/** A base class for saving DStream data as partitioned tables */
@SerialVersionUID(1L)
abstract class SavingStream[T](stream: DStream[T])(implicit es: ExecutorSupplier) extends Serializable {
  import SavingStream._

  /** converter from type [[T]] to [[DataFrame]], which derived classes should implement */
  protected def toDF(rdd: RDD[T]): DataFrame

  /** classic double-checked locking because lazy val doesn't work well with serialization */
  @transient var executor: RichExecutorService = _

  def ex: RichExecutorService = {
    if (executor == null) {
      this.synchronized {
        if (executor == null) {
          executor = new RichExecutorService(es.get())
        }
      }
    }
    executor
  }

  def saveAsPartitionedTable(table: String, path: String, format: String = "orc")(toPartition: Time => Seq[(String, String)]): Unit = {
    stream.foreachRDD { (rdd, time) =>
      ex.submit {
        toDF(rdd).appendToExternalTablePartition(table, path, format, toPartition(time): _*)
      }
    }
  }

  def saveAsDailyPartitionedTable(table: String, path: String, dateColumn: String = "date", format: String = "orc"): Unit = {
    saveAsPartitionedTable(table, path, format) { time =>
      val ms = time.milliseconds
      Seq(dateColumn -> yyyyMMdd(ms))
    }
  }

  def saveAsHourlyPartitionedTable(table: String, path: String, dateColumn: String = "date", hourColumn: String = "hour", format: String = "orc"): Unit = {
    saveAsPartitionedTable(table, path, format) { time =>
      val ms = time.milliseconds
      Seq(dateColumn -> yyyyMMdd(ms), hourColumn -> hh(ms))
    }
  }

  def saveAsTenMinutelyPartitionedTable(table: String, path: String, dateColumn: String = "date", hourColumn: String = "hour", minuteColumn: String = "minute", format: String = "orc"): Unit = {
    saveAsPartitionedTable(table, path, format) { time =>
      val ms = time.milliseconds
      Seq(dateColumn -> yyyyMMdd(ms), hourColumn -> hh(ms), minuteColumn -> m0(ms))
    }
  }

  def saveAsMinutelyPartitionedTable(table: String, path: String, dateColumn: String = "date", hourColumn: String = "hour", minuteColumn: String = "minute", format: String = "orc"): Unit = {
    saveAsPartitionedTable(table, path, format) { time =>
      val ms = time.milliseconds
      Seq(dateColumn -> yyyyMMdd(ms), hourColumn -> hh(ms), minuteColumn -> mm(ms))
    }
  }

}

class ProductStream[T <: Product : TypeTag](stream: DStream[T])(implicit ctx: HiveContext, es: ExecutorSupplier) extends SavingStream[T](stream) {
  override def toDF(rdd: RDD[T]) = ctx.createDataFrame(rdd)
}

class JsonStream(stream: DStream[String])(implicit ctx: HiveContext, es: ExecutorSupplier) extends SavingStream[String](stream) {
  override def toDF(rdd: RDD[String]) = ctx.read.json(rdd)
}

class MapStream[T](stream: DStream[Map[String, T]])(implicit ctx: HiveContext, es: ExecutorSupplier) extends SavingStream[Map[String, T]](stream) {
  import com.kakao.mango.json._

  override def toDF(rdd: RDD[Map[String, T]]) = ctx.read.json(rdd.map(toJson))
}

class RowStream(stream: DStream[Row])(implicit ctx: HiveContext, es: ExecutorSupplier, schema: StructType) extends SavingStream[Row](stream) {
  override def toDF(rdd: RDD[Row]): DataFrame = ctx.createDataFrame(rdd, schema)
}
