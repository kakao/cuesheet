package com.kakao.cuesheet.convert

import com.kakao.cuesheet.convert.ExecutorSupplier.runNow
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/** provides implicit converters for various extension methods */
trait Implicits {

  implicit def toSplittableRDD[T](rdd: RDD[T]): SplittableRDD[T] = new SplittableRDD[T](rdd)

  implicit def arrayToConvertibleRDD[T](rdd: RDD[Array[T]]): ConvertibleRDD[Seq[T]] = new ConvertibleRDD(rdd.map(_.toSeq))

  implicit def seqToConvertibleRDD[T](rdd: RDD[Seq[T]]): ConvertibleRDD[Seq[T]] = new ConvertibleRDD(rdd)

  implicit def toSavingRDD[T](rdd: RDD[T]): SavingRDD[T] = new SavingRDD[T](rdd)

  implicit def toStringRDD(rdd: RDD[String]): StringRDD = new StringRDD(rdd)

  implicit def toByteArrayRDD(rdd: RDD[Array[Byte]]): ByteArrayRDD = new ByteArrayRDD(rdd)

  implicit def toStringByteArrayRDD(rdd: RDD[(String, Array[Byte])]): StringByteArrayRDD = new StringByteArrayRDD(rdd)

  implicit def toMapRDD[K, V](rdd: RDD[Map[K, V]]): MapRDD[K, V] = new MapRDD[K, V](rdd)

  implicit def toStringKeyRDD[T](rdd: RDD[(String, T)]): StringKeyRDD[T] = new StringKeyRDD[T](rdd)

  implicit def toJoinableRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): JoinableRDD[K, V] = new JoinableRDD[K, V](rdd)

  implicit def toRichDataFrame[T <: Product : TypeTag](rdd: RDD[T])(implicit ctx: HiveContext): RichDataFrame = ctx.createDataFrame(rdd)

  implicit def toRichDataFrame(df: DataFrame): RichDataFrame = new RichDataFrame(df)

  implicit def toProductStream[T <: Product : TypeTag](stream: DStream[T])(implicit ctx: HiveContext, es: ExecutorSupplier = runNow): SavingStream[T] = new ProductStream[T](stream)

  implicit def toMapStream[T](stream: DStream[Map[String, T]])(implicit ctx: HiveContext, es: ExecutorSupplier = runNow): SavingStream[Map[String, T]] = new MapStream(stream)

  implicit def toJsonStream(stream: DStream[String])(implicit ctx: HiveContext, es: ExecutorSupplier = runNow): SavingStream[String] = new JsonStream(stream)

  implicit def toRowStream(stream: DStream[Row])(implicit ctx: HiveContext, es: ExecutorSupplier = runNow, schema: StructType): SavingStream[Row] = new RowStream(stream)


  implicit def toRichSparkContext(sc: SparkContext): RichSparkContext = new RichSparkContext(sc)

  implicit def toRichStreamingContext(ssc: StreamingContext): RichStreamingContext = new RichStreamingContext(ssc)

}
