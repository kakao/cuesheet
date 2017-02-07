package com.kakao.cuesheet.accumulator

import com.kakao.mango.concurrent.NamedSingletons
import org.apache.spark.AccumulatorParam.{DoubleAccumulatorParam, FloatAccumulatorParam, LongAccumulatorParam, IntAccumulatorParam}
import org.apache.spark._

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap => MutableHashMap}
import spire.math.Numeric

/** Accumulator factory manages multiple accumulators indexed by String keys */
object AccumulatorFactory {

  def int(sc: SparkContext, initialValue: Int = 0) = new AccumulatorFactory[Int](sc, initialValue)(IntAccumulatorParam)

  def long(sc: SparkContext, initialValue: Long = 0L) = new AccumulatorFactory[Long](sc, initialValue)(LongAccumulatorParam)

  def float(sc: SparkContext, initialValue: Float = 0.0f) = new AccumulatorFactory[Float](sc, initialValue)(FloatAccumulatorParam)

  def double(sc: SparkContext, initialValue: Double = 0.0) = new AccumulatorFactory[Double](sc, initialValue)(DoubleAccumulatorParam)

  def hash[K, V: Numeric](sc: SparkContext) = {
    implicit val param = HashMapParam[K, V]()
    new AccumulableFactory[MutableHashMap[K, V], (K, V)](sc, MutableHashMap())
  }

}

trait Accumulation[A] {
  self: NamedSingletons[_] =>

  def values(): Map[String, A]
  def clear(): Unit = registry.clear()
}

class AccumulatorFactory[T: AccumulatorParam](sc: SparkContext, initialValue: T) extends NamedSingletons[Accumulator[T]] with Accumulation[T] {
  override def newInstance(key: String): Accumulator[T] = sc.accumulator(initialValue)
  override def values(): Map[String, T] = Map(registry.asScala.mapValues(_.value).toSeq: _*)
}

class AccumulableFactory[R, T](sc: SparkContext, initialValue: R)(implicit param: AccumulableParam[R, T]) extends NamedSingletons[Accumulable[R, T]] with Accumulation[R] {
  override def newInstance(key: String): Accumulable[R, T] = sc.accumulable(initialValue)
  override def values(): Map[String, R] = Map(registry.asScala.mapValues(_.value).toSeq: _*)
}
