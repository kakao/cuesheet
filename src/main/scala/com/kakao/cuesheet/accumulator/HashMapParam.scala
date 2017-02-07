package com.kakao.cuesheet.accumulator

import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.{AccumulableParam, SparkConf}
import spire.math.Numeric

import scala.collection.mutable.{HashMap => MutableHashMap}

/** An implementation of AccumulableParam to use with HashMap[K, V] */
case class HashMapParam[K, V: Numeric]() extends AccumulableParam[MutableHashMap[K, V], (K, V)] {

  private val add = implicitly[Numeric[V]].additive.op _

  def addAccumulator(acc: MutableHashMap[K, V], elem: (K, V)): MutableHashMap[K, V] = {
    val (k1, v1) = elem
    acc += acc.find(_._1 == k1).map {
      case (k2, v2) => k2 -> add(v1, v2)
    }.getOrElse(elem)

    acc
  }

  def addInPlace(acc1: MutableHashMap[K, V], acc2: MutableHashMap[K, V]): MutableHashMap[K, V] = {
    acc2.foreach(elem => addAccumulator(acc1, elem))
    acc1
  }

  def zero(initialValue: MutableHashMap[K, V]): MutableHashMap[K, V] = {
    val ser = new JavaSerializer(new SparkConf(false)).newInstance()
    val copy = ser.deserialize[MutableHashMap[K, V]](ser.serialize(initialValue))
    copy.clear()
    copy
  }
}
