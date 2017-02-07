package com.kakao.cuesheet.convert

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

class SplittableRDD[T](rdd: RDD[T]) {

  def splitTo[U: ClassTag : TypeTag]: RDD[U] = splitTo[U]("\t")

  def splitTo[U: ClassTag : TypeTag](delimiter: String): RDD[U] = {
    val converter = RecordConverter[U]
    rdd.map(_.toString.split(delimiter, -1).toSeq).map(converter)
  }

}
