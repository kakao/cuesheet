package com.kakao.cuesheet.convert

import org.apache.spark.rdd.RDD
import com.kakao.mango.json._
import scala.reflect._
import scala.reflect.runtime.universe._


class ConvertibleRDD[T <: Seq[_]](rdd: RDD[T]) {

  def convertTo[U: ClassTag : TypeTag]: RDD[U] = {
    val converter = RecordConverter[U]
    rdd.map(converter)
  }

  def json: RDD[String] = {
    rdd.map(toJson)
  }

}
