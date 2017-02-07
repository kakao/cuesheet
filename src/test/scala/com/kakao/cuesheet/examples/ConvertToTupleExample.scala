package com.kakao.cuesheet.examples

import com.kakao.cuesheet.CueSheet
import com.kakao.cuesheet.examples.util.ExampleData
import org.apache.spark.rdd.RDD

object ConvertToTupleExample extends CueSheet {{

  val rdd: RDD[Array[String]] = sc.textFile(ExampleData.path).map(_.split("\t"))

  val tuples = rdd.convertTo[(Int, String, Double)]

  tuples.collect().foreach {
    case (id, key, score) => println(s"[$id] $key => $score")
  }

  sc.stop()

}}
