package com.kakao.cuesheet.examples

import com.kakao.cuesheet.CueSheet
import com.kakao.cuesheet.examples.util.{ExampleData, Row}
import org.apache.spark.rdd.RDD

/** convertTo will convert any sequence-like records into case classes, etc. */
object ConvertToCaseClassExample extends CueSheet {{

  val rdd: RDD[Array[String]] = sc.textFile(ExampleData.path).map(_.split("\t"))

  val rows = rdd.convertTo[Row]

  rows.collect().foreach {
    case Row(id, key, score) => println(s"[$id] $key => $score")
  }

  sc.stop()

}}
