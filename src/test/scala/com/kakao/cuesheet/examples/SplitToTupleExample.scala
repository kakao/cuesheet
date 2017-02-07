package com.kakao.cuesheet.examples

import com.kakao.cuesheet.CueSheet
import com.kakao.cuesheet.examples.util.ExampleData
import org.apache.spark.rdd.RDD

object SplitToTupleExample extends CueSheet {{

  val file = sc.textFile(ExampleData.path)

  val tuples: RDD[(Int, String, Double)] = file.splitTo[(Int, String, Double)]

  tuples.collect().foreach {
    case (id, key, score) => println(s"[$id] $key => $score")
  }

  sc.stop()

}}
