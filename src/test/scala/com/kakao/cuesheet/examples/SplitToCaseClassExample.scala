package com.kakao.cuesheet.examples

import com.kakao.cuesheet.CueSheet
import com.kakao.cuesheet.examples.util.{ExampleData, Row}
import org.apache.spark.rdd.RDD

/** splitTo will split tab-separated values into case classes */
object SplitToCaseClassExample extends CueSheet {{

  val file = sc.textFile(ExampleData.path)

  val rows: RDD[Row] = file.splitTo[Row]
  
  rows.collect().foreach {
    case Row(id, key, score) => println(s"[$id] $key => $score")
  }

  sc.stop()

}}
