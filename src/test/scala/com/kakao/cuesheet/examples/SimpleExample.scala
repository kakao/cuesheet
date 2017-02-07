package com.kakao.cuesheet.examples

import com.kakao.cuesheet.CueSheet

object SimpleExample extends CueSheet {{
  val rdd = sc.parallelize(1 to 100)
  println(s"sum = ${rdd.sum()}")
  println(s"sum2 = ${rdd.map(_ + 1).sum()}")
}}
