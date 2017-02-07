package com.kakao.cuesheet.examples

import com.kakao.cuesheet.CueSheet
import com.kakao.mango.text.ThreadSafeDateFormat

/** foreachBatch will run a function every batch */
object SchedulingExample extends CueSheet {{
  val format = ThreadSafeDateFormat("yyyy-MM-dd HH:mm:ss")
  ssc.foreachBatch { time =>
    println(format(time) + " : " + sc.parallelize(0 until 100).map(_ + 1).sum())
  }
}}
