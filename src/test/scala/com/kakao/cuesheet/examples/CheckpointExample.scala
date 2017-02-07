package com.kakao.cuesheet.examples

import com.kakao.cuesheet.CueSheet

import scala.concurrent.duration._
import scala.util.Random

object CheckpointExample extends CueSheet(
  "spark.streaming.checkpoint.path" -> "/path/to/checkpoint/directory",
  "spark.streaming.checkpoint.createOnError" -> "true"
) {{

  ssc.generated(100.millis) {
    Random.nextInt()
  }.transform {
    (rdd, time) => rdd.context.parallelize(Seq(time.milliseconds))
  }.print()

}}