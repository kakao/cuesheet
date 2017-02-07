package com.kakao.cuesheet.examples

import com.kakao.cuesheet.CueSheet

/** mapToTabSeparatedValues will convert any records to TSV, resulting in RDD[String];
  * overwriteAsTabSeparatedValues will save any records as TSV */
object TabSeparatedValuesExample extends CueSheet {{

  val rdd = sc.parallelize(Seq[(Int, (String, Double))](
    (1, ("hello", 0.23)),
    (2, ("world", 0.34)),
    (3, ("foo", 0.45)),
    (4, ("bar", 0.56))
  ))

  rdd.mapToTabSeparatedValues.collect().foreach(println)
  rdd.overwriteAsTabSeparatedValues("example.tsv")

  sc.stop()

}}
