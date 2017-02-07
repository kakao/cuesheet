package com.kakao.cuesheet.convert

import org.apache.spark.rdd.RDD

class MapRDD[K, V](rdd: RDD[Map[K, V]]) extends SaveToES(rdd)
