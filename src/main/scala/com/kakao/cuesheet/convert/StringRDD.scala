package com.kakao.cuesheet.convert

import org.apache.spark.rdd.RDD

class StringRDD(rdd: RDD[String]) extends SaveToES(rdd)
