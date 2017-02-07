package com.kakao.cuesheet.convert

import java.io.FileNotFoundException

import com.github.nscala_time.time.Imports._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class RichSparkContext(val sc: SparkContext) extends HBaseReaders {

  private val defaultDateFormat = "yyyy-MM-dd"

  private def toDateTime(dateString: String, dateFormat: String = defaultDateFormat) = {
    dateString.toLowerCase match {
      case "today" =>
        DateTime.now
      case "yesterday" =>
        DateTime.yesterday
      case s =>
        DateTimeFormat.forPattern(dateFormat).parseDateTime(s)
    }
  }

  /**
   * for example,
   *
   *    sc.latestTextFile("/aa/bb", "2015-01-15", 0)
   *
   *    try to read "/aa/bb/2015-01-15"
   *    if "/aa/bb/2015-01-15" does not exist, raise FileNotFoundException
   *
   *    sc.latestTextFile("/aa/bb", "2015-01-15", 1)
   *
   *    try to read "/aa/bb/2015-01-15"
   *    if "/aa/bb/2015-01-15" does not exist
   *    try to read "/aa/bb/2015-01-14"
   *    if "/aa/bb/2015-01-14" does not exits, raise FileNotFoundException
   *
   *    sc.latestTextFile("/aa/bb", "2015-01-15", 2)
   *    try to read "/aa/bb/2015-01-15"
   *    try to read "/aa/bb/2015-01-14" 1
   *    try to read "/aa/bb/2015-01-13" 2
   *
   */
  def latestTextFile(
      rootPath: String,
      date: String,
      maxLookupDays: Int,
      dateFormat: String = defaultDateFormat,
      minPartitions: Int = sc.defaultMinPartitions,
      delimiter: String = "/",
      depth: Int = 1): RDD[String] = {

    require(maxLookupDays >= 0)

    var epoch = 0
    var hit = false
    var latestRdd: RDD[String] = null

    while(!hit && epoch <= maxLookupDays) {
      try {
        val dt = toDateTime(date, dateFormat).minusDays(epoch)
        latestRdd = elasticTextFile(rootPath, dt.toString(dateFormat), 0, 0, true, dateFormat, minPartitions, delimiter, depth)
        hit = true
      } catch {
        case e: FileNotFoundException =>
          epoch += 1
      }
    }

    if(latestRdd == null)
      throw new FileNotFoundException(s"$rootPath$delimiter$date and maxTrial is $maxLookupDays")

    latestRdd
  }

  /**
   * for example,
   *
   *    sc.elasticTextFile("/aa/bb", "2015-01-15")
   *
   *      /aa/bb/2015-01-15 today
   *
   *    sc.elasticTextFile("/aa/bb", "2015-01-15", 1, 2)
   *
   *      /aa/bb/2015-01-14 before 1
   *      /aa/bb/2015-01-15 today
   *      /aa/bb/2015-01-16 after 1
   *      /aa/bb/2015-01-17 after 2
   *
   *    @param strict whether to emit FileNotFoundException on nonexistent path
   *
   *    for Hive table partitioned by date_id?
   *
   *    sc.elasticTextFile("/aa/bb/date_id", "2015-01-15", 1, 2, delimitor = "=")
   *
   *      /aa/bb/date_id=2015-01-14 before 1
   *      /aa/bb/date_id=2015-01-15 today
   *      /aa/bb/date_id=2015-01-16 after 1
   *      /aa/bb/date_id=2015-01-17 after 2
   *
   */
  def elasticTextFile(
      rootPath: String,
      date: String,
      before: Int = 0,
      after: Int = 0,
      strict: Boolean = true,
      dateFormat: String = defaultDateFormat,
      minPartitions: Int = sc.defaultMinPartitions,
      delimiter: String = "/",
      depth: Int = 1): RDD[String] = {
    require(before >= 0)
    require(after >= 0)

    val dt = toDateTime(date, dateFormat).minusDays(before)
    val duration = before + after

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    val paths = (0 to duration).map {
      i =>
        val d = rootPath + delimiter + dt.plusDays(i).toString(dateFormat)
        val path = new Path(d)
        val existence = fileSystem.exists(path) && fileSystem.getContentSummary(path).getLength > 0 // exists and file size is not 0
        val depthApplied = if(depth <= 1) d else d + "/*" * depth
        (existence, depthApplied)
    }

    val inputPaths = paths.filter(_._1 == true).map(_._2).mkString(",")
    val notFoundPaths = paths.filter(_._1 == false).map(_._2).mkString(",")

    if((strict && notFoundPaths.length > 0) || inputPaths.length == 0)
      throw new FileNotFoundException(notFoundPaths)

    sc.textFile(inputPaths, minPartitions)
  }

  def mapElasticTextFile[T](
      rootPath: String,
      date: String,
      before: Int = 0,
      after: Int = 0,
      strict: Boolean = false,
      dateFormat: String = defaultDateFormat,
      minPartitions: Int = sc.defaultMinPartitions,
      delimiter: String = "/",
      depth: Int = 1)(func: (String, RDD[String]) => RDD[T]): RDD[T] = {

    require(before >= 0)
    require(after >= 0)

    val dt = toDateTime(date, dateFormat).minusDays(before)
    val duration = before + after

    var rddUnion: RDD[T] = null
    (0 to duration).foreach {
      i =>
        val date = dt.plusDays(i).toString(dateFormat)
        try {
          val rdd = sc.elasticTextFile(rootPath, date,
            strict = true, dateFormat = dateFormat, minPartitions = minPartitions, delimiter = delimiter, depth = depth)
          if(rddUnion == null)
            rddUnion = func(date, rdd)
          else
            rddUnion = rddUnion.union(func(date, rdd))
        } catch {
          case ex: FileNotFoundException =>
            println(s"WARN: No such ${ex.getMessage}")
        }
    }
    rddUnion
  }

  def foreachElasticTextFile(
      rootPath: String,
      date: String,
      before: Int = 0,
      after: Int = 0,
      strict: Boolean = false,
      dateFormat: String = defaultDateFormat,
      minPartitions: Int = sc.defaultMinPartitions,
      delimiter: String = "/",
      depth: Int = 1)(func: (String, RDD[String]) => Unit): Unit = {

    require(before >= 0)
    require(after >= 0)

    val dt = toDateTime(date, dateFormat).minusDays(before)
    val duration = before + after

    (0 to duration).foreach {
      i =>
        val date = dt.plusDays(i).toString(dateFormat)
        try {
          val rdd = sc.elasticTextFile(rootPath, date,
            strict = true, dateFormat = dateFormat, minPartitions = minPartitions, delimiter = delimiter, depth = depth)
          func(date, rdd)
        } catch {
          case ex: FileNotFoundException =>
            println(s"WARN: No such ${ex.getMessage}")
        }
    }
  }

}

