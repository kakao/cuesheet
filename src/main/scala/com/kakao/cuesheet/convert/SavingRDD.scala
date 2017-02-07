package com.kakao.cuesheet.convert

import java.io.{OutputStream, OutputStreamWriter, PrintWriter, BufferedOutputStream}
import java.net.URI

import com.kakao.mango.io.AutoClosing
import com.kakao.mango.logging.Logging
import com.kakao.mango.util.Conversions
import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.control.NonFatal

object SavingRDD extends Logging {

  def fileSystem(sc: SparkContext, path: String = ""): FileSystem = {
    if (path.startsWith("hdfs://")) {
      FileSystem.get(new URI(path), sc.hadoopConfiguration)
    } else {
      FileSystem.get(sc.hadoopConfiguration)
    }
  }

  def delete(sc: SparkContext, path: String): Unit = {
    val hdfs = fileSystem(sc, path)
    val success = try {
      hdfs.delete(new Path(path), true)
    } catch {
      case NonFatal(e) => logger.error(s"Exception occurred while deleting $path", e); false
    }
    if (!success) logger.warn(s"Could not delete $path, proceeding anyway")
  }

  val TAB = "\t"

  def split(record: Any): TraversableOnce[Any] = record match {
    case record: Array[_] => record.flatMap(split)
    case record: CharSequence => Some(record)
    case record: Product => record.productIterator.flatMap(split)
    case record: TraversableOnce[_] => record.flatMap(split)
    case null => Some("")
    case _ => Some(record)
  }

}

/** Saving functions that allows overwriting on files, unlike RDD's saveAs functions
  * Additionally, can save RDD of Array, Seq, Tuple, Case Class types to delimited text files
  */
class SavingRDD[T](rdd: RDD[T]) extends Conversions {

  import com.kakao.cuesheet.convert.SavingRDD._

  def mapToTabSeparatedValues: RDD[String] = mapToDelimiterSeparatedValues(TAB)

  def mapToDelimiterSeparatedValues(delimiter: String): RDD[String] = rdd.map(split(_).mkString(delimiter))

  def saveAsTabSeparatedValues(path: String): Unit = saveAsDelimiterSeparatedValues(TAB, path)

  def saveAsTabSeparatedValues(path: String, codec: Class[_ <: CompressionCodec]): Unit = {
    saveAsDelimiterSeparatedValues(TAB, path, codec)
  }

  def saveAsDelimiterSeparatedValues(delimiter: String, path: String): Unit = {
    mapToDelimiterSeparatedValues(delimiter).saveAsTextFile(path)
  }

  def saveAsDelimiterSeparatedValues(delimiter: String, path: String, codec: Class[_ <: CompressionCodec]): Unit = {
    mapToDelimiterSeparatedValues(delimiter).saveAsTextFile(path, codec)
  }

  def overwriteAsTabSeparatedValues(path: String): Unit = {
    overwriteAsDelimiterSeparatedValues(TAB, path)
  }

  def overwriteAsTabSeparatedValues(path: String, codec: Class[_ <: CompressionCodec]): Unit = {
    overwriteAsDelimiterSeparatedValues(TAB, path, codec)
  }

  def overwriteAsDelimiterSeparatedValues(delimiter: String, path: String): Unit = {
    delete(rdd.context, path)
    saveAsDelimiterSeparatedValues(delimiter, path)
  }

  def overwriteAsDelimiterSeparatedValues(delimiter: String, path: String, codec: Class[_ <: CompressionCodec]): Unit = {
    delete(rdd.context, path)
    saveAsDelimiterSeparatedValues(delimiter, path, codec)
  }

  def overwriteAsTextFile(path: String): Unit = {
    delete(rdd.context, path)
    rdd.saveAsTextFile(path)
  }

  def overwriteAsTextFile(path: String, codec: Class[_ <: CompressionCodec]): Unit = {
    delete(rdd.context, path)
    rdd.saveAsTextFile(path, codec)
  }

  def overwriteAsObjectFile(path: String): Unit = {
    delete(rdd.context, path)
    rdd.saveAsObjectFile(path)
  }

  def saveAsSingleFile(output: OutputStream, delimiter: String): Unit = {
    val data = rdd.map(split(_).mkString(delimiter)).collect()
    AutoClosing(output) { output =>
      for (line <- data) {
        output.write(line.bytes)
        output.write('\n')
      }
    }
  }

  def saveAsSingleFile(output: OutputStream): Unit = saveAsSingleFile(output, "\t")

  def saveAsSingleFile(path: String, codec: Class[_ <: CompressionCodec with Configurable], delimiter: String = "\t", overwrite: Boolean = true): Unit = {
    val file = fileSystem(rdd.context, path).create(new Path(path), overwrite)
    val instance = codec.newInstance
    instance.setConf(rdd.sparkContext.hadoopConfiguration)
    val out = instance.createOutputStream(new BufferedOutputStream(file))
    saveAsSingleFile(out, delimiter)
  }

  def saveAsSingleFile(path: String, delimiter: String, overwrite: Boolean): Unit = {
    val file = fileSystem(rdd.context, path).create(new Path(path), overwrite)
    saveAsSingleFile(new BufferedOutputStream(file), delimiter)
  }

  def saveAsSingleFile(path: String, delimiter: String): Unit = saveAsSingleFile(path, delimiter, overwrite = true)

  def saveAsSingleFile(path: String): Unit = saveAsSingleFile(path, "\t")

}
