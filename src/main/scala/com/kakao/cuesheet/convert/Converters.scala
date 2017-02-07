package com.kakao.cuesheet.convert

import java.util.Date

import com.github.nscala_time.time.Imports._
import com.kakao.mango.text.ThreadSafeDateFormat

import scala.Function.unlift
import scala.util.Try

/** contains various type converters for JDBC types. Pattern match statements catch:
 *
 * Number (Integer, Long, Float, Double, BigDecimal) - NUMERIC, DECIMAL, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, FLOAT, DOUBLE, etc.
 * String - CHAR, VARCHAR, LONGVARCHAR, TEXT, etc.
 * Date (java.sql.{Date, Time, Timestamp}) - TIME, DATE, DATETIME, TIMESTAMP, etc.
 * Boolean - BIT, BOOL, BOOLEAN, etc.
 * Array[Byte] - BINARY, VARBINARY, LONGVARBINARY, BLOB, etc.
 */
object Converters {
  import java.nio.charset.StandardCharsets.UTF_8

  def asInt(any: AnyRef): java.lang.Integer = any match {
    case value: Number => value.intValue
    case value: String => value.toInt
    case value: Date => (value.getTime / 1000).toInt
    case value: java.lang.Boolean => if (value) 1 else 0
    case value: Array[Byte] => new String(value, UTF_8).toInt
  }

  def asLong(any: AnyRef): java.lang.Long = any match {
    case value: Number => value.longValue
    case value: String => value.toLong
    case value: java.util.Date => value.getTime
    case value: java.lang.Boolean => if (value) 1L else 0L
    case value: Array[Byte] => new String(value, UTF_8).toLong
  }

  def asFloat(any: AnyRef): java.lang.Float = any match {
    case value: Number => value.floatValue
    case value: String => value.toFloat
    case value: Date => value.getTime.toFloat
    case value: java.lang.Boolean => if (value) 1.0f else 0.0f
    case value: Array[Byte] => new String(value, UTF_8).toFloat
  }

  def asDouble(any: AnyRef): java.lang.Double = any match {
    case value: Number => value.doubleValue
    case value: String => value.toDouble
    case value: Date => value.getTime.toDouble
    case value: java.lang.Boolean => if (value) 1.0 else 0.0
    case value: Array[Byte] => new String(value, UTF_8).toDouble
  }

  val dateFormat = ThreadSafeDateFormat("yyyyMMdd")
  val timeFormat = ThreadSafeDateFormat("HHmmss")
  val timestampFormat = ThreadSafeDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  val moreFormats = Seq(
    "yyyyMMddHHmmss",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy/mm/dd HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss.SSSZ",   // ISO 8601
    "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"  // ISO 8601
  ).map(ThreadSafeDateFormat.apply)

  val formats = Seq(dateFormat, timeFormat, timestampFormat) ++ moreFormats

  def asString(any: AnyRef): String = any match {
    case value: Number => value.toString
    case value: String => value
    case value: java.sql.Date => dateFormat.format(value)
    case value: java.sql.Time => timeFormat.format(value)
    case value: java.sql.Timestamp => timestampFormat.format(value)
    case value: java.lang.Boolean => if (value) "true" else "false"
    case value: Array[Byte] => new String(value, UTF_8)
  }

  def asDate(any: AnyRef): Date = any match {
    case value: Number => new Date(value.longValue)
    case value: String => formats.collectFirst(unlift(format => Try(format.parse(value)).toOption)).orNull
    case value: Date => value
    case value: java.lang.Boolean => null
    case value: Array[Byte] => asDate(new String(value, UTF_8))
  }

  def asLocalDate(any: AnyRef): LocalDate = any match {
    case value: Number => new LocalDate(value.longValue)
    case value: String => new LocalDate(value)
    case value: Date => LocalDate.fromDateFields(value)
    case value: java.lang.Boolean => null
    case value: Array[Byte] => new LocalDate(new String(value, UTF_8))
  }

  def asLocalDateTime(any: AnyRef): LocalDateTime = any match {
    case value: Number => new LocalDateTime(value.longValue)
    case value: String => new LocalDateTime(value)
    case value: Date => LocalDateTime.fromDateFields(value)
    case value: java.lang.Boolean => null
    case value: Array[Byte] => new LocalDateTime(new String(value, UTF_8))
  }

  object Classes {
    val INT = classOf[Int]
    val JAVA_INT = classOf[java.lang.Integer]
    val LONG = classOf[Long]
    val JAVA_LONG = classOf[java.lang.Long]
    val FLOAT = classOf[Float]
    val JAVA_FLOAT = classOf[java.lang.Float]
    val DOUBLE = classOf[Double]
    val JAVA_DOUBLE = classOf[java.lang.Double]
    val STRING = classOf[String]
    val DATE = classOf[Date]
    val LOCAL_DATE = classOf[LocalDate]
    val LOCAL_DATETIME = classOf[LocalDateTime]
  }

  import Classes._

  def converterTo(clazz: Class[_]): AnyRef => AnyRef = clazz match {
    case INT | JAVA_INT => asInt(_: AnyRef)
    case LONG | JAVA_LONG => asLong(_: AnyRef)
    case FLOAT | JAVA_FLOAT => asFloat(_: AnyRef)
    case DOUBLE | JAVA_DOUBLE => asDouble(_: AnyRef)
    case STRING => asString(_: AnyRef)
    case DATE => asDate(_: AnyRef)
    case LOCAL_DATE => asLocalDate(_: AnyRef)
    case LOCAL_DATETIME => asLocalDateTime(_: AnyRef)
    case _ => (obj: AnyRef) => obj
  }

}
