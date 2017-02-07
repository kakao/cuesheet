package com.kakao.cuesheet.convert

import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.spark.sql.types._
import RichDataFrame._

object RichDataFrame {

  /** returns corresponding Hive SQL types for each Spark SQL data type */
  def toHiveType(tpe: DataType): String = tpe match {
    case ByteType => "tinyint"
    case ShortType => "smallint"
    case IntegerType => "int"
    case LongType => "bigint"
    case FloatType => "float"
    case DoubleType => "double"
    case t: DecimalType => "decimal"
    case TimestampType => "timestamp"
    case DateType => "date"
    case StringType => "string"
    case BooleanType => "boolean"
    case BinaryType => "binary"
    case ArrayType(element, nullable) => s"array<${toHiveType(element)}>"
    case MapType(key, value, nullable) => s"map<${toHiveType(key)}, ${toHiveType(value)}>"
    case StructType(fields) => s"struct<${fields.map(f => f.name + ":" + toHiveType(f.dataType)).mkString(", ")}>"
  }

  def toHiveSchema(schema: StructType): String = {
    schema.map { f => s"`${f.name}` ${toHiveType(f.dataType)}" }.mkString("(\n", ",\n", "\n)")
  }

  def toPartitionSchema(partitionColumns: Seq[String]): String = {
    partitionColumns.map { c => c + " string" }.mkString("(\n", ",\n", "\n)")
  }

  def toPartitionSpec(partitions: Seq[(String, String)]): String = {
    partitions.map { case (column, value) => s"$column = '$value'"}.mkString("(\n", ",\n", "\n)")
  }

  def split(table: String): (String, String) = table.split("\\.") match {
    case Array(d, t) => (d, t)
    case Array(t) => ("default", t)
    case _ => throw new IllegalArgumentException(s"Malformed table name: $table")
  }

}

/** provides various functions to save a DataFrame as an external table or partitions */
class RichDataFrame(df: DataFrame) {

  val schema = df.schema
  val sqlContext = df.sqlContext


  def saveAsExternalTable(table: String, path: String, format: String = "orc", mode: SaveMode = SaveMode.ErrorIfExists): Unit = {
    val tableExists = exists(table)

    if (mode == SaveMode.ErrorIfExists && tableExists) {
      throw new RuntimeException(s"table $table already exists")
    }

    df.write.mode(mode).format(format).save(path)

    if (!tableExists) {
      createTable(table, schema, format, path)
    }
  }

  def overwriteAsExternalTable(table: String, path: String, format: String = "orc"): Unit = {
    saveAsExternalTable(table, path, format, SaveMode.Overwrite)
  }

  def appendToExternalTable(table: String, path: String, format: String = "orc"): Unit = {
    saveAsExternalTable(table, path, format, SaveMode.Append)
  }


  def saveAsExternalTablePartition(table: String, path: String, mode: SaveMode, format: String, partitions: (String, String)*): Unit = {
    val columns = partitions.map { case (column, value) => column }

    if (!exists(table)) {
      createPartitionedTable(table, schema, columns, format, path)
    }

    val subPath = partitions.map { case (column, value) => s"$column=$value" }.mkString("/")

    val partitionPath = path.stripSuffix("/") + "/" + subPath

    df.write.mode(mode).format(format).save(partitionPath)

    addPartition(table, partitions, partitionPath)
  }

  def saveAsExternalTablePartition(table: String, path: String, mode: SaveMode, partitions: (String, String)*): Unit = {
    saveAsExternalTablePartition(table, path, mode, "orc", partitions: _*)
  }

  def overwriteAsExternalTablePartition(table: String, path: String, format: String, partitions: (String, String)*): Unit = {
    saveAsExternalTablePartition(table, path, SaveMode.Overwrite, partitions: _*)
  }

  def overwriteAsExternalTablePartition(table: String, path: String, partitions: (String, String)*): Unit = {
    saveAsExternalTablePartition(table, path, SaveMode.Overwrite, partitions: _*)
  }

  def appendToExternalTablePartition(table: String, path: String, format: String, partitions: (String, String)*): Unit = {
    saveAsExternalTablePartition(table, path, SaveMode.Append, format, partitions: _*)
  }

  def appendToExternalTablePartition(table: String, path: String, partitions: (String, String)*): Unit = {
    saveAsExternalTablePartition(table, path, SaveMode.Append, partitions: _*)
  }


  private def exists(table: String): Boolean = {
    val (db, tbl) = split(table)
    sqlContext.tableNames(db).contains(tbl)
  }

  private def createTable(table: String, schema: StructType, format: String, location: String): Unit = {
    sqlContext.sql(s"create external table if not exists $table ${toHiveSchema(schema)} stored as $format location '$location'")
  }

  private def createPartitionedTable(table: String, schema: StructType, partitionColumns: Seq[String], format: String, location: String): Unit = {
    sqlContext.sql(s"create external table if not exists $table ${toHiveSchema(schema)} " +
      s"partitioned by ${toPartitionSchema(partitionColumns)} stored as $format location '$location'")
  }

  private def addPartition(table: String, partitions: Seq[(String, String)], location: String): Unit = {
    val (db, tbl) = split(table)
    sqlContext.synchronized {
      sqlContext.sql(s"use $db")
      sqlContext.sql(s"alter table $tbl add if not exists partition ${toPartitionSpec(partitions)} location '$location'")
    }
  }

}
