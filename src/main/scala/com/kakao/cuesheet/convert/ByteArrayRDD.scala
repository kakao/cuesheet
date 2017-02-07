package com.kakao.cuesheet.convert

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.spark.rdd.RDD

class ByteArrayRDD(rdd: RDD[Array[Byte]]) {

  /** [[Schema]] is not Serializable, so make it a JSON record */
  def parseAvro(schema: Schema): RDD[GenericRecord] = parseAvro(schema.toString)

  def parseAvro(schemaJson: String): RDD[GenericRecord] = {
    rdd.mapPartitions { partition =>
      val schema = new Parser().parse(schemaJson)
      val reader = new GenericDatumReader[GenericRecord](schema)
      partition.map(Avro.recordDecoder(reader))
    }
  }

  def parseAvroToMap(schema: Schema): RDD[Map[String, Any]] = parseAvro(schema).map(Avro.toMap)

  def parseAvroToMap(schema: String): RDD[Map[String, Any]] = parseAvro(schema).map(Avro.toMap)

  def parseAvroToJson(schema: Schema): RDD[String] = parseAvro(schema).map(Avro.toJson)

  def parseAvroToJson(schema: String): RDD[String] = parseAvro(schema).map(Avro.toJson)

}
