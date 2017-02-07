package com.kakao.cuesheet.convert

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.spark.rdd.RDD

class StringByteArrayRDD(rdd: RDD[(String, Array[Byte])]) {

  /** Convert to JSON because [[Schema]] is not Serializable */
  def parseAvro(schema: Schema): RDD[(String, GenericRecord)] = parseAvro(schema.toString)

  def parseAvro(schemaJson: String): RDD[(String, GenericRecord)] = {
    rdd.mapPartitions { partition =>
      val schema = new Parser().parse(schemaJson)
      val reader = new GenericDatumReader[GenericRecord](schema)
      val decoder = Avro.recordDecoder(reader)
      partition.map {
        case (key, value) => (key, decoder(value))
      }
    }
  }

  def parseAvroToMap(schema: Schema): RDD[(String,Map[String, Any])] = parseAvro(schema).mapValues(Avro.toMap)

  def parseAvroToMap(schema: String): RDD[(String,Map[String, Any])] = parseAvro(schema).mapValues(Avro.toMap)

  def parseAvroToJson(schema: Schema): RDD[(String,String)] = parseAvro(schema).mapValues(Avro.toJson)

  def parseAvroToJson(schema: String): RDD[(String,String)] = parseAvro(schema).mapValues(Avro.toJson)

}
