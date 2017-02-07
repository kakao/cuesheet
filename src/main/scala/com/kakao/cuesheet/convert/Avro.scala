package com.kakao.cuesheet.convert

import org.apache.avro.generic.{GenericArray, GenericRecord}
import org.apache.avro.io.{DatumReader, DecoderFactory}
import org.apache.avro.util.Utf8

import scala.collection.JavaConversions._
import scala.collection.immutable.ListMap

object Avro {

  /** @return a function that parses Avro bytes to a [[GenericRecord]], according to the schema */
  def recordDecoder(reader: DatumReader[GenericRecord]): Array[Byte] => GenericRecord = bytes => {
    val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    reader.read(null, decoder)
  }

  /** Converts an Avro record to Scala Map. easier to use than Avro's jasonEncoder,
    * which produces {"field":{"string":"value"}} rather than {"field": "value"} */
  def toMap(record: GenericRecord): Map[String, Any] = {
    ListMap(record.getSchema.getFields.map { field =>
      val key = field.name()
      val value = unwrap(record.get(key))
      (key, value)
    }: _*)
  }

  /** Convert the Avro object types into simple types (String, Long, Seq, Map, etc.),
    * so that Jackson and others can understand them easily */
  def unwrap(obj: Any): Any = obj match {
    case utf8: Utf8 => utf8.toString
    case array: GenericArray[_] => array.map(unwrap)
    case nested: GenericRecord => toMap(nested)
    case other => other
  }

  /** Convert an Avro record to JSON string */
  def toJson(record: GenericRecord): String = com.kakao.mango.json.toJson(toMap(record))

  // kafkaParam keys for AvroKafkaDecoder

  /** specify schema in kafkaParams */
  val SCHEMA = "cuesheet.avro.schema"

  /** specify the number of header bytes to skip per record, in kafkaParams */
  val SKIP_BYTES = "cuesheet.avro.skipBytes"

}
