package com.kakao.cuesheet.convert

import java.util.Arrays.copyOfRange

import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}

/** A Kafka [[Decoder]] implementation for serialized Avro stream
  * Depending on the data type to be parsed to,
  * one can use [[AvroRecordDecoder]], [[AvroMapDecoder]], and [[AvroJsonDecoder]]. */
sealed trait AvroDecoder[T] extends Decoder[T] {

  def props: VerifiableProperties

  protected val schema = new Schema.Parser().parse(props.getString(Avro.SCHEMA))
  protected val skipBytes = props.getInt(Avro.SKIP_BYTES, 0)

  protected val reader = new GenericDatumReader[GenericRecord](schema)
  protected val decoder = Avro.recordDecoder(reader)

  private def skip(bytes: Array[Byte], size: Int): Array[Byte] = {
    val length = bytes.length
    length - size match {
      case remaining if remaining > 0 => copyOfRange(bytes, size, length)
      case _ => new Array[Byte](0)
    }
  }

  def parse(bytes: Array[Byte]): GenericRecord = {
    val data = if (skipBytes == 0) bytes else skip(bytes, skipBytes)
    decoder(data)
  }
}

class AvroRecordDecoder(val props: VerifiableProperties) extends AvroDecoder[GenericRecord] {
  override def fromBytes(bytes: Array[Byte]): GenericRecord = parse(bytes)
}

class AvroMapDecoder(val props: VerifiableProperties) extends AvroDecoder[Map[String, Any]] {
  override def fromBytes(bytes: Array[Byte]): Map[String, Any] = Avro.toMap(parse(bytes))
}

class AvroJsonDecoder(val props: VerifiableProperties) extends AvroDecoder[String] {
  override def fromBytes(bytes: Array[Byte]): String = Avro.toJson(parse(bytes))
}
