package com.etl.spark.streaming.scala

import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaMetadata}
import org.apache.avro.Schema
import java.io.InputStream
import scala.io.Source

object AvroSchemaRegistry {

  val subjectName: String = "bdsf2001_manik_enriched_trip-value"
  val srClient = new CachedSchemaRegistryClient("http://localhost:8081", 10)
  val enrichedTripMetadata: SchemaMetadata = srClient.getLatestSchemaMetadata(subjectName)
  val enrichedTripSchema: Schema = srClient.getSchemaById(enrichedTripMetadata.getId).rawSchema().asInstanceOf[Schema]

  def registerAvroSchema(): Unit = {

    val avroSchemaFromFile: InputStream = getClass.getResourceAsStream("/enriched_trip.avsc")
    val avroSchemaStr = Source.fromInputStream(avroSchemaFromFile).getLines().mkString("\n")

    val parsedAvroSchema: Schema = new Schema.Parser().parse(avroSchemaStr)
    srClient.register(subjectName, new AvroSchema(parsedAvroSchema).asInstanceOf[ParsedSchema])
  }
}