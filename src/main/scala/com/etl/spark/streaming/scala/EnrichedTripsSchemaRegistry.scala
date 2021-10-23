package com.etl.spark.streaming.scala

import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import java.io.InputStream
import scala.io.Source

object EnrichedTripsSchemaRegistry {

  val subjectName: String = "bdsf2001_manik_enriched_trip1"

  def registerAvroSchema(): Unit = {

    val enrichedTripsAvscFromFile: InputStream = getClass.getResourceAsStream("/enriched_trip.avsc")
    val enrichedTripsAvscFromFileStr: String =
      Source
        .fromInputStream(enrichedTripsAvscFromFile)
        .getLines().mkString("\n")

    val enrichedTripParsedAvsc: Schema = new Schema.Parser().parse(enrichedTripsAvscFromFileStr)
    val srClient = new CachedSchemaRegistryClient("http://localhost:8081", 10)
    srClient.register(subjectName, new AvroSchema(enrichedTripParsedAvsc).asInstanceOf[ParsedSchema])
  }
}