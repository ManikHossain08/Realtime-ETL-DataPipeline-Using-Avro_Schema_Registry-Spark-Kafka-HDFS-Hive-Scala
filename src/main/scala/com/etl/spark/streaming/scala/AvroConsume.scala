package com.etl.spark.streaming.scala

import io.confluent.kafka.serializers.{AbstractKafkaSchemaSerDeConfig, KafkaAvroDeserializer}
import org.apache.avro.generic.GenericRecord
import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import scala.collection.JavaConverters._

object AvroConsume extends App {

  val consumerProperties = new Properties()
  consumerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  consumerProperties.setProperty(GROUP_ID_CONFIG, "trip-consumer")
  consumerProperties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProperties.setProperty(AUTO_OFFSET_RESET_CONFIG, "latest")
  consumerProperties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getName)
  consumerProperties.setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")

  val avroConsumer = new KafkaConsumer[String, GenericRecord](consumerProperties)
  avroConsumer.subscribe(List("bdsf2001_manik_enriched_trip").asJava)

  while(true) {
    val polledRecords = avroConsumer.poll(Duration.ofSeconds(1))
    if (!polledRecords.isEmpty) {
      val recordIterator = polledRecords.iterator()
      while(recordIterator.hasNext) {
        val r = recordIterator.next().value()
        println(s"| ${r.get("start_date")} | ${r.get("start_station_code")} | ${r.get("end_date")} | ${r.get("end_station_code")} " +
          s"|${r.get("timezone")}")
      }
    }
  }

}
