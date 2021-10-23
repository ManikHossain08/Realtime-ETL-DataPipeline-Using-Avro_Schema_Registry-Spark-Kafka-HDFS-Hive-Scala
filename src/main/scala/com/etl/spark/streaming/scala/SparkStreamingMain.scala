package com.etl.spark.streaming.scala

import com.etl.spark.streaming.scala.configurations.{Base, HadoopClientConfig, KafkaProducerConsumerConfig, SparkAppConfig}
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord, GenericRecordBuilder}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters.asScalaBufferConverter

object SparkStreamingMain extends App with SparkAppConfig with KafkaProducerConsumerConfig with HadoopClientConfig with Base {

  import spark.implicits._

  StagingAndSchemaDeploy.putFilesToHDFS()
  EnrichedTripsSchemaRegistry.registerAvroSchema()
  val enrichedStationInfoDF: DataFrame = spark.read
    .option("header", "true")
    .schema(StationInformation.stationSchema)
    .csv(s"$stagingDir/station_information/enriched_station_information.csv")
  enrichedStationInfoDF.createOrReplaceTempView("tblEnrichedStationInfo")


  kafkaConsumerStream
    .map(_.value())
    .foreachRDD { stopTimeRDD =>
      val stopTimeDf = stopTimeRDD.map(Trip(_)).toDF()
      stopTimeDf.createOrReplaceTempView("tblTrips")
      val enrichedTrips = spark.sql(
        """
          |   SELECT * from tblTrips
          |   cross join tblEnrichedStationInfo
          |""".stripMargin
      )
      produceEnrichedTripsGenericRecords(enrichedTrips)
    }

  ssc.start()
  ssc.awaitTermination()

  def produceEnrichedTripsGenericRecords(enrichedTripsDF: DataFrame): Unit = {

    val enrichedTripsList: List[String] =
      enrichedTripsDF
        .as[EnrichedTrip]
        .map(obj => EnrichedTrip.toCsv(obj))
        .collectAsList()
        .asScala
        .toList
    enrichedTripsList.foreach(println)


    val srClient = new CachedSchemaRegistryClient("http://localhost:8081", 10)
    val movieMetadata = srClient.getLatestSchemaMetadata(EnrichedTripsSchemaRegistry.subjectName)
    val movieSchema = srClient.getSchemaById(movieMetadata.getId).rawSchema().asInstanceOf[Schema]

    val enrichedTripRecords: List[GenericData.Record] = enrichedTripsList
      .map(_.split(",", -1))
      .map { fields =>
        new GenericRecordBuilder(movieSchema)
          .set("start_date", fields(0))
          .set("start_station_code", fields(1).toInt)
          .set("end_date", fields(2))
          .set("end_station_code", fields(3).toInt)
          .set("duration_sec", fields(4).toInt)
          .set("is_member", fields(5).toInt)
          .set("system_id", fields(6))
          .set("timezone", fields(7))
          .set("station_id", fields(8).toInt)
          .set("name", fields(9))
          .set("short_name", fields(10))
          .set("lat", fields(11).toDouble)
          .set("lon", fields(12).toDouble)
          .set("capacity", fields(13).toInt)
          .build()
      }

    enrichedTripRecords
      .map(record => new ProducerRecord[String, GenericRecord]("test3_enriched_trip",
        record.get("station_id").toString, record)
      ).foreach(kafkaProducer.send)

    kafkaProducer.flush()
    kafkaProducer.close()
  }
}
