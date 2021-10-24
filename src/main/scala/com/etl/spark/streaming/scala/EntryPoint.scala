package com.etl.spark.streaming.scala

import com.etl.spark.streaming.scala.configurations.{Base, HadoopClientConfig, KafkaConfig, SparkAppConfig}
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.sql.DataFrame
import scala.collection.JavaConverters._

object EntryPoint extends App with SparkAppConfig with KafkaConfig with HadoopClientConfig with Base {

  import spark.implicits._

  AutoStaging.putFilesToHDFS()
  //EnrichedTripsSchemaRegistry.registerAvroSchema()

  val enrichedStationInfoDF = spark.read
    .option("header", value = "true")
    .schema(StationInformation.stationInfoSchema)
    .csv(s"$stagingDir/station_information/enriched_station_information.csv")
  enrichedStationInfoDF.createOrReplaceTempView("tblEnrichedStationInfo")

  kafkaConsumerStream
    .map(_.value())
    .foreachRDD { stopTimeRDD =>
      val tripsDF = stopTimeRDD.map(Trip(_)).toDF()
      tripsDF.createOrReplaceTempView("tblTrip")
      val enrichedTripsDF = spark.sql(
        """
          |   SELECT * from tblTrip
          |   cross join tblEnrichedStationInfo
          |""".stripMargin
      )
      produceKafkaMsgFromGenericRecords(enrichedTripsDF)
    }

  ssc.start()
  ssc.awaitTermination()
  kafkaProducer.flush()
  kafkaProducer.close()

  def produceKafkaMsgFromGenericRecords(enrichedTripsDF: DataFrame): Unit = {

    val enrichedTripsList: List[String] =
      enrichedTripsDF
        .as[EnrichedTrip]
        .map(obj => EnrichedTrip.toCsv(obj))
        .collectAsList()
        .asScala
        .toList

    val enrichedTripGenericRecordList = enrichedTripsList
      .map(_.split(",", -1))
      .map { fields =>
        new GenericRecordBuilder(AvroSchemaRegistry.enrichedTripSchema)
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

    enrichedTripGenericRecordList
      .map(record => new ProducerRecord[String, GenericRecord]("test3_enriched_trip",
        record.get("station_id").toString, record)
      ).foreach(kafkaProducer.send)
  }
}
