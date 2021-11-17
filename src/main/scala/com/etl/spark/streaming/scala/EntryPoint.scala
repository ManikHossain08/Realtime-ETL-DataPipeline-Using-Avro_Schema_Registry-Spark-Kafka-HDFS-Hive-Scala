package com.etl.spark.streaming.scala

import com.etl.spark.streaming.scala.configurations.{Base, HadoopConfig, KafkaConfig, SparkConfig}
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.kafka.clients.producer.ProducerRecord

object EntryPoint extends App with SparkConfig with KafkaConfig with HadoopConfig with Base {

  import spark.implicits._

  spark
    .read
    .format("parquet")
    .load(s"$stagingDir/enriched_station_information")
    .createOrReplaceTempView("tblEnrichedStationInfo")

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
      ).collect()

      val enrichedTripGenericRecordList = enrichedTripsDF
        .map { fields =>
          new GenericRecordBuilder(AvroSchemaRegistry.enrichedTripAvroSchema)
            .set("start_date", fields(0))
            .set("start_station_code", fields(1))
            .set("end_date", fields(2))
            .set("end_station_code", fields(3))
            .set("duration_sec", fields(4))
            .set("is_member", fields(5))
            .set("system_id", fields(6))
            .set("timezone", fields(7))
            .set("station_id", fields(8))
            .set("name", fields(9))
            .set("short_name", fields(10))
            .set("lat", fields(11))
            .set("lon", fields(12))
            .set("capacity", fields(13))
            .build()
        }.toList

      enrichedTripGenericRecordList
        .map(record => new ProducerRecord[String, GenericRecord]("bdsf2001_manik_enriched_trip",
          record.get("station_id").toString, record)
        ).foreach(kafkaProducer.send)
    }

  ssc.start()
  ssc.awaitTermination()
  kafkaProducer.flush()
  kafkaProducer.close()

}

/**
 * Using EnrichedTrip case class we are making the complex which is also difficult to debug as well
 * so do not use collect in code which cause performance issue overall. follow this approach from line #30-59 instead
 * of using the following line which I have use before.
 *
 * Bellow code snippet is problematic in terms of performance and debugging efficiency and extra effort to use
 * another case class. After this coding implementation we can remove the case class of EnrichedTrip and companion
 * object as well.
 *
 * /**
 *  val enrichedTripsList: List[String] =
      enrichedTripsDF
        .as[EnrichedTrip]
        .map(obj => EnrichedTrip.toCsv(obj))
        .collectAsList()
        .asScala
        .toList   // COMMENT: TO MUCH PROBLEMATIC CODE BECAUSE WE USE CASE CLASS THEN USE COLLECT

    val enrichedTripGenericRecordList = enrichedTripsList
      .map(_.split(",", -1))  // HERE WE AGAIN DID SOME SPLITTING WHICH IS NOT GOOD SO USING NEW CODE ABOVE
      .map { fields =>       // WE SOLVE THIS KIND OF PROBLEM COMPLETELY.
        new GenericRecordBuilder(AvroSchemaRegistry.enrichedTripAvroSchema)
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
 *
 * */
 *
 */
