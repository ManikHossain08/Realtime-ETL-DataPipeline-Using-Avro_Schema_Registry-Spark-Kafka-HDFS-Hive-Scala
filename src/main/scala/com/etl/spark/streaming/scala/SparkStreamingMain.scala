package com.etl.spark.streaming.scala

import com.etl.spark.streaming.scala.configurations.{Base, HadoopClientConfig, KafkaConsumerConfig, SparkAppConfig}
import org.apache.spark.sql.{DataFrame, SaveMode}

object SparkStreamingMain extends App with SparkAppConfig with KafkaConsumerConfig with HadoopClientConfig with Base {

  import spark.implicits._

  StagingAndSchemaDeploy.putFilesToHDFS()
  val enrichedStationInfoDF: DataFrame = spark.read
    .option("header", "true")
    .schema(StationInformation.stationSchema)
    .csv(s"$stagingDir/station_information/enriched_station_information.csv")
  enrichedStationInfoDF.createOrReplaceTempView("tblEnrichedStationInfo")


  kafkaTripsStream
    .map(_.value())
    .foreachRDD { stopTimeRDD =>
      val stopTimeDf = stopTimeRDD.map(Trip(_)).toDF()
      stopTimeDf.createOrReplaceTempView("tblTrips")
      val enrichedTrips = spark.sql(
        """SELECT
          |   *
          |   from tblTrips
          |   cross join tblEnrichedStationInfo
          |""".stripMargin
      )
      partitionAndStoreToHdfs_Soln1(enrichedTrips)
    }

  ssc.start()
  ssc.awaitTermination()

  def partitionAndStoreToHdfs_Soln1(enrichedTripsDS: DataFrame): Unit = {
    enrichedTripsDS
      .write
      .mode(SaveMode.Append)
      .csv(s"$stagingDir/enriched_trips")
  }
}
