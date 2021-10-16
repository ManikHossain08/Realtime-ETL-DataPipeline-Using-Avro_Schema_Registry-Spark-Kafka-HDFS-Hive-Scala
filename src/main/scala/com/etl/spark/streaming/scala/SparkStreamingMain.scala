package com.etl.spark.streaming.scala

import com.etl.spark.streaming.scala.configurations.{Base, HadoopClientConfig, KafkaConsumerConfig, SparkAppConfig}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{col, current_timestamp, window}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import java.util.UUID

object SparkStreamingMain extends App with SparkAppConfig with KafkaConsumerConfig with HadoopClientConfig with Base {

  StagingAndSchemaDeploy.putFilesToHDFS()

  import spark.implicits._

  BatchEnrichment.enrichedTripsDF.createOrReplaceTempView("tblEnrichedTrip")

  kafkaStopTimeStream
    .map(_.value())
    .foreachRDD { stopTimeRDD =>
      val stopTimeDf = stopTimeRDD.map(StopTime(_)).toDF()
      stopTimeDf.createOrReplaceTempView("tblStopTime")
      val enrichedStopTime = spark.sql(
        """SELECT
          |   et.tripId,
          |   et.serviceId,
          |   et.routeId,
          |   et.tripHeadSign,
          |   et.date,
          |   et.exceptionType,
          |   et.routeLongName,
          |   et.routeColor,
          |   st.arrivalTime,
          |   st.departureTime,
          |   st.stopId,
          |   st.stopSequence,
          |   et.wheelchairAccessible
          | from tblStopTime st
          | left join tblEnrichedTrip et
          | on st.tripId = et.tripId
          |""".stripMargin
      )
      partitionAndStoreToHdfs_Soln1(enrichedStopTime)
      //partitionAndStoreToHdfs_Soln2(enrichedStopTime.as[EnrichedStopTime])
      //windowing30Secs(enrichedStopTime.as[EnrichedStopTime])
    }

  ssc.start()
  ssc.awaitTermination()

  /**
   * -----TWO SOLUTIONS FOR PARTITION TABLE FOR HIVE ON THE LOCATION HDFS DATA WAREHOUSE----
   *
   * IMPORTANT NOTE: SOLUTION-1
   * This is better solution but in this case huge amount of files will be created on the HDFS,
   * so we have to do some mechanism for concatenating the HDFS files into less #files.
   * I do not know the mechanism yet but I have to learn it for future use and
   * in the professional career.
   */
  def partitionAndStoreToHdfs_Soln1(enrichedStopTimeDS: DataFrame): Unit = {
    enrichedStopTimeDS
      .write
      .partitionBy("wheelchairAccessible")
      .mode(SaveMode.Append)
      .csv(s"$stagingDir/enriched_stop_time")
  }

  /**
   * IMPORTANT NOTE: SOLUTION-2
   * This is good solution but dangerous in terms of distribution of works between the executors, in this case,
   * if the manual work does not work perfectly then some executors may be remained idle.
   * be careful and avoid this kind of manual implementation. Although I recover something
   * using the "foreachPartition" API that will distribute the total amount of works
   * then the partition on each executor will work and no executor will not be remain idle.
   */
  def partitionAndStoreToHdfs_Soln2(enrichedStopTimeDS: Dataset[EnrichedStopTime]): Unit = {
    enrichedStopTimeDS
      .rdd
      .foreachPartition { enrichedStopTimePar =>
        val enrichedStopTimes = enrichedStopTimePar.toList.groupBy(_.wheelchairAccessible)

        enrichedStopTimes.foreach { case (partitionId, enrichedStopTimes) =>
          val partitionDir = new Path(s"$stagingDir/enriched_stop_time/wheelchair_accessible=$partitionId")
          if (!fileSystem.exists(partitionDir)) fileSystem.mkdirs(partitionDir)
          val outFilePath =
            new Path(s"$stagingDir/enriched_stop_time/wheelchair_accessible=$partitionId/${UUID.randomUUID()}")

          val outputStream = fileSystem.create(outFilePath)
          enrichedStopTimes
            .foreach { enrichedStopTime =>
              outputStream.write(EnrichedStopTime.toCsv(enrichedStopTime).getBytes)
              outputStream.write("\n".getBytes)
            }
          outputStream.close()
        }
      }
  }

  /**
   * This method will count the wheelchairAccessible in a 5 seconds of sliding windows and 10 seconds of
   * window duration. Finally print the result onto the console using the last statement.
   * We can write these result table anywhere such as database, in parquet, json csv any kind of file format
   * we can store those data.
   *
   */
  def windowing30Secs(enrichedStopTime: Dataset[EnrichedStopTime]) :Unit = {
    val enrichedStopTimeDS = enrichedStopTime.withColumn("eventTimeStamp", current_timestamp())
    val countsWA = enrichedStopTimeDS.groupBy(window(col("eventTimeStamp"),
    "30 seconds", "5 seconds"), col("wheelchairAccessible")).count()
    val sortedWA = countsWA.orderBy(col("count").desc)
    val query = sortedWA.writeStream.outputMode("complete").format("console")
    .queryName("wheelChairCounts").start()
    println(query)
  }
}
