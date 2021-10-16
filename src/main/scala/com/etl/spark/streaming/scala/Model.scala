package com.etl.spark.streaming.scala

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object StationInformation {
  val stationSchema: StructType = StructType(
    List(
      StructField("system_id", StringType, nullable = false),
      StructField("timezone", StringType, nullable = false),
      StructField("station_id", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("short_name", StringType, nullable = true),
      StructField("lat", DoubleType, nullable = true),
      StructField("lon", DoubleType, nullable = true),
      StructField("capacity", IntegerType, nullable = true)
    )
  )
}

case class Trip(start_date: String,
                start_station_code: Int,
                end_date: String,
                end_station_code: String,
                duration_sec: Int,
                is_member: Int
                   )

object Trip {
  def apply(line: String): Trip = {
    val fields: Array[String] = line.split(",", -1)
    Trip(fields(0), fields(1).toInt, fields(2), fields(3), fields(4).toInt, fields(5).toInt)
  }
}

case class EnrichedStopTime(tripId: String,
                            serviceId: String,
                            routeId: Int,
                            tripHeadSign: String,
                            date: Option[String],
                            exceptionType: Option[Int],
                            routeLongName: String,
                            routeColor: String,
                            arrivalTime: String,
                            departureTime: String,
                            stopId: String,
                            stopSequence: Int,
                            wheelchairAccessible: Boolean)

object EnrichedStopTime {
  def toCsv(enrichedStopTime: EnrichedStopTime): String = {
    s"${enrichedStopTime.tripId}," +
      s"${enrichedStopTime.serviceId}," +
      s"${enrichedStopTime.routeId}," +
      s"${enrichedStopTime.tripHeadSign}," +
      s"${enrichedStopTime.date.getOrElse("")}," +
      s"${enrichedStopTime.exceptionType.getOrElse("")}," +
      s"${enrichedStopTime.routeLongName}," +
      s"${enrichedStopTime.routeColor}," +
      s"${enrichedStopTime.arrivalTime}," +
      s"${enrichedStopTime.departureTime}," +
      s"${enrichedStopTime.stopId}," +
      s"${enrichedStopTime.stopSequence}"
  }
}
