package com.etl.spark.streaming.scala

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object StationInformation {
  val stationInfoSchema: StructType = StructType(
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
                end_station_code: Int,
                duration_sec: Int,
                is_member: Int
               )

object Trip {
  def apply(line: String): Trip = {
    val fields: Array[String] = line.split(",", -1)
    Trip(fields(0), fields(1).toInt, fields(2), fields(3).toInt, fields(4).toInt, fields(5).toInt)
  }
}

case class EnrichedTrip(start_date: String,
                        start_station_code: Int,
                        end_date: String,
                        end_station_code: Int,
                        duration_sec: Int,
                        is_member: Int,
                        system_id: String,
                        timezone: String,
                        station_id: Int,
                        name: String,
                        short_name: String,
                        lat: Double,
                        lon: Double,
                        capacity: Int
                       )


object EnrichedTrip {

  def toCsv(enrichedTrip: EnrichedTrip): String = {
    s"${enrichedTrip.start_date}," +
      s"${enrichedTrip.start_station_code}," +
      s"${enrichedTrip.end_date}," +
      s"${enrichedTrip.end_station_code}," +
      s"${enrichedTrip.duration_sec}," +
      s"${enrichedTrip.is_member}," +
      s"${enrichedTrip.system_id}," +
      s"${enrichedTrip.timezone}," +
      s"${enrichedTrip.station_id}," +
      s"${enrichedTrip.name}," +
      s"${enrichedTrip.short_name}," +
      s"${enrichedTrip.lat}," +
      s"${enrichedTrip.lon}," +
      s"${enrichedTrip.capacity}"
  }
}
