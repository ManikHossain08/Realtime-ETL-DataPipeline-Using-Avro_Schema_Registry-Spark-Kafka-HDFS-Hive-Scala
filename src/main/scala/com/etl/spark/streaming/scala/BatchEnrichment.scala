package com.etl.spark.streaming.scala

import com.etl.spark.streaming.scala.configurations.{HadoopClientConfig, SparkAppConfig}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object BatchEnrichment extends SparkAppConfig with HadoopClientConfig {

  import spark.implicits._

  /**
   * IMPORTANT NOTE:
   * Follow similar kind of approach of riding the data from HDFS in real project and in the
   * professional level it is good for learning level project.
   * But I should not follow different kind of approach like this, advised to follow a single one.
   */
  val rawTrips: RDD[String] = spark.sparkContext.textFile(s"$stagingDir/trips/trips.txt")
  val tripsRdd: RDD[Trip] = rawTrips.filter(!_.contains("trip_id")).map(Trip(_))
  val tripsDF: DataFrame = tripsRdd.toDF()
  tripsDF.createOrReplaceTempView("tblTrip")

  val routesDF: DataFrame = spark.read
    .option("header", "true")
    .schema(Route.routeSchema)
    .csv(s"$stagingDir/routes/routes.txt")
    .select("route_id", "route_long_name", "route_color")
  routesDF.createOrReplaceTempView("tblRoute")

  val calendarDatesDF: DataFrame = spark.read
    .option("header", "true")
    .schema(CalendarDate.calDateSchema)
    .csv(s"$stagingDir/calendar_dates/calendar_dates.txt")
  calendarDatesDF.createOrReplaceTempView("tblCalDate")

  val enrichedTripsDF: DataFrame = spark.sql(
    s"""SELECT
       |    trips.tripId,
       |    trips.serviceId,
       |    trips.routeId,
       |    trips.tripHeadSign,
       |    trips.wheelchairAccessible,
       |    calenders.date,
       |    calenders.exception_type exceptionType,
       |    routes.route_long_name routeLongName,
       |    routes.route_color routeColor
       | FROM tblTrip trips
       | LEFT JOIN tblRoute routes
       | ON trips.routeId = routes.route_id
       | LEFT JOIN tblCalDate calenders
       | ON trips.serviceId = calenders.service_id""".stripMargin
  )
}
