name := "course5project"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.4"
val kafkaVersion = "2.3.1"

/*
 NOTE: Hadoop-hdfs & kafka-clients will be already there if I install the spark, so don't need to add these
 library additionally
 */
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"% sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming"% sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion % "provided"
)

/*
NOTE: FOR CREATING THE EXTERNAL TABLE ext_enriched_stop_time WITH PARTITION
I USE THIS QUERY AND "MSCK REPAIR TABLE ext_enriched_stop_time" COMMAND TO
UPDATE THE METADATA AFTER SAVING THE DATA FROM THE SCALA APPLICATION to HDFS data warehouse.

QUERY:
    CREATE external TABLE ext_enriched_stop_time (
        tripId int,
        serviceId STRING,
        routeId int,
        tripHeadSign STRING,
        date string,
        exceptionType int,
        longRouteName string,
        routeColor string,
        arrivalTime string,
        departureTime string,
        stopId string,
        stopSequence int
    )
    PARTITIONED BY (wheelchairAccessible boolean)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    location '/user/bdsf2001/manik/project5/enriched_stop_time'

 */