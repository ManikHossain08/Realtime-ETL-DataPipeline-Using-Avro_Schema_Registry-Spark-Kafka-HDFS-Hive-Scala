package com.etl.spark.streaming.scala.configurations

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext

trait SparkAppConfig {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark Streaming With Avro Schema Registry")
    .master("local[*]")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  val ssc: StreamingContext = new StreamingContext(sc, Seconds(10))
}
