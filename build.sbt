import sbt.Keys.libraryDependencies

name := "course5project"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.8"
val kafkaVersion = "2.5.1"
val confluentVersion = "5.5.1"
val jacksonCoreVersion = "2.13.0"

resolvers += "Confluent".at("https://packages.confluent.io/maven/")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"% sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming"% sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion % "provided",
  "io.confluent"     % "kafka-schema-registry-client" % confluentVersion,
  "io.confluent"     % "kafka-avro-serializer"        % confluentVersion
)

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % jacksonCoreVersion
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % jacksonCoreVersion
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % jacksonCoreVersion