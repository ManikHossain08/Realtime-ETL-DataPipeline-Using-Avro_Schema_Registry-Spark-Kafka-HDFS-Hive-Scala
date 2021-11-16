# ETL-DataPipeLine-Using-Avro_Schema_Registry-Spark-Kafka-HDFS-Hive-Scala

# Summary
In this sprint, you need to use your expertise in Spark and Kafka to process “Trip” data with “Station
Information”.
Skillset
- Scala
- Hive
- HDFS
- Spark (Core, SQL, Streaming)
- Kafka
- Confluent Schema Registry
- Parquet
- Avro
# Input
## Trips
The main data for this pipeline is historical data of trips. We will create a stream and process this stream
in our pipeline. To prepare data, go to the BiXi website (link provided in the documentation section) and under “Trip
History” section, you will find links to the data from previous year all the way back to 2014. Download
the data of the previous year. There is one file per month. Take the last month. The files are quite big, so
we just take a sample. Extract the first 100 lines of the data after skipping the first line which is the CSV
header. In Linux or Mac, you can run:
head -n 101 <filename> | tail -n 100 > 100_trips.csv
Prepare a command line to produce the content of 100_trips.csv file to Kafka. You can use Kafka console
producer command or implement a tool of your choice. It is better to be able to produce data in small
batches e.g. 10 lines. It gives a better chance to monitor your pipeline.
  
# Enriched Station Information
This is the artifact of your sprint 2. Note the location on HDFS.
Prerequisites
In order to start the project, you need to prepare the environment which is
• Create required Kafka topics
• Prepare the input
• Register Avro schema

<img width="1069" alt="image" src="https://user-images.githubusercontent.com/45977153/141873581-e4becb03-0979-4bbc-a7c0-d2dd7368f5c8.png">

<img width="1069" alt="image" src="https://user-images.githubusercontent.com/45977153/141873640-c51c49d4-3998-4b07-950a-fc715673b15b.png">

<img width="1069" alt="image" src="https://user-images.githubusercontent.com/45977153/141873691-4588542f-06cb-4f4f-8197-72703662e053.png">

<img width="1069" alt="image" src="https://user-images.githubusercontent.com/45977153/141873744-fe51099f-bc8c-4c03-ad0e-cd206e3f83c3.png">

<img width="1069" alt="image" src="https://user-images.githubusercontent.com/45977153/141873785-8d4c8534-6bf5-42d4-92d3-16dd17cdde29.png">



 

