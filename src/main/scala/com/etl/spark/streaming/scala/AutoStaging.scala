package com.etl.spark.streaming.scala

import com.etl.spark.streaming.scala.configurations.HadoopClientConfig
import org.apache.hadoop.fs.Path
import scala.collection.immutable.HashMap

object AutoStaging extends HadoopClientConfig {
  private val localSysDir = "./data"
  private val batchFiles = HashMap("station_information" -> "enriched_station_information.csv")

  def putFilesToHDFS(): Unit = {
    deleteStagingDir(stagingDir)
    batchFiles.foreach {
      case (folderName, fileName) =>
        fileSystem.mkdirs(new Path(s"$stagingDir/$folderName"))
        if (fileName.nonEmpty)
          fileSystem.copyFromLocalFile(
            new Path(s"$localSysDir/$fileName"), new Path(s"$stagingDir/$folderName")
          )
    }
  }

  def deleteStagingDir(folderPath: String): Unit = {
    val pathHDFS = new Path(folderPath)
    if (fileSystem.exists(pathHDFS)) {
      fileSystem.delete(pathHDFS, true)
    }
  }
}
