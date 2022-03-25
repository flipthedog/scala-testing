package com.floris

import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]) {
    // Create spark session
    val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local").getOrCreate()
    println("Successfully started SparkSession")

    val jsonFile = "/home/floris/IdeaProjects/scala-test-again/playground-project/data/all_scripts_raw.json" // Should be some file on your system
    val csvFile = "/home/floris/IdeaProjects/scala-test-again/playground-project/data/artists.csv" // Should be some file on your system

    //val logData = spark.read.textFile(logFile).cache()
    val df = spark.read.csv(csvFile)

    df.show()

    df.write.parquet("/home/floris/IdeaProjects/scala-test-again/playground-project/data/output.parquet")

    println(s"Finished.")
    spark.stop()
  }

}
