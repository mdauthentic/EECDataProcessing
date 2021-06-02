package fr.mdauthentic.eec

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object SparkJob {

  /**
   * Read data from file
   *
   * @param spark spark session
   * @param path  file path
   * @return dataframe of sku data
   * */
  def readFile(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", value = true)
      .csv(path)
      .persist(StorageLevel.MEMORY_AND_DISK)
  }

  /**
   * Compute the statistics on the EEC dataset
   *
   * @param data  EEC dataframe
   * */
  def dataAggregator(data: DataFrame): DataFrame = {
    // convert time column to timestamp
    val energyDF = data.withColumn("time", col("time").cast("timestamp"))
    // group data on meter and time columns
    energyDF.groupBy(col("meter"), window(col("time"), "1 hour"))
      .agg(mean("power") as "mean_power",
        min("power") as "min_power", max("power") as "max_power",
        count("meter") as "reading_per_hr", first("energy") as "start_val",
        callUDF("percentile_approx", col("power"), lit(0.75)).as("3rd_quartile_power"),
        last("energy") as "end_val")
      .withColumn("energy_consumption", expr("end_val - start_val"))
      .select(col("meter"), col("window.start").alias("start_time"),
        col("window.end").alias("end_time"), col("energy_consumption"),
        col("mean_power"), col("min_power"), col("max_power"),
        col("3rd_quartile_power"), col("reading_per_hr"))
      .orderBy(desc("meter"))
  }

  /**
   * Write dataframe to file
   * @param data  dataframe we want to write to file
   * @param outputPath folder where the result is written to
   * */
  def dataSink(data: DataFrame, outputPath: String = "resources/tmp/"): Unit = {
    data.coalesce(1)
      .write.format("csv")
      .option("header", "false")
      .option("sep",",")
      .csv(outputPath)

    // Output file name is usually non-readable/too long, so we rewrite them with timestamp
    val timestamp = DateTimeFormatter.ofPattern("yyyyMMdd-HHmm").format(LocalDateTime.now)
    val outFilename = "out-" +timestamp
    val fs = FileSystem.get(new java.net.URI(outputPath), new Configuration())
    fs.globStatus(new Path(outputPath + "/*.*"))
      .filter(_.getPath.toString.split("/")
        .last.split("\\.").last == "csv").foreach { e =>
      fs.rename(new Path(e.getPath.toString), new Path(outputPath + outFilename))
    } // end
  }

}
