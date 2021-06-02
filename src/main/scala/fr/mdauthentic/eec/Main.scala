package fr.mdauthentic.eec

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Main extends App {

  // start Spark session
  val spark = SparkSession
    .builder()
    .appName("Electrical Energy Consumption")
    .config("spark.master", "local[*]")
    .getOrCreate()
  // Set spark events reporting at an acceptable level
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  // check file path isn't empty
  if (args.isEmpty) {
    println("File path argument missing.")
    sys.exit()
  }

  final val path = args(0)
  try {
    // read dataset
    val data = SparkJob.readFile(spark, path)
    // remove missing data
    val cleanData = data.na.drop()
    // do aggregation and process data
    val aggDF = SparkJob.dataAggregator(cleanData)
    // write to file
    SparkJob.dataSink(aggDF)
    println("Process complete.")
    // destruct session and exit
    spark.stop()
    sys.exit()
  } catch {
    case e: RuntimeException =>
      println(e.getMessage)
      spark.stop()
  }

}
