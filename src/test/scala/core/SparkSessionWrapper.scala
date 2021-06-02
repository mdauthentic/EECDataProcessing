package core

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  lazy val spark: SparkSession = {
    SparkSession.builder()
      .master("local")
      .appName("Spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }
}
