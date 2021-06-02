package core

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.{RelationalGroupedDataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.scalatest.FunSpec

class SparkJobTest extends FunSpec with SparkSessionWrapper with DatasetComparer {

  import spark.implicits._

  val data: RelationalGroupedDataset = Seq(("A", "2017-01-01 00:15:01+00:00", 1.0),
    ("A", "2017-01-01 00:15:02+00:00", 2.0),
    ("A", "2017-02-02 00:15:02+00:00", 4.0),
    ("A", "2017-02-02 00:15:30+00:00", 3.0),
    ("C", "2017-01-01 10:15:00+00:00", 4.0),
    ("B", "2017-01-08 23:30:00+00:00", 1.43))
    .toDF("meter", "time", "power")
    .groupBy(col("meter"), window(col("time"), "1 hour"))

  describe("#count_power") {
    it("returns a count of grouped data in the power column") {
      val minDF = data.agg(min($"power") as "min_power").select($"meter", $"min_power")
      val schema = List(
        StructField("meter", StringType, nullable = true),
        StructField("min_power", DoubleType, nullable = true)
      )
      val dataRow = Seq(Row("A", 1.0), Row("A", 3.0), Row("C", 4.0), Row("B", 1.43))
      val expectedDF = spark.createDataFrame(spark.sparkContext.parallelize(dataRow), StructType(schema))
      assertSmallDatasetEquality(minDF, expectedDF, orderedComparison = false)
    }
  }

  describe("#min_power") {
    it("should return the mean of grouped data in the power column") {
      val minDF = data.agg(min($"power") as "min_power").select($"meter", $"min_power")
      val schema = List(
        StructField("meter", StringType, nullable = true),
        StructField("min_power", DoubleType, nullable = true)
      )
      val dataRow = Seq(Row("A", 1.0), Row("A", 3.0), Row("C", 4.0), Row("B", 1.43))
      val expectedDF = spark.createDataFrame(spark.sparkContext.parallelize(dataRow), StructType(schema))
      assertSmallDatasetEquality(minDF, expectedDF, orderedComparison = false)
    }
  }

  describe("#mean_power") {
    it("should return the mean of grouped data in the power column") {
      val meanDF = data.agg(mean($"power") as "mean_power").select($"meter", $"mean_power")
      val schema = List(
        StructField("meter", StringType, nullable = true),
        StructField("mean_power", DoubleType, nullable = true)
      )
      val dataRow = Seq(Row("A", 1.5), Row("A", 3.5), Row("C", 4.0), Row("B", 1.43))
      val expectedDF = spark.createDataFrame(spark.sparkContext.parallelize(dataRow), StructType(schema))
      assertSmallDatasetEquality(meanDF, expectedDF, orderedComparison = false)
    }
  }

  describe("#max_power") {
    it("should return the max of grouped data in the power column") {
      val meanDF = data.agg(mean($"power") as "max_power").select($"meter", $"max_power")
      val schema = List(
        StructField("meter", StringType, nullable = true),
        StructField("max_power", DoubleType, nullable = true)
      )
      val dataRow = Seq(Row("A", 1.5), Row("A", 3.5), Row("C", 4.0), Row("B", 1.43))
      val expectedDF = spark.createDataFrame(spark.sparkContext.parallelize(dataRow), StructType(schema))
      assertSmallDatasetEquality(meanDF, expectedDF, orderedComparison = false)
    }
  }

}
