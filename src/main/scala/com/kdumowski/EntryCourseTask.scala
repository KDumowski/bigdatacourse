package com.kdumowski

import org.apache.spark.sql.SparkSession

object EntryCourseTask {

  def getOrCreateSparkSession(appName: String) = SparkSession
    .builder()
    .appName(appName)
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSession("BigDataCourse")

    val productSchema = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/products.csv")

    productSchema.createOrReplaceTempView("products")

    val results = sparkSession.sql("SELECT origin_country, AVG(price), (SUM(rating_five_count) / SUM(rating_count)) * 100 as five_percentage from products " +
      "WHERE origin_country IS NOT NULL " +
      "GROUP BY origin_country " +
      "ORDER BY origin_country")
      .collect()
      // Not worth investigating why spark parses this as origin_country in entry task so just filter out "hwalle" and null
      // The CSV file has some errors like lacking data, wrong formats in wrong columns, so this is the most probable reason
      .filter(product => product.get(0).toString != "hwalee")
      .foreach(println)
  }
}
