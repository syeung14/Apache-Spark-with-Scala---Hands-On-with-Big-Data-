package com.sundogsoftware.spark.learn

import org.apache.spark.sql._
import org.apache.log4j._

object Product {

  ///defined the dataset
  case class RiskGraphModel(Edge: String,
                            AwsAccountId: String,
                            ResourceType: String,
                            LastUpdated: String,
                            Resource: String)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("SparkCoding")
      .master("local[*]")
      .getOrCreate()

    val inputStrings = List("Lily 23", "Jack 29", "Mary 29", "James 8")

    val regularRDDs = spark.sparkContext.parallelize(inputStrings)


    var myRange = spark.range(1000).toDF("number")
    myRange.show(false)

    spark.stop()
  }
}