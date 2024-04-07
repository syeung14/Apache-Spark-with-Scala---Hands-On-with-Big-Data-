package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._

object RiskGraphDataset {

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
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val schemaRiskGraph = spark.read
      .option("header", "true")
      .csv("data/ml-100k/riskgraph.csv")
      .as[RiskGraphModel]

    schemaRiskGraph.printSchema()
    println(schemaRiskGraph.count())

    schemaRiskGraph.createOrReplaceTempView("RiskGraphModel")
    val accountIdRes = spark.sql("select * from RiskGraphModel where AwsAccountId = '000000000099'")

    val results = accountIdRes.collect()
    println(results.length)
    println(results(20))

    schemaRiskGraph.select(schemaRiskGraph("Edge"), schemaRiskGraph("Resource")).show(truncate = false)
    schemaRiskGraph.groupBy("AwsAccountId").count().show()

    spark.stop()
  }
}