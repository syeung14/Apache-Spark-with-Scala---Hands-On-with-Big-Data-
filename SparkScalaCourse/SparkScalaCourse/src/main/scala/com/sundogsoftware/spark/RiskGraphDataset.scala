package com.sundogsoftware.sparr

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.SparkContext

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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
//    testActions(schemaRiskGraph)

    schemaRiskGraph.createOrReplaceTempView("RiskGraphModel")

//    runBasicSparkSQL(spark.sqlContext.sparkSession, "none")
//    runLoadGroupByDataToMap(schemaRiskGraph, spark.sqlContext)
    loadListAccountIdToMap(spark.sqlContext.sparkSession)

//    runBasicTransformAndAction(schemaRiskGraph)
    spark.stop()
  }

  def loadToMap(awsAccountId: String, sparkSession: SparkSession): Unit = {

    val accountIdRes = sparkSession.sql("select * from RiskGraphModel where AwsAccountId = '" + awsAccountId + "'")
    val res = accountIdRes.collect() // OOM?
    res.foreach(item => {
      val node = createGraphNode(item)
      allDataMap.update(node._2._3, node +: allDataMap(node._2._3)) //OOM?
    })

//    allDataMap.foreach(item => {
//      println(s"Key: ${item._1}, Value: ${item._2}")
//    })
    println("load test map size: " + allDataMap.size + ", " + awsAccountId + ", curr length: " + res.length)

  }

  def loadListAccountIdToMap(sparkSession: SparkSession): Unit = {
    someAwsAccountIdsLite.foreach(acct => {
        loadToMap(acct, sparkSession)
    })
  }

  def runLoadGroupByDataToMap(df: Dataset[RiskGraphModel], sqlContext: SQLContext): Unit = {
    println(s"loading data with group by AwsAccountId")
    val groupRes = df.groupBy("AwsAccountId").count()
    groupRes.show(truncate = false)
    groupRes.foreach(item => {
      val awsAccountId = item.getAs[String]("AwsAccountId")
      val count = item.getAs[Long]("count")
      println(awsAccountId + " : " + count)
//      loadToMap(awsAccountId, sqlContext.sparkSession)  # causing Spark internal NullpointException
    })
    println(s"loading data with group by - finished")
    //    println(s"count " + df.count())
//    println(s"loading data with group by - count finished")
  }

  type BFSData = (String, String, String, String)
  type BFSNode = (String, BFSData)
  val allDataMap : mutable.Map[String, ListBuffer[BFSNode]] = mutable.Map.empty.withDefaultValue(ListBuffer())
  val someAwsAccountIds: List[String] = List( "000000007599", "000000009174", "000000001945", "000000002616", "000000002162"
    , "000000005297", "000000006996", "000000001943", "000000001652", "000000001873", "000000008206")

  val someAwsAccountIdsLite: List[String] = List("000000000099", "000000000950", "000000000976", "000000000803", "000000000417", "000000000842")

  def createGraphNode(r: Row): BFSNode = {
    val edge = r.getAs[String]("Edge")
    val resource = r.getAs[String]("Resource")
    val awsAccountId = r.getAs[String]("AwsAccountId")
    val resourceType = r.getAs[String]("ResourceType")
    (resource + "-" + edge, (resource, edge, awsAccountId, resourceType))
  }

  def runBasicSparkSQL(sparkSession: SparkSession, tableName: String): Unit = {
    println(s"loading data with Spark SQL " + tableName)
    // does this SQL care about the index at DDL table?
    val accountIdRes = sparkSession.sql("select * from RiskGraphModel where AwsAccountId = '000000000099'")

    println(s"loading data with Spark SQL - collect " + tableName)

    val res = accountIdRes.collect() // OOM

    res.foreach(item => {
      val node = createGraphNode(item)
      allDataMap.update(node._2._3, node +: allDataMap(node._2._3))
    })

//    for ((k, v) <- allDataMap) {
//      println(v.size + "::" + s"Key: $k, value: $v")
//    }
  }

  def runBasicTransformAndAction(df: Dataset[RiskGraphModel]): Unit = {
    println(s"loading data with group by")
    df.groupBy("AwsAccountId").count().show(100)
    println(s"loading data with group by - finished")
    println(s"count " + df.count())
    println(s"loading data with group by - count finished")
  }

  def printIt(msg: Row): Unit = {
    println(msg.getAs[String]("Resource"))
  }

  def printCol(schemaRiskGraph: DataFrame): Unit = {
    schemaRiskGraph.select(schemaRiskGraph("Edge"), schemaRiskGraph("Resource")).show(truncate = false)
  }

  def testActions(df: Dataset[RiskGraphModel]): Unit = {
    println("func: " + df.count())
  }
}