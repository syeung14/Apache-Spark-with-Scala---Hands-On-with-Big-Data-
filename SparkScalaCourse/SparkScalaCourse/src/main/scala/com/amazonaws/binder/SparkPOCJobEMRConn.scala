package com.amazonaws.binder
//import com.amazonaws.services.glue.{DynamoDbDataSink, GlueContext}
import org.apache.spark.SparkContext

//trying to use emr-hadoop-dynamodb.jar
object SparkPOCJobEMRConn {
  def main(sysArgs: Array[String]): Unit = {
//    val glueContext = new GlueContext(SparkContext.getOrCreate())
//    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
//    Job.init(args("JOB_NAME"), glueContext, args.asJava)
//
//    val tableName: String = "dsunds-test-graph-lite"
//    val dynamicFrame = glueContext
//      .getSourceWithFormat(
//        connectionType = "dynamodb",
//        options = JsonOptions(
//          Map(
//            "dynamodb.input.tableName" -> tableName,
//            "dynamodb.throughput.read.percent" -> "1.0",
//            "dynamodb.splits" -> "100"
//          )
//        )
//      )
//      .getDynamicFrame()
//
//    print(dynamicFrame.getNumPartitions)
//    val dataFrame = dynamicFrame.toDF();
//    println("count-lite: " + dataFrame.count())
//
//    val simplified = dynamicFrame.toDF()
//    simplified.printSchema()
//    simplified.sqlContext.sql("select ")
//
//    println("completed")
//    Job.commit()
  }
}
