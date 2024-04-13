package com.sundogsoftware.spark.learn

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.graphx._

/** Some examples of GraphX in action with the Marvel superhero dataset! */
object GraphTest {
  
  // Function to extract hero ID -> hero name tuples (or None in case of failure)
  def parseNames(line: String) : Option[(VertexId, String)] = {
    val fields = line.split('\"')
    if (fields.length > 1) {
      val heroID:Long = fields(0).trim().toLong
      if (heroID < 6487) {  // ID's above 6486 aren't real characters
        return Some( fields(0).trim().toLong, fields(1))
      }
    } 
  
    None // flatmap will just discard None results, and extract data from Some results.
  }
  
  /** Transform an input line from marvel-graph.txt into a List of Edges */
  def makeEdges(line: String) : List[Edge[Int]] = {
    import scala.collection.mutable.ListBuffer
    var edges = new ListBuffer[Edge[Int]]()
    val fields = line.split(" ")
    val origin = fields(0)
    for (x <- 1 until (fields.length - 1)) {
      // Our attribute field is unused, but in other graphs could
      // be used to deep track of physical distances etc.
      edges += Edge(origin.toLong, fields(x).toLong, 0)
    }
    
    edges.toList
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "GraphX")

    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"),
      (3L, "Charles"), (4L, "Diane"), (5L, "Went to gym this morning")))

    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"),
      Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"),
      Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))

    val myGraph = Graph(myVertices, myEdges)

    val data = myGraph.mapTriplets(t => (t.attr, t.attr=="is-friends-with" &&
      t.srcAttr.toLowerCase.contains("a"))).triplets.collect

    val res = myGraph.aggregateMessages[Int](_.sendToSrc(1), _ + _).rightOuterJoin(myGraph.vertices).map(_._2.swap).collect
    res.foreach( (i) => {
      println(i)
    })

  }
}