package Demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD 
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx.lib._
import Algorithm._

object LPA_test {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    //设置运行环境
    val conf = new SparkConf().setAppName("GraphLoader") 
    val sc = new SparkContext(conf)
    
    
//    val rdd = sc.makeRDD(Array("1 2","1 3","2 4","3 4","3 5","4 5","5 6","6 7","6 9","7 11","7 8","9 8","9 13","8 10","10 13","13 12","10 11","11 12"))
//    val edge =rdd .map( line =>{
//      val pair = line.split(" ")
//      Edge( pair(0).toLong,pair(1).toLong,1L )
//      })
//     
//    val graph = Graph.fromEdges( edge,0 )
////     
    
    
    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, "xrli/graphx/testedges2.txt").cache
    
    val cgraph = LPA.run(graph)
    
    println("Show cgraph:")
    cgraph.vertices.collect().foreach(println) 
    
    val mycgraph = myLPA.run(graph)
    
    println("Show mycgraph:")
    mycgraph.vertices.collect().foreach(println) 
    
    sc.stop
  }
}