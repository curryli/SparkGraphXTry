package Demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD 
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx.lib._
import Algorithm._

object Pagerank_test {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    //设置运行环境
    val conf = new SparkConf().setAppName("GraphLoader") 
    val sc = new SparkContext(conf)
    
     
    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, "xrli/graphx/testedges2.txt").cache
    
    
    
    
     
    val Convgraph = simple_Pagerank.runUntilConvergence(graph, tol=0.001, resetProb= 0.15)
    
    println("Show Convgraph:")
    Convgraph.vertices.collect().foreach(println) 
    
    
    
    val staticgraph = simple_Pagerank.run(graph, numIter=100, resetProb= 0.15)  //可以看出对于该数据，用静态算法很难得到准确的结果
    
    println("Show staticgraph:")
    staticgraph.vertices.collect().foreach(println) 
    
    
    sc.stop
  }
}