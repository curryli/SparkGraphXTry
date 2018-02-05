package test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.graphx.Graph.graphToGraphOps
 
object LoadGraph {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    //设置运行环境
    val conf = new SparkConf().setAppName("GraphLoader") 
    val sc = new SparkContext(conf)
 
    val graph = GraphLoader.edgeListFile(sc, "xrli/graphx/web-Google.txt")
 
    println("vertices count is : " + graph.vertices.count)
    println("edges count is : " + graph.edges.count)
    
    // Run PageRank
    val ranks = graph.pageRank(0.0001).vertices
    // Join the ranks with the usernames
    ranks.sortBy(_._2, false).saveAsTextFile("xrli/graphx/graph.pr")  
    ranks.top(5)(Ordering.by(_._2)).foreach(println) 
    println("PageRanK finished successfully.")
    
    
    //Connected Components  连通体算法用id标注图中每个连通体，将连通体中序号最小的顶点的id作为连通体的id。  使用最小编号的顶点标记图的连通体
    val connectedComponentsGraph = graph.connectedComponents().vertices  
    connectedComponentsGraph.sortBy(_._2, false).saveAsTextFile("xrli/graphx/graph.cc")  
    connectedComponentsGraph.top(5)(Ordering.by(_._2)).foreach(println)
    println("connectedComponents successfully.")
    
    //TriangleCount主要用途之一是用于社区发现 保持sourceId小于destId  
    val triangleCountGraph = graph.triangleCount()  
    triangleCountGraph.vertices.sortBy(_._2, false).saveAsTextFile("xrli/graphx/graph.tc")  
    triangleCountGraph.vertices.top(5)(Ordering.by(_._2)).foreach(println)  
    println("triangleCountGraph successfully.")
    
    
    sc.stop()
  }
}