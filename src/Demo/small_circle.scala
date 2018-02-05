package Demo
//http://blog.csdn.net/u013045749/article/details/50496924   两种写法都可以
//目的：  在设置传播方向沿着src到dst时，找到三步之内能形成环路的顶点

//思路：每一个顶点构建一个候选集合，存储能到达该顶点的顶点id,初始为空，迭代三次表示三步之内能到达该顶点的顶点，如果那些顶点内部包含它自己，那么这就可以构成一个环路。

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD 
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx.lib._

object small_circle {
  def main(args : Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    //设置运行环境
    val conf = new SparkConf().setAppName("GraphLoader") 
    val sc = new SparkContext(conf)
    
    val graph:Graph[Int,Int] = GraphLoader.edgeListFile(sc, "xrli/graphx/testedges2.txt").cache
   
    graph.vertices.collect.foreach(println(_))
 
    //初始化一个新图，每一个顶点的属性集合都为空 
    val initialGraph = graph.mapVertices((id, attr) => Set[VertexId]())
    val circleGraph = initialGraph.pregel(    //pregel 注意大小写
        // 最初值为空
        Set[VertexId](),
        //最大迭代次数为3
        3,
        // 发送消息（默认出边）的方向
        EdgeDirection.Out) (
            // 用户定义的接收消息,每一个顶点把收到的信息合并  attr为原节点属性 ，msg为消息类 
            (id, attr, msg) => (msg ++ attr),
            // 计算消息 方式为   (目标节点id，消息 )  也就是把消息发送给目标节点
            edge => Iterator((edge.dstId, (edge.srcAttr + edge.srcId))),
            // 合并消息
            (a, b) => (a ++ b)
        // 取出包含自身id的点,如果三轮迭代之后属性集合中包含自己，说明经过三步可以回到自身，是一个三角形。
        ).subgraph(vpred = (id, attr) => attr.contains(id))
 
    println("circleGraph")
    circleGraph.vertices.collect.foreach(println(_))
    sc.stop
  }
  
}



//(4,1)
//(0,1)
//(6,1)
//(2,1)
//(1,1)
//(3,1)
//(5,1)
//circleGraph
//(4,Set(0, 5, 1, 6, 2, 3, 4))
//(6,Set(0, 5, 6, 2, 4))
//(2,Set(0, 5, 1, 6, 2, 3, 4))
//(5,Set(0, 5, 6, 2, 3, 4))