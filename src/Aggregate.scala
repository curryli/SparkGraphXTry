import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
 
object Aggregate {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    //设置运行环境
    val conf = new SparkConf().setAppName("SimpleGraphX") 
    val sc = new SparkContext(conf)
 
    //设置顶点和边，注意顶点和边都是用元组定义的Array
    //顶点的数据类型是VD:(String,Int)
    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42))
    )
    //边的数据类型ED:Int
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(4L, 1L, 1)
    )
 
    //构造vertexRDD和edgeRDD
    val vRDD = sc.parallelize(vertexArray)
    val eRDD = sc.parallelize(edgeArray)
 
    //构造图Graph[VD,ED]
    val graph = Graph(vRDD, eRDD)
 

    
   
      //***********************************************************************************
    //***************************  聚合操作    ****************************************
    //**********************************************************************************  

    val oldestFollower: VertexRDD[(Long,(String,Int))] = graph.aggregateMessages[(Long,(String,Int))](
      triplet => {   
          triplet.sendToDst((triplet.dstId,triplet.srcAttr))
     },
      (a, b) => if (a._2._2 > b._2._2) a else b
    )
  
     println("Each oldestFollower's is ")
     oldestFollower.collect.foreach(v=>println(v._1 +"," + v._2._2._1+ "," +  v._2._2._2 ))
 
 
    sc.stop()
  }
}