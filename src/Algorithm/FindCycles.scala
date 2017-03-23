package Algorithm

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import scala.collection.mutable.MutableList
import scala.Range
 
object FindCycles {
  private var trace = MutableList[Long]()
  
  def findCycle(sc: SparkContext, v: Long, vertexArray: Array[(Long, Long)], edgeArray: Array[Edge[Int]], visited: MutableList[Int]) 
    {
        if(visited(v.toInt)==1)
        {
            
            if(trace.contains(v))
            {
                //println("In find " + v.toInt)
                var j = trace.indexOf(v)
                println("Cycle:");
                while(j<trace.length)
                {
                    print(trace(j)+" ");
                    j = j+1;
                }
                println("\n");
                return;
            }
            return;
        }
        
        //println("current v: " + v.toInt)
        visited(v.toInt)=1;
        trace.+=(v);
        
        for(i <- 0 to vertexArray.length) 
        {
          //println("i is : " + i)
          val pairs = edgeArray.map(e=>(e.srcId,e.dstId))
          if(pairs.contains((v.toLong,i.toLong)))
                findCycle(sc, i.toLong, vertexArray, edgeArray, visited);
        }
        trace = trace.dropRight(1);
    }
    
  def main(args: Array[String]) {
    //屏蔽日志
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.INFO)
    Logger.getLogger("org").setLevel(Level.INFO);
    Logger.getLogger("akka").setLevel(Level.INFO);
    Logger.getLogger("hive").setLevel(Level.INFO);
    Logger.getLogger("parse").setLevel(Level.INFO);
    
    //设置运行环境
    val conf = new SparkConf().setAppName("FindCycles") 
    val sc = new SparkContext(conf)
 
    //设置顶点和边，注意顶点和边都是用元组定义的Array
    //顶点的数据类型是VD:(String,Int)
    val vertexArray = Array(
      (0L, 0L),
      (1L, 1L),
      (2L, 2L),
      (3L, 3L),
      (4L, 4L),
      (5L, 5L),
      (6L, 6L)
    )
    //边的数据类型ED:Int
    val edgeArray = Array(
      Edge(0L, 1L, 1),
      Edge(0L, 2L, 1),
      Edge(1L, 3L, 1),
      Edge(2L, 5L, 1),
      Edge(3L, 4L, 1),
      Edge(4L, 2L, 1),
      Edge(5L, 4L, 1),
      Edge(5L, 6L, 1),
	    Edge(6L, 0L, 1),
	    Edge(6L, 2L, 1)
    )
 
    //构造vertexRDD和edgeRDD
    val vertexRDD = sc.parallelize(vertexArray)
    val edgeRDD = sc.parallelize(edgeArray)
  
    var visited = new MutableList[Int]()
    for(i<-Range(0,vertexArray.length))
      visited.+=(0)
    
    println("Starting find...")
    findCycle(sc, 1, vertexArray, edgeArray, visited) 
  
    sc.stop()
  }
}


//Cycle:
//4 2 5 
//
//Cycle:
//1 3 4 2 5 6 0 
//
//Cycle:
//2 5 6 0 
//
//Cycle:
//2 5 6