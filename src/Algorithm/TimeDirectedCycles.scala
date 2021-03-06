package Algorithm

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import scala.collection.mutable.MutableList
import scala.Range
 
object TimeDirectedCycles {
  private var trace = MutableList[Long]()
  private var curtime = 0
  private var lasttime = 0
  def findCycle(sc: SparkContext, v: Long, gV: VertexRDD[Long], gE: EdgeRDD[Int], visited: MutableList[Int]) 
    {
        
        if(visited(v.toInt)==1 && curtime>lasttime)
        {
            //println("curtime is " + curtime + ". lasttime is " + lasttime)
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
        lasttime = curtime
        trace.+=(v);
        
        for(i <- 0L to gV.count()) 
        {
          //println("i is : " + i)
          val pairs = gE.map(e=>(e.srcId,e.dstId,e.attr))
          val pairrdd = pairs.filter(f=>f._1==v & f._2==i)
            if(pairrdd.count!=0){
                curtime = pairrdd.first()._3
                //println("curtime is " + curtime + ". lasttime is " + lasttime)
                if(curtime>lasttime){
                  findCycle(sc, i.toLong, gV, gE, visited);
                }
                  
            }
        }
        //println(trace)
        
        
        trace = trace.dropRight(1);
    }
    
  def main(args: Array[String]) {
    //屏蔽日志
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    Logger.getLogger("hive").setLevel(Level.OFF);
    Logger.getLogger("parse").setLevel(Level.OFF);
    
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
      Edge(0L, 1L, 6),    //这里属性是时间，数值越大，表示时间越靠后
      Edge(1L, 2L, 1),
      Edge(2L, 0L, 5),
      Edge(0L, 3L, 4),
      Edge(3L, 2L, 3)
    )
 
    //构造vertexRDD和edgeRDD
    val vertexRDD = sc.parallelize(vertexArray)
    val edgeRDD = sc.parallelize(edgeArray)
 
    //构造图Graph[VD,ED]
    val graph = Graph(vertexRDD, edgeRDD)
    
    val gV = graph.vertices
    val gE = graph.edges
    
    
    var visited = new MutableList[Int]()
    for(i<-Range(0,vertexArray.length))    //初始化
      visited.+=(0)

    println("Starting find...")
    findCycle(sc, 1, gV, gE, visited) 
  
    sc.stop()
  }
}


//120
 