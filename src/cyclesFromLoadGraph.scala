import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import Algorithm._
import scala.collection.mutable.MutableList
import scala.Range
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object cyclesFromLoadGraph {
  private var trace = MutableList[Long]()
  var visited:scala.collection.mutable.Map[Long,Boolean] = scala.collection.mutable.Map[Long,Boolean]()
 

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.ERROR);
    Logger.getLogger("parse").setLevel(Level.ERROR);

    //    require(args.length == 3)

    val conf = new SparkConf().setAppName("Graphx Test")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)
    val startTime = System.currentTimeMillis(); 
    
    //val testGraph = GraphLoader.edgeListFile(sc, "xrli/mytestedges.txt")
    val testGraph = GraphLoader.edgeListFile(sc, "xrli/testedges2.txt")
    
    val gV = testGraph.vertices
    val gE = testGraph.edges
    //gE.collect().foreach {println}
   
    val gVList = gV.collect          //要先转化成数组，不要在RDD的foreach里面复制，否则不起任何作用
    gVList.foreach{vp=>    //初始化
      visited += (vp._1.toLong -> false)
    }
//    println("Initialized visited: ")
//    visited.foreach(println) 
    
    trace.clear()
    
    val startPoint = gV.first()._1.toLong
    //val startPoint = 128075651301304L
    
    println("Starting find from " + startPoint + ": ")
    findCycle(sc, startPoint, gV, gE)
    
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
  }
  
  
  def findCycle(sc: SparkContext, Vid: Long, gV: VertexRDD[Int], gE: EdgeRDD[Int]) 
    {
        //println("current trace: " + trace)
        //println("current Vid: " + Vid)
        if(visited(Vid)==true)
        {
            if(trace.contains(Vid))
            {
                //println("In find " + v.toInt)
                var j = trace.indexOf(Vid)
                println("current j: " + j)
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

        visited(Vid)=true;
        trace.+=(Vid);
        
        
        val Vlist = gV.collect()
        
        Vlist.foreach{rdd=> 
          var new_vid = rdd._1.toLong
          //println("i is : " + i)
          val pairs = gE.map(e=>(e.srcId,e.dstId))
          if(pairs.filter(f=>f._1==Vid & f._2==new_vid).count!=0)
                findCycle(sc,new_vid.toLong, gV, gE);
        }
  
        //println(trace)
        trace = trace.dropRight(1);
    }
  

}