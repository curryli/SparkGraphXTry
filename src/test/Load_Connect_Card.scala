package test

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import Algorithm._
import org.apache.spark.graphx._
import Algorithm._

object Load_Connect_Card {
  class VertexProperty()
  class EdgePropery()

  //顶点: 卡片，属性：id, 帐号，
  case class CardVertex(val priAcctNo: String, val inDgr: Int, val outDgr: Int) extends VertexProperty

  case class TransferProperty(val src_card: String, val dst_card: String, val transAt: Int, val transDt: String) extends EdgePropery
 
 

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
    
    val verticefile = sc.textFile("xrli/POC/cc_10_vertices")//.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val edgefile = sc.textFile("xrli/POC/cc_10_edges")//.persist(StorageLevel.MEMORY_AND_DISK_SER)
    
 
    val verticeRDD = verticefile.map { line=>
        val lineArray = line.split("\t")
        val vid = lineArray(0).toLong
        val vpropstr = lineArray(1).trim().split("CardVertex\\(")(1)
        val cardstr = vpropstr.substring(0, vpropstr.length()-1).split(",")(0)
        (vid,cardstr)      //这里设置顶点编号就是前面的卡编号
    }
    


    val edgeRDD = edgefile.map { line=>
        val lineArray = line.split("\t")

        val srcId = lineArray(0).toLong
        val dstId = lineArray(1).toLong
        
        val edgepropstr = lineArray(2).trim().split("TransferProperty\\(")(1)
        val proplist = edgepropstr.substring(0, edgepropstr.length()-1).split(",")
 
       // 读入时指定编码    slice  前包后不包     substring  start到end 
       val item = new TransferProperty(proplist(0), proplist(1) , proplist(2).toInt, proplist(3)) //金额、时间
       Edge(srcId, dstId, item) // srcId,destId
    }
 
  
    // 定义一个默认用户，避免有不存在用户的关系  
    val graph = Graph(verticeRDD, edgeRDD) 
    println("Load done in " + (System.currentTimeMillis()-startTime) + " ms.")
    //println("graph.numEdges is " + graph.numEdges)

    val POCFaninFile = sc.textFile("xrli/POC/FI0321.csv")//.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val rdd1 = POCFaninFile.map(line => line.split(",")(0).trim)                //.map(item => item(0))         
    val rdd2 = POCFaninFile.map(line => line.split(",")(1).trim)
    val POCCardRDD = rdd1.union(rdd2).distinct()
     
    val loadedvrdd = graph.vertices.map{f => f._2}
    val interRdd = loadedvrdd.intersection(POCCardRDD)
    println("POCCardRDD num is :" + POCCardRDD.count())
    println("interRdd num is :" + interRdd.count())
    
    POCCardRDD.subtract(loadedvrdd).collect().foreach(println)
    
   println("All done in " + (System.currentTimeMillis()-startTime) + " ms.")
  }
  
  
  
  
  
  
}