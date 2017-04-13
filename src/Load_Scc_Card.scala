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
import scala.collection.mutable.{Buffer,Set,Map}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import Algorithm._

object Load_Scc_Card {
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
    
    val verticefile = sc.textFile("xrli/POC/scc_10_vertices")//.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val edgefile = sc.textFile("xrli/POC/scc_10_edges")//.persist(StorageLevel.MEMORY_AND_DISK_SER)
    
 
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
    println("graph.numEdges is " + graph.numEdges)

//678856664375130 CardVertex(ef10ce9243bbbc7ca1aa73a3c831ea1a,3,0)
//50323566228396  CardVertex(2d7e9ea68b99f5f700e90828726886af,1,0)
//6788115067      776216246887956 TransferProperty(3ce2d9f44086c6a1bb24c7c0d8225d06,1fc62cc1d8b0c82029f16d7d8b023932,540000,2016-11-03 12:42:09.0)
//10637919657     108970919512408 TransferProperty(dc7e3fe5dacc0eca799a5d8bbd80ab76,aec9402f561547eeeb660bcba6e96973,1000000,2016-11-01 10:40:58.0)

   val tmpgraph = graph.subgraph(epred = triplet => (triplet.attr.src_card.equals("cc8d24a0d7b97c54b189abb97a1996f6") || triplet.attr.dst_card.equals("cc8d24a0d7b97c54b189abb97a1996f6")))
   println("tmpgraph.numEdges is " + tmpgraph.numEdges)
   
   tmpgraph.edges.collect().foreach { println}
   println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
  }
  
  
  
  
  
  
}