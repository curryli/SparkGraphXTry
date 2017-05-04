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

object Static_CC_count {
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
    
    println("vertice num: " +  verticefile.count())
 
    val verticeRDD = verticefile.map { line=>
        val lineArray = line.split("\t")
        val vid = lineArray(0).toLong
        val cid = lineArray(2).toLong
        (cid,1)      //这里设置顶点编号就是前面的卡编号
    }
    

    val cc_count_RDD = verticeRDD.reduceByKey(_+_)
    
    val testpoints = List(0,1,2,3,4,5,6,7,8,9,10,20,30,40,50,60,70,80,90,100,200,300,400,500,600,700,800,900,1000,2000,3000,4000,5000)
    
    for(p <- testpoints){
      val vcount_RDD = cc_count_RDD.filter(f => f._2>p)
	   
      val vnum = vcount_RDD.reduce((a,b) => (0L, a._2+ b._2))._2
	    
      println("point " + p + ": cc num " + vcount_RDD.count() + ": vertice num " + vnum)
    }
 
    
   println("All done in " + (System.currentTimeMillis()-startTime) + " ms.")
  }
  
  
  
  
  
  
}