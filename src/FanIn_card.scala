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

object FanIn_card {
  class VertexProperty()
  class EdgePropery()

  //顶点: 卡片，属性：id, 帐号，
  case class CardVertex(val priAcctNo: String, val inDgr: Int, val outDgr: Int) extends VertexProperty

  case class TransferProperty(val src_card: String, val dst_card: String, val transAt: Int, val transDt: String) extends EdgePropery
 
  
  // The graph might then have the type:
  var graph: Graph[VertexProperty, EdgePropery] = null

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
    
 //869004002626858 CardVertex(5db0180f8a2bb3a17f0a48276ad1bb0a,0,2)        275790015286939
    val verticeRDD = verticefile.map { line=>
        val lineArray = line.split("\t")
        val vid = lineArray(0).toLong
        val cid = lineArray(2).trim().toLong
        
        val vpropstr = lineArray(1).trim().split("CardVertex\\(")(1)
        val vproplist = vpropstr.substring(0, vpropstr.length()-1).split(",") 
        val cardstr = vproplist(0)
        val indeg = vproplist(1).toInt
        val outdeg = vproplist(2).toInt
        (vid, (cid, cardstr, indeg, outdeg))      //这里设置顶点编号就是前面的卡编号
    }

 //1754383212      102063742906970 TransferProperty(729dbd7c9945313a8b03eccdf774ac6e,f708ab2f06e694b44a43dd95ea57753a,120000,2016-11-03 17:44:01.0)
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
    println("graph.numEdges is " + graph.numEdges)
 
    
    val SumInVRDD = graph.aggregateMessages[Int](
      triplet => {   
          triplet.sendToDst(triplet.attr.transAt);         //我这边要统计每个节点的(卡号，入读，出度，总金额，总次数,总异地转出次数)，所以不管是转出金额还是转入金额我们都累加起来
     },
      (a, b) =>  (a+b)
    )
    
    val SumOutVRDD = graph.aggregateMessages[Int](
      triplet => {   
          triplet.sendToSrc(triplet.attr.transAt);         //我这边要统计每个节点的(卡号，入读，出度，总金额，总次数,总异地转出次数)，所以不管是转出金额还是转入金额我们都累加起来
     },
      (a, b) =>  (a+b)
    )
    
    val break_rate =  SumInVRDD.join(SumOutVRDD).map(vp => (vp._1, vp._2._1.toFloat/vp._2._2.toFloat)).filter(f=> f._2>0.8 && f._2<1.2)
    println("break_rate num is " + break_rate.count)
    //break_rate.collect().foreach(println)
    val breaklist = break_rate.map(vp => vp._1).collect
     
//    val SumInOutGraph = graph.outerJoinVertices(SumInVRDD){
//      (vid, card, q) => (card, q.getOrElse((0.0,0,0,0)))
//     }
    
    val gV2 = graph.subgraph(vpred = (id, prop) => (prop._3>10 && prop._4 <4 && prop._4 >0 && breaklist.contains(id)))
    
    println("gV2.numVertices ", gV2.numVertices)
    
    
     val showbreak_graph = gV2.outerJoinVertices(break_rate){
      (vid, prop, q) => (prop._2, q.getOrElse(0.0))
     }
    
    showbreak_graph.vertices.collect().foreach(println)
    
    
    //val centerV = gV2.vertices.collect()
    val related_cc = gV2.vertices.map(vp => vp._2._1).collect()
    val ccgraph = graph.subgraph(vpred = (id, prop) => (related_cc.contains(prop._1)))
    println("ccgraph.numVertices ", ccgraph.numVertices)
    println("ccgraph.numEdges ", ccgraph.numEdges)
//    ccgraph.edges.map( x => x.attr.src_card + "\t" + x.attr.dst_card + "\t" + x.attr.transAt+","+x.attr.transDt).saveAsTextFile("xrli/POC/ccgraph_20_inout_3")
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////    
     
     
    val centerCardlist = gV2.vertices.map(vp => vp._2._2).collect()
    val direct_edges = graph.edges.filter {ep => centerCardlist.contains(ep.attr.dst_card) || centerCardlist.contains(ep.attr.src_card)}
    
    println("direct_edges num is ", direct_edges.count())
   
    val direct_results = direct_edges.map( x => x.attr.src_card + "\t" + x.attr.dst_card + "\t" + x.attr.transAt + "\t" + x.attr.transDt)
    println("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE: ")
    
    direct_results.collect().foreach { println}
   
    //direct_results.saveAsTextFile("xrli/POC/direct_edges_20_inout_3")
    
    
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
  }
  
  
  
  
  
  
}