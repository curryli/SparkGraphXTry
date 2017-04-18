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
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object CC_filter {
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
 
    val smallccRdd = graph.vertices.map(vp=>(vp._2._1, 1)).reduceByKey(_+_).filter(f=>f._2<2000).map(f=>f._1)
     
    val gV2 = graph.subgraph(vpred = (id, prop) => (prop._3>10 && prop._3 <200 && prop._4>10 && prop._4 <200))
    println("gV2.numVertices ", gV2.numVertices)
    
    var complexCC = gV2.vertices.map(vp=>(vp._2._1, 1)).reduceByKey(_+_).filter(f=>f._2>3 && f._2<50)
    println("complexCC count is ")
    complexCC.collect.foreach(println)
    
    var complexCCRdd =  complexCC.map(f=>f._1) 
    println("complexCC num is  ", complexCC.count())
    
    complexCCRdd = complexCCRdd.intersection(smallccRdd) 
    println("small complexCC num is  ", complexCCRdd.count())
    
    val complexCClist = complexCCRdd.collect
    
    val ccgraph = graph.subgraph(vpred = (id, prop) => (complexCClist.contains(prop._1)))
    println("ccgraph.numVertices ", ccgraph.numVertices)
    println("ccgraph.numEdges ", ccgraph.numEdges)
    
    val cc_vertices = ccgraph.vertices.map(f=>f._2._2 + "\t" + f._2._1)
    cc_vertices.collect().foreach {println}
    
    println("cc_edges EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE: ")
    
    val cc_edges = ccgraph.edges.map( x => x.attr.src_card + "\t" + x.attr.dst_card + "\t" + x.attr.transAt + "\t" + x.attr.transDt)
    cc_edges.collect().foreach {println}
    
    //cc_results.saveAsTextFile("xrli/POC/ccgraph_20_inout_3")
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////    
     
     
    val centerCardlist = gV2.vertices.map(vp => vp._2._2).collect()
    val direct_edges = ccgraph.edges.filter {ep => centerCardlist.contains(ep.attr.dst_card) || centerCardlist.contains(ep.attr.src_card)}
    
    println("direct_edges num is ", direct_edges.count())
   
    val direct_results = direct_edges.map( x => x.attr.src_card + "\t" + x.attr.dst_card + "\t" + x.attr.transAt + "\t" + x.attr.transDt)
    println("direct_results: DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD: ")
    
//    direct_results.collect().foreach { println}
   
    //direct_results.saveAsTextFile("xrli/POC/direct_edges_20_inout_3")
    
   val core_v1 =  direct_edges.map( x => x.attr.src_card)
   val core_v2 =  direct_edges.map( x => x.attr.dst_card)
   val core_v = core_v1.union(core_v2)
    
   val schemacc = StructType(StructField("card",StringType,true)::StructField("cc",LongType,true):: Nil)
   val card_cc = ccgraph.vertices.map(f=>Row(f._2._2 ,f._2._1)) 
   val card_ccDF = sqlContext.createDataFrame(card_cc, schemacc)
   card_ccDF.registerTempTable("card_cc_tb")
   
   
   val schema_core_v_cc = StructType(StructField("card",StringType,true):: Nil)
   val core_v_cc = core_v.map(f=>Row(f))
   val core_v_ccDF = sqlContext.createDataFrame(core_v_cc, schema_core_v_cc)
   core_v_ccDF.registerTempTable("core_v_cc_tb")
   
   println("direct_vertices: DD: ")

   sqlContext.sql("select * from card_cc_tb t1 left semi join core_v_cc_tb t2 on (t1.card=t2.card)").rdd.collect.foreach {println}  
   
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
  }
  
  
  
  
  
  
}