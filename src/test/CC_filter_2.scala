package test

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import Algorithm._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import Algorithm._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object CC_filter_2 {
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
 
    
    
//     val testList = List(0,1,2,3,4,5,6,7,8,9,10,20,30,40,50,60,70,80,90,100,200,300,400,500,600,700,800,900,1000)
//     for(i <- testList){
//      val gV2 = graph.subgraph(vpred = (id, prop) => ((prop._3+prop._4)>20))
//      var complexCC = gV2.vertices.map(vp=>(vp._2._1, 1)).reduceByKey(_+_).filter(f=>f._2>i)
//      
//      println("complexCC num > " + i + " is : " + complexCC.count())
//    }
    
    
  
    val gV2 = graph.subgraph(vpred = (id, prop) => ((prop._3+prop._4)>30))
    var complexCC = gV2.vertices.map(vp=>(vp._2._1, 1)).reduceByKey(_+_).filter(f=>f._2>15)
      
      var complexCCRdd =  complexCC.map(f=>f._1) 
    println("complexCC num is  ", complexCC.count())
    
      
    val smallccRdd = graph.vertices.map(vp=>(vp._2._1, 1)).reduceByKey(_+_).filter(f=>f._2<3000).map(f=>f._1)
    complexCCRdd = complexCCRdd.intersection(smallccRdd) 
    println("small complexCC num is  ", complexCCRdd.count())
     
    val complexCClist = complexCCRdd.collect
    
    val ccgraph = graph.subgraph(vpred = (id, prop) => (complexCClist.contains(prop._1)))
    println("ccgraph.numVertices ", ccgraph.numVertices)
    println("ccgraph.numEdges ", ccgraph.numEdges)
    
    
    ccgraph.vertices.map{vp =>
      val vid = vp._1
      val cid = vp._2._1
      val card = vp._2._2
      vid +"\t" + cid +"\t" + card
    }.saveAsTextFile("xrli/POC/V_MCSs_Complex_4")
                       
    
 //Edge(6788115067,776216246887956,TransferProperty(3ce2d9f44086c6a1bb24c7c0d8225d06,1fc62cc1d8b0c82029f16d7d8b023932,540000,2016-11-03 12:42:09.0))   
    ccgraph.edges.map{ep =>
      val srcid = ep.srcId
      val dstid = ep.dstId
      val money = ep.attr.transAt
      val time = ep.attr.transDt
      srcid +"\t" + dstid +"\t" + money +"\t" + time
    }.saveAsTextFile("xrli/POC/E_MCSs_Complex_4")
    
    
    
//    ccgraph.vertices.saveAsTextFile("xrli/POC/V_MCSs_Complex")
//    ccgraph.edges.saveAsTextFile("xrli/POC/E_MCSs_Complex")
//    var vlist = List(0L)
//    var elist = List(0L)
//    var mlist = List(0L)
//    var dlist = List(0L)
//    
//    for(cc <- complexCClist){
//      val tmpgraph = graph.subgraph(vpred = (id, prop) => (prop._1==cc))
//      vlist = vlist.::(tmpgraph.numVertices)
//      elist = elist.::(tmpgraph.numEdges)
//      val totalmoney_mcs = tmpgraph.edges.map(f => f.attr.transAt.toLong).reduce(_+_)
//      mlist = mlist.::(totalmoney_mcs)
//      dlist = elist.::(tmpgraph.numEdges/tmpgraph.numVertices)
//    }
//    
//    println("vlist: ")
//    vlist.foreach { println }
//    println("elist: ")
//    elist.foreach { println }
//    println("mlist: ")
//    mlist.foreach { println }
//    println("dlist: ")
//    dlist.foreach { println }
    
    
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
  }
  
  
  
  
  
  
}