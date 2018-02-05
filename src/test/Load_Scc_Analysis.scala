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

object Load_Scc_Analysis {
  class VertexProperty()
  class EdgePropery()

  //顶点: 卡片，属性：id, 帐号，
  //顶点: 卡片，属性：id, 帐号，
  case class CardVertex(val priAcctNo: String, val inDgr: Int, val outDgr: Int) extends VertexProperty

  case class TransferProperty(val transAt: Int, val transDt: String) extends EdgePropery
 
 

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
    
    val verticefile = sc.textFile("xrli/POC/prepare_1000_vertices")//.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val edgefile = sc.textFile("xrli/POC/prepare_1000_edges")//.persist(StorageLevel.MEMORY_AND_DISK_SER)
    
 
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
       val item = new TransferProperty(proplist(2).toInt, proplist(3)) //金额、时间
       Edge(srcId, dstId, item) // srcId,destId
    }
  
    val graph = Graph(verticeRDD, edgeRDD) 
    println("Load done in " + (System.currentTimeMillis()-startTime) + " ms.")
    println("graph.numvertices is " + graph.numVertices)
    println("graph.numEdges is " + graph.numEdges)

    
    
   ////////////////////////////////////////////////////4 Tarjan算法计算强联通图////////////////////////////////////////////////////////////
    val cardmap_Rdd = graph.vertices.map(v => (v._1.toLong, v._2))
    val cardmap = scala.collection.mutable.Map[Long, String]()
      
    cardmap_Rdd.collect().foreach{p=>
      cardmap.put(p._1, p._2)
    }
     
    var gMap = scala.collection.mutable.Map[Long, List[Long]]()

    val vRdd = graph.vertices.map(f=>f._1.toLong)
    vRdd.collect().map{v=>
      gMap += (v -> Nil)
    }
 
    val edgeRdd = graph.edges.map{x => (x.srcId.toLong, x.dstId.toLong)}.combineByKey(
      (v : Long) => List(v),
      (c : List[Long], v : Long) => v :: c,
      (c1 : List[Long], c2 : List[Long]) => c1 ::: c2
    )
    
    edgeRdd.collect().map{m =>
      gMap += (m._1 -> m._2)
    }

    //println(Tarjan.tarjan_anyType(gMap))
    val scc_buffers = Tarjan.tarjan_anyType(gMap)
    println("Tarjan done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    
    val scc_rdd = sc.parallelize(scc_buffers).map { xbuff => 
      val len = xbuff.size
      var cardlist = List[String]() 
      for(i<- xbuff) { cardlist = cardlist.::(cardmap(i))}
      //println(cardlist)
      (len, cardlist)
    }
    
    //scc_rdd.filter(f=>f._1>5).sortBy(f => f._1, false).collect.foreach(println)
    val scc_list = scc_rdd.filter(f=>f._1>5).map(f=>f._2).collect
    
    var scc_card_set = scala.collection.mutable.Set[String]()
    
    scc_list.foreach { xlist => 
      xlist.foreach { x => scc_card_set.add(x) }   
    }
    
    println("scc_card_set done in " + (System.currentTimeMillis()-startTime) + " ms.")
    //scc_card_set.foreach { println} 
    
/////////////////////////////////////////////////////////////////////再筛选////////////////////////////////////////////    
    
    
    
    //根据顶点的出入度，筛选顶点:边和点剩余15%
    val scc_related_graph = graph.subgraph(vpred = (id, prop) => (scc_card_set.contains(prop)))
     
   //连通子图
    val v_cc_file = sc.textFile("xrli/POC/cc_10_vertices")
    val v_prop_RDD = v_cc_file.map { line=>
        val lineArray = line.split("\t")
        val vid = lineArray(0).toLong
        
        val vpropstr = lineArray(1).trim().split("CardVertex\\(")(1)
        val vproplist = vpropstr.substring(0, vpropstr.length()-1).split(",") 
        val cardstr = vproplist(0)
        val indeg = vproplist(1).toInt
        val outdeg = vproplist(2).toInt
        
        val cid = lineArray(2).trim().toLong
        (vid, cid, indeg, outdeg)      //这里设置顶点编号就是前面的卡编号
    }
  
    val v_cc_RDD = v_prop_RDD.map(f=>(f._1, f._2))
     
    val cc_Graph = scc_related_graph.outerJoinVertices(v_cc_RDD){
      (vid, cardstr, ccOpt) => (cardstr, ccOpt.getOrElse(0L))
    }
     println("cc_Graph done in " + (System.currentTimeMillis()-startTime) + " ms.")
    println("cc_Graph.numvertices is " + cc_Graph.numVertices)
    println("cc_Graph.numEdges is " + cc_Graph.numEdges) 
   
    val small_cc = cc_Graph.vertices.map(vp=>(vp._2._2, 1)).reduceByKey(_+_).filter(f=>f._2>10 && f._2<1000).map(f=>f._1)
    val smallcclist = small_cc.collect
    
 //   val simple_cc = v_prop_RDD.map(f=>(f._2, (f._3,f._4))).filter(f => f._2._1>50 || f._2._2>50).map(f=>(f._1,1)).reduceByKey(_+_).filter(f=>f._2<10).map(f=>f._1)
//    val smallcclist = small_cc.intersection(simple_cc).collect()
    
    
    val small_circles_graph = cc_Graph.subgraph(vpred = (id, prop) => (smallcclist.contains(prop._2)))
    
    println("small_circles_graph.numVertices ", small_circles_graph.numVertices)
    println("small_circles_graph.numEdges ", small_circles_graph.numEdges)
    
    
     val small_circle_vertices = small_circles_graph.vertices.map(f=>f._2._1 + "\t" + f._2._2)
     small_circle_vertices.collect().foreach {println}
  
     println("cc_edges EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE: ")
    
     val small_circle_edges = small_circles_graph.edges.map( x => cardmap(x.srcId) + "\t" + cardmap(x.dstId) + "\t" + x.attr.transAt/100 + "\t" + x.attr.transDt)
     small_circle_edges.collect().foreach {println}
    
   println("All done in " + (System.currentTimeMillis()-startTime) + " ms.")
  }
  
  
  
  
  
  
}