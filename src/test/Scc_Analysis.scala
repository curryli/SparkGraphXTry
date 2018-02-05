package test

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
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import Algorithm._

object Scc_Analysis {
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
    
    //    var data = hc.sql(s"select tfr_in_acct_no,tfr_out_acct_no,default.md5_int(tfr_in_acct_no) as tfr_in_acct_no_id, default.md5_int(tfr_out_acct_no) as tfr_out_acct_no_id,trans_at  from tbl_common_his_trans where " +
    //      s"unix_timestamp(trans_dt_tm,'yyyyMMddHHmmss') > unix_timestamp('20161001','yyyyMMdd')  and " +
    //      s"unix_timestamp(trans_dt_tm,'yyyyMMddHHmmss') < unix_timestamp('20160601','yyyyMMdd')  and " +
    //      s"trans_id='S33'")

    val data = hc.sql(s"select " +
      s"tfr_out_acct_no," +
      s"tfr_in_acct_no," +
      s"cast (trans_at as int) as money, " +
      s"to_ts " +
      s"from 00010000_default.tbl_poc_test " +
      s" where trans_at is not null and trans_at>1000 and tfr_in_acct_no is not null and tfr_out_acct_no is not null and to_ts is not null").repartition(10).cache()

      println("SQL done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
      
//5d2bfedebff7b6ffb14ac28e64403eed        e2f30408d23c94affba0116221627eb9        1000000 2016-11-01 15:16:24.0
//2393dbfeb5054fff6edfc7607094b4bc        ccf345475c90dddd964a988ab2f6d087        2500000 2016-11-01 13:50:10.0

    println("start sql ....")

    //1.构建边:转出卡、转入卡、转账金额、转账时间
    val transferRelationRDD = data.map {
      r =>
        val srcId = HashEncode.HashMD5(r.getString(0))
        val dstId = HashEncode.HashMD5(r.getString(1))
        val item = new TransferProperty(r.getInt(2), r.getString(3)) //金额、时间
        Edge(srcId, dstId, item) // srcId,destId
    }

    //2.构建顶点：

    //2.1 转入卡
    val inCardRDD = data.map {
      r =>
        val item = new CardVertex(r.getString(0), 0, 0)
        (HashEncode.HashMD5(r.getString(0)), item) //destId
    }

    //2.2 转出卡
    val outCardRDD = data.map {
      r =>
        val item = new CardVertex(r.getString(1), 0, 0)
        (HashEncode.HashMD5(r.getString(1)), item) //srcId
    }

    //2.3 顶点合并
    val cardRDD = inCardRDD.union(outCardRDD).distinct()

    //3. 构建图
    var g = Graph(cardRDD, transferRelationRDD).partitionBy(PartitionStrategy.RandomVertexCut)

    //3.1  边聚合
    g = g.groupEdges((a, b) => new TransferProperty(a.transAt+b.transAt, a.transDt))
    
    //3.2 计算出入度
    val degreeGraph = g.outerJoinVertices(g.inDegrees) {
      (id, v, inDegOpt) => CardVertex(v.priAcctNo, inDegOpt.getOrElse(0), v.outDgr)
    }.outerJoinVertices(g.outDegrees) {
      (id, v, outDegOpt) => CardVertex(v.priAcctNo, v.inDgr, outDegOpt.getOrElse(0))
    }

    //3.3 筛选边出入度和为2的图:两张卡号单向转账>=1次,待用
    val degree2Graph = degreeGraph.subgraph(vpred => (vpred.srcAttr.inDgr + vpred.srcAttr.outDgr + vpred.dstAttr.inDgr + vpred.dstAttr.outDgr) == 2)

    //3.4 去除边出入度和为2的图
    var gV1 = degreeGraph.subgraph(vpred => (vpred.srcAttr.inDgr + vpred.srcAttr.outDgr + vpred.dstAttr.inDgr + vpred.dstAttr.outDgr) > 2)

    //重新添加出入度，为了去掉degree2Graph的顶点
    gV1 = gV1.outerJoinVertices(gV1.inDegrees) {
      (id, v, inDegOpt) => CardVertex(v.priAcctNo, inDegOpt.getOrElse(0), v.outDgr)
    }.outerJoinVertices(gV1.outDegrees) {

      (id, v, outDegOpt) => CardVertex(v.priAcctNo, v.inDgr, outDegOpt.getOrElse(0))
    }

    println("gV1 done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
    
    //根据顶点的出入度，筛选顶点:边和点剩余15%
    val gV2 = gV1.subgraph(vpred = (id, card) => !(card.inDgr == 0 && card.outDgr == 0))
 
 
    println("Load done in " + (System.currentTimeMillis()-startTime) + " ms.")
    println("graph.numvertices is " + gV2.numVertices)
    println("graph.numEdges is " + gV2.numEdges)

    
    
   ////////////////////////////////////////////////////4 Tarjan算法计算强联通图////////////////////////////////////////////////////////////
    val cardmap_Rdd = gV2.vertices.map(v => (v._1.toLong, v._2))
    val cardmap = scala.collection.mutable.Map[Long, String]()
      
    cardmap_Rdd.collect().foreach{p=>
      cardmap.put(p._1, p._2.priAcctNo)
    }
     
    var gMap = scala.collection.mutable.Map[Long, List[Long]]()

    val vRdd = gV2.vertices.map(f=>f._1.toLong)
    vRdd.collect().map{v=>
      gMap += (v -> Nil)
    }
 
    val edgeRdd = gV2.edges.map{x => (x.srcId.toLong, x.dstId.toLong)}.combineByKey(
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
    val scc_related_graph = gV2.subgraph(vpred = (id, prop) => (scc_card_set.contains(prop.priAcctNo)))
     
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
  
    val v_cc_RDD = v_prop_RDD.map(f=>(f._1, f._2))   //卡节点号，cc号
     
    val cc_Graph = scc_related_graph.outerJoinVertices(v_cc_RDD){
      (vid, cardstr, ccOpt) => (cardstr, ccOpt.getOrElse(0L))
    }
    println("cc_Graph done in " + (System.currentTimeMillis()-startTime) + " ms.")
    println("cc_Graph.numvertices is " + cc_Graph.numVertices)
    println("cc_Graph.numEdges is " + cc_Graph.numEdges) 
   
    val small_cc = cc_Graph.vertices.map(vp=>(vp._2._2, 1)).reduceByKey(_+_).filter(f=>f._2>50 && f._2<1000).map(f=>f._1)
 //   val smallcclist = small_cc.collect
    
    val simple_cc = v_prop_RDD.map(f=>(f._2, (f._3,f._4))).filter(f => f._2._1>50 || f._2._2>50).map(f=>(f._1,1)).reduceByKey(_+_).filter(f=>f._2<10).map(f=>f._1)
    val interest_cc = small_cc.intersection(simple_cc)
    
    val smallcclist = interest_cc.collect()
   
 
    
    val small_circles_graph = cc_Graph.subgraph(vpred = (id, prop) => (smallcclist.contains(prop._2)))
    
    println("small_circles_graph.numVertices ", small_circles_graph.numVertices)
    println("small_circles_graph.numEdges ", small_circles_graph.numEdges)
    
    
     val small_circle_vertices = small_circles_graph.vertices.map(f=>f._2._1.priAcctNo + "\t" + f._2._2)
     small_circle_vertices.collect().foreach {println}
    
     var ccmap = scala.collection.mutable.Map[Long, Long]()
    
     small_circles_graph.vertices.map(f=>(f._1.toLong, f._2._2)).collect.foreach(f=> ccmap += (f._1-> f._2))
//     println("ccmap: CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC")
//     ccmap.foreach(println)
  
     println("cc_edges EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE: ")
    
     val small_circle_edges = small_circles_graph.edges.map( x => cardmap(x.srcId) + "\t" + cardmap(x.dstId)  + "\t" + ccmap(x.srcId) + "\t" + x.attr.transAt/100 + "\t" + x.attr.transDt )
     small_circle_edges.collect().foreach {println}
    
   println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
  }
  
  
  
  
  
  
}