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
import Algorithm._

object Save_ConnectComponent_Card {
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
 
    val data = hc.sql(s"select " +
      s"tfr_out_acct_no," +
      s"tfr_in_acct_no," +
      s"cast (trans_at/100 as int) as money, " +
      s"to_ts " +
      s"from 00010000_default.tbl_poc_test " +
      s" where trans_at is not null and trans_at>=500000 and tfr_in_acct_no is not null and tfr_out_acct_no is not null and to_ts is not null").repartition(10).cache()

      println("SQL done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
      
//5d2bfedebff7b6ffb14ac28e64403eed        e2f30408d23c94affba0116221627eb9        1000000 2016-11-01 15:16:24.0
//2393dbfeb5054fff6edfc7607094b4bc        ccf345475c90dddd964a988ab2f6d087        2500000 2016-11-01 13:50:10.0

    println("start sql ....")

    //1.构建边:转出卡、转入卡、转账金额、转账时间
    val transferRelationRDD = data.map {
      r =>
        val srcId = HashEncode.HashMD5(r.getString(0))
        val dstId = HashEncode.HashMD5(r.getString(1))
        val item = new TransferProperty( r.getString(0), r.getString(1) , r.getInt(2), r.getString(3)) //金额、时间
        Edge(srcId, dstId, item) // srcId,destId
    }

    //2.构建顶点：
 
    val outCardRDD = data.map {
      r =>
        val item = new CardVertex(r.getString(0), 0, 0)
        (HashEncode.HashMD5(r.getString(0)), item) //destId
    }

     
    val inCardRDD = data.map {
      r =>
        val item = new CardVertex(r.getString(1), 0, 0)
        (HashEncode.HashMD5(r.getString(1)), item) //srcId
    }

    //2.3 顶点合并
    val cardRDD = inCardRDD.union(outCardRDD).distinct()

    //3. 构建图
    var g = Graph(cardRDD, transferRelationRDD).partitionBy(PartitionStrategy.RandomVertexCut)


    //3.1  边聚合
    g = g.groupEdges((a, b) => new TransferProperty(a.src_card, a.dst_card, a.transAt+b.transAt, a.transDt))

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

    //根据顶点的出入度，筛选顶点:边和点剩余15%
    val gV2 = gV1.subgraph(vpred = (id, card) => !(card.inDgr == 0 && card.outDgr == 0)).cache()
    data.unpersist()

    //4 计算联通图
   
    val cGraph = ConnectedComponents.run(gV2)
     
    //    
    val ccgraph = gV2.outerJoinVertices(cGraph.vertices){
      (vid, tempProperty, connectedOpt) => (tempProperty, connectedOpt.getOrElse(0L))
    }
    // ccgraph   (vid,顶点属性，对应团体)
    
//    val verticeStr = ccgraph.vertices.map{vp =>
//      val cc = vp._2._2
//      val card = vp._2._1.priAcctNo
//      vp._1 +"," + cc +"," + card
//    }
//    //50323566228396,50323566228396,2d7e9ea68b99f5f700e90828726886af     卡号id， 团体id 卡号名
//    verticeStr.saveAsTextFile("xrli/POC/cc_10_verticeStr")
    
    
    //保存数据
    //(869004002626858,(CardVertex(5db0180f8a2bb3a17f0a48276ad1bb0a,2,0),275790015286939))
//  卡号，                                                                                            卡号名，入度 ，出度     对应团体                
    ccgraph.vertices.map{vp =>
      val vid = vp._1
      val vprop = vp._2._1
      val cid = vp._2._2
      vid +"\t" + vprop +"\t" + cid
    }.saveAsTextFile("xrli/POC/cc_10_vertices")
                       
    
 //Edge(6788115067,776216246887956,TransferProperty(3ce2d9f44086c6a1bb24c7c0d8225d06,1fc62cc1d8b0c82029f16d7d8b023932,540000,2016-11-03 12:42:09.0))   
    ccgraph.edges.map{ep =>
      val srcid = ep.srcId
      val dstid = ep.dstId
      val eprop = ep.attr
      srcid +"\t" + dstid +"\t" + eprop
    }.saveAsTextFile("xrli/POC/cc_10_edges")
    
    //想要把顶点和边的信息全部加载的时候参考TransNet.LoadBigHash。  只想加载边信时使用LoadSccCard.scala
    
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
  

  }
}
