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

object Maped_scc_card {
  class VertexProperty()
  class EdgePropery()

  //顶点: 卡片，属性：id, 帐号，
  case class CardVertex(val priAcctNo: String, val inDgr: Int, val outDgr: Int) extends VertexProperty

  case class TransferProperty(val transAt: Int, val transDt: String) extends EdgePropery
 
  
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
    
    //    var data = hc.sql(s"select tfr_in_acct_no,tfr_out_acct_no,default.md5_int(tfr_in_acct_no) as tfr_in_acct_no_id, default.md5_int(tfr_out_acct_no) as tfr_out_acct_no_id,trans_at  from tbl_common_his_trans where " +
    //      s"unix_timestamp(trans_dt_tm,'yyyyMMddHHmmss') > unix_timestamp('20161001','yyyyMMdd')  and " +
    //      s"unix_timestamp(trans_dt_tm,'yyyyMMddHHmmss') < unix_timestamp('20160601','yyyyMMdd')  and " +
    //      s"trans_id='S33'")

    val data = hc.sql(s"select " +
      s"tfr_in_acct_no," +
      s"tfr_out_acct_no," +
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
 
    
    val cardmap_Rdd = gV2.vertices.map(v => (v._1.toLong, v._2.priAcctNo))
    val cardmap = scala.collection.mutable.Map[Long, String]()
      
    cardmap_Rdd.collect().foreach{p=>
      cardmap.put(p._1, p._2)
    }
    
    cardmap.foreach(println)
////////////////////////////////////////////////////4 Tarjan算法计算强联通图////////////////////////////////////////////////////////////
    println("current vertice num is : " + gV2.numVertices)
    println("current edge num is : " + gV2.numEdges)
    
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
    
    scc_rdd.sortBy(f => f._1, false).saveAsTextFile("xrli/sscRddFile")
    //scc_rdd.map(x=>(x._1, x._2))
    //scc_rdd.saveAsTextFile("xrli/sscRddFile")
    
    
    
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////    
     
    println("All done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes.")
  }
  
  
  
  
  
  
}