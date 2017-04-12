package Algorithm

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import scala.collection.mutable.{Buffer,Set,Map}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
 
object Tarjan {
//private var edgeMap: scala.collection.mutable.Map[Long, List[Long]] = scala.collection.mutable.Map[Long, List[Long]]()
  
//  private val st = Buffer.empty[Int]
//  private val st_set = Set.empty[Int]
//  private val i = Map.empty[Int, Int]
//  private val lowl = Map.empty[Int, Int]
//  private val result = Buffer.empty[Buffer[Int]]
  
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.ERROR);
    Logger.getLogger("parse").setLevel(Level.ERROR);
 
    //设置运行环境
    val conf = new SparkConf().setAppName("Tarjan_Load") 
    val sc = new SparkContext(conf)
 
    val graph = GraphLoader.edgeListFile(sc, "xrli/scctest.txt")
 
   //println("vertices is : " + graph.vertices)
    //println("edges is : " + graph.edges)
    
//    val a = graph.edges.map{x => (x.srcId.toLong, x.dstId.toLong)}.combineByKey(
//      (v : Long) => List(v),
//      (c : List[Long], v : Long) => v :: c,
//      (c1 : List[Long], c2 : List[Long]) => c1 ::: c2
//    ).collect
//    
//    a.foreach(println)
    
    val edgeMaps = graph.edges.map{x => (x.srcId.toInt, x.dstId.toInt)}.combineByKey(
      (v : Int) => List(v),
      (c : List[Int], v : Int) => v :: c,
      (c1 : List[Int], c2 : List[Int]) => c1 ::: c2
    )
    
    var g = scala.collection.mutable.Map[Int, List[Int]]()

    val vRdd = graph.vertices.map(f=>f._1.toInt)
    vRdd.collect().map{v=>
      g += (v -> Nil)
    }
 
    edgeMaps.collect().map{m =>
      g += (m._1 -> m._2)
    }
    
    println(tarjan_scc(g))
    
    sc.stop()
  }
  
  
  
 def tarjan_scc(g: Map[Int, List[Int]])= {
  val st = Buffer.empty[Int]
  val st_set = Set.empty[Int]
  val i = Map.empty[Int, Int]
  val lowl = Map.empty[Int, Int]
  val result = Buffer.empty[Buffer[Int]]

  def visit(v: Int): Unit = {
    i(v) = i.size
    lowl(v) = i(v)
    st += v
    st_set += v

    for (w <- g(v)) {
      if (!i.contains(w)) {
        visit(w)
        lowl(v) = math.min(lowl(w), lowl(v))
      } else if (st_set(w)) {
        lowl(v) = math.min(lowl(v), i(w))
      }
    }

    if (lowl(v) == i(v)) {
      val scc = Buffer.empty[Int]
      var w = -1

      while(v != w) {
        w = st.remove(st.size - 1)
        scc += w
        st_set -= w
      }

      if(scc.size>1)
        result += scc
    }
  }

  for (v <- g.keys) 
    if (!i.contains(v)) 
      visit(v)
     
  result
}
 
 
 def tarjan_anyType[TP](g: Map[TP, List[TP]])= {
  val stack = collection.mutable.Stack[TP]() 
  val index = Map.empty[TP, Int]
  val lowl = Map.empty[TP, Int]
  val result = Buffer.empty[Buffer[TP]]
  var time = 0;

  def visit(v: TP): Unit = {
    index(v) = time
    lowl(v) = time
    time =time+1
    stack.push(v)
  

    for (w <- g(v)) {
      if (!index.contains(w)) {
        visit(w)
        lowl(v) = math.min(lowl(w), lowl(v))
      } else if (stack.contains(w)) {
        lowl(v) = math.min(lowl(v), index(w))
      }
    }

    if (lowl(v) == index(v)) {
      val scc = Buffer.empty[TP]
       
      var over = false
      while(!over) {
        var p = stack.pop()
        scc += p
        if(v == p)
          over = true
      }
 
      if(scc.size>1)
        result += scc
    }
  }

  for (v <- g.keys) 
    if (!lowl.contains(v)) 
      visit(v)
     
  result
}
 
 
  
  
}