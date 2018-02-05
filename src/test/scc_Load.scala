package test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import Algorithm._
import org.apache.spark.graphx.Graph.graphToGraphOps

object scc_Load {
  
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.ERROR);
    Logger.getLogger("parse").setLevel(Level.ERROR);
 
    //设置运行环境
    val conf = new SparkConf().setAppName("scc_Load") 
    val sc = new SparkContext(conf)
 
    val textfile = sc.textFile("xrli/MappedIn.txt").cache  //.persist(StorageLevel.MEMORY_AND_DISK_SER) 
    // 读入时指定编码  
    val rdd1 = textfile.map(line => line.split("\\s+")(0).trim)                       
    val rdd2 = textfile.map(line => line.split("\\s+")(1).trim)
    val AllCardList = rdd1.union(rdd2).distinct()
    
    val vFile = AllCardList
    
    val verticeRDD = vFile.map { line=>
        val vid = line.toLong
        val card = line
        (vid,card)      //这里设置顶点编号就是前面的卡编号
    }
    


    val edgeRDD = textfile.map { line=>
        val lineArray = line.split("\\s+")
        val srcId = lineArray(0).toLong
        val dstId = lineArray(1).toLong
        Edge(srcId, dstId, ())
    }
 
  
    // 定义一个默认用户，避免有不存在用户的关系  
    val graph = Graph(verticeRDD, edgeRDD) 
 
 
    println("vertice num is : " + graph.numVertices)
    println("edge num is : " + graph.numEdges)

    
    var g = scala.collection.mutable.Map[Int, List[Int]]()

    val vRdd = graph.vertices.map(f=>f._1.toInt)
    vRdd.collect().map{v=>
      g += (v -> Nil)
    }
 
    
    val edgeRdd = graph.edges.map{x => (x.srcId.toInt, x.dstId.toInt)}.combineByKey(
      (v : Int) => List(v),
      (c : List[Int], v : Int) => v :: c,
      (c1 : List[Int], c2 : List[Int]) => c1 ::: c2
    )
    
    edgeRdd.collect().map{m =>
      g += (m._1 -> m._2)
    }
    
    println(Tarjan.tarjan_scc(g))
    
    //val a = graph.stronglyConnectedComponents(5)
    //println(a.numVertices)
    
    sc.stop()
  }
  
}