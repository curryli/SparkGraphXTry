import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD 
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx.lib._


object KcoreTest {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    //设置运行环境
    val conf = new SparkConf().setAppName("GraphLoader") 
    val sc = new SparkContext(conf)
 
    val friendsGraph = GraphLoader.edgeListFile(sc, "xrli/graphx/gephiByname.net")

    var degreeGraph = friendsGraph.outerJoinVertices(friendsGraph.degrees) {
     (vid, oldData, newData) => newData.getOrElse(0)
    }.cache()                   //生成一个新图degreeGraph，每个图的顶点属性只有degree，即vertexRDD是(vid, degree)

    val kNum = 9
    var lastVerticeNum: Long = degreeGraph.numVertices     //计算图的顶点总数,返回Long型
    var thisVerticeNum: Long = -1
    var isConverged = false
    val maxIter = 1000
    var i = 1
    while (!isConverged && i <= maxIter) {  //删除 degre e< kNum 的顶点
    val subGraph = degreeGraph.subgraph(
    vpred = (vid, degree) => degree >= kNum
    ).cache()

    degreeGraph = subGraph.outerJoinVertices(subGraph.degrees) {   //重新生成新的degreeGraph
     (vid, vd, degree) => degree.getOrElse(0)
    }.cache()

    thisVerticeNum = degreeGraph.numVertices
    
    if (lastVerticeNum == thisVerticeNum) {
      isConverged = true
      println("vertice num is " + thisVerticeNum + ", iteration is " + i)
    } else {
      println("lastVerticeNum is " + lastVerticeNum + ", thisVerticeNum is " + thisVerticeNum + ", iteration is " + i + ", not converge")
      lastVerticeNum = thisVerticeNum
    }
    i += 1
  } 
  // do something to degreeGraph 
    
    //degreeGraph.edges.saveAsTextFile("xrli/degreeGraphEdges")
    
//Edge(54459,9931,1)
//Edge(54459,12384,1)
//Edge(54459,13909,1)
//Edge(54459,15502,1)
//Edge(54459,20016,1)
      
    degreeGraph.edges.map(edge =>(edge.srcId, edge.dstId)).saveAsTextFile("xrli/degreeGraphEdges")
    
    sc.stop()
  }
}