package Demo

//找到一个顶点的一跳节点和二跳节点

//思路：源顶点赋值为2，其他顶点赋值为-1，从源顶点向出边走一步，属性减一即为1，然后这些为1的属性再向出边走一步，属性再减一即为0，如果有多条路径传递到一个顶点，取大的。最后统计为标签为1的即为1跳顶点，标签为0的即为2跳顶点。


import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD 
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx.lib._


object jump_node {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    //设置运行环境
    val conf = new SparkConf().setAppName("GraphLoader") 
    val sc = new SparkContext(conf)
    
    
    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, "xrli/graphx/testedges2.txt").cache
     
    def sendMsgFunc(edge: EdgeTriplet[Int, Int]) = {
      if (edge.srcAttr <= 0) {
        if (edge.dstAttr <= 0) {
          Iterator.empty//都小于0，说明从源节点还没有传递到这里
        }else {
          Iterator((edge.srcId,edge.dstAttr - 1))//目的节点大于0，将目的节点属性减一赋值给源节点
        }
      }else {
        if(edge.dstAttr <= 0) {
          Iterator((edge.dstId,edge.srcAttr -1))//源节点大于0，将源节点属性减一赋值给目的节点
        }else {
          Iterator.empty//都大于0，说明在二跳节点以内，不操作
        }
 
      }
    }
     
    //初始化一个新图，每一个顶点的属性集合都为空 
    val initialGraph =  graph.mapVertices((vid,value) => if(vid ==2) 2 else -1) //初始化信息，源节点为2，其他节点为-1
    
    val friends = initialGraph.pregel(-1,2,EdgeDirection.Either)(   //最大迭代次数为2
      vprog = (vid,attr,msg) => math.max(attr, msg),//顶点操作，到来的属性和原属性比较，较大的作为该节点的属性
      sendMsgFunc,
      (a,b) => math.max(a, b)//当有多个属性传递到一个节点，取大的，因为大的离源节点更近
    ).subgraph(vpred =(vid,v) =>v >= 0)
    
    
    println("friends")
    friends.vertices.collect.foreach(println(_))
    sc.stop
  }
}


//friends
//(4,1)
//(0,1)
//(6,1)
//(2,2)
//(1,0)
//(3,0)
//(5,1)