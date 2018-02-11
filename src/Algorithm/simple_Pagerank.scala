package Algorithm
import scala.reflect.ClassTag
import org.apache.spark.graphx._
import scala.Iterator
import org.apache.spark.Logging
import org.apache.spark.graphx._
//http://blog.csdn.net/lsshlsw/article/details/41176093

object simple_Pagerank {
  
  //第一种（静态）PageRank模型      在调用时提供一个参数number，用于指定迭代次数，即无论结果如何，该算法在迭代number次后停止计算，返回图结果。
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], numIter: Int, resetProb: Double = 0.15): Graph[Double, Double] ={
    //下列这段代码用于初始化PageRank图模型，具体内容是赋予每个顶点属性为值1.0，赋予每条边属性为值“1/该边的出发顶点的出度数”。 
    val pagerankGraph: Graph[Double, Double] = graph
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }   //vdata是原来graph的顶点属性   deg是graph.outDegrees的属性           => deg.getOrElse(0)表示只保留deg属性
      .mapTriplets( e => 1.0 / e.srcAttr )
      .mapVertices( (id, attr) => 1.0 )
      .cache()

    //用于得到一个迭代器，把    该边的出发顶点的出度数*该边权重 （该边传递的实际PR值）  发送给dstId
    def sendMessage(edge: EdgeTriplet[Double, Double]) =
      Iterator((edge.dstId, edge.srcAttr * edge.attr))
      
    //用于将顶点属性值和传递的值进行累加  
    def messageCombiner(a: Double, b: Double): Double = a + b

     //用于计算计算接收到消息之后，当前节点的权重   resetProb = 0.15  就是随机跳出概率
    def vertexProgram(id: VertexId, attr: Double, msgSum: Double): Double =
      resetProb + (1.0 - resetProb) * msgSum
      
      
    val initialMessage = 0.0

    val pregelGraph = myPregel(pagerankGraph, initialMessage, numIter, activeDirection = EdgeDirection.Out)(
      vertexProgram, sendMessage, messageCombiner)
      
    pagerankGraph.unpersist(blocking=false)
    pregelGraph
  }

  
  
  //第二种（动态）PageRank模型    在调用时提供一个参数tol，用于指定前后两次迭代的结果差值应小于tol，以达到最终收敛的效果时才停止计算，返回图结果。
  //初始化参数和上面不同的是少了numIter（迭代次数），多了tol（比较两次迭代的结果差）
  def runUntilConvergence[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], tol: Double, numIter: Int=200, resetProb: Double = 0.15): Graph[Double, Double] =
  {
 
    val pagerankGraph: Graph[(Double, Double), Double] = graph
      .outerJoinVertices(graph.outDegrees) {(vid, vdata, deg) => deg.getOrElse(0)}
      .mapTriplets( e => 1.0 / e.srcAttr )
      .mapVertices( (id, attr) => (0.0, 0.0) )   //顶点属性值的初始化，但是属性值带两个参数即（初始PR值，两次迭代结果的差值）
      .cache()

  // 同样用于得到一个迭代器，但是多了一个条件判定：如果源顶点的delta值小于tol就清空迭代器即返回空迭代。  
    def sendMessage(edge: EdgeTriplet[(Double, Double), Double]) = {
      if (edge.srcAttr._2 > tol) {
        Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
      } else {
        Iterator.empty
      }
    }

      //消息是deg ，double类型
    def messageCombiner(a: Double, b: Double): Double = a + b

    
   //  多了一个返回值delta（newPR-oldPR）  
    def vertexProgram(id: VertexId, attr: (Double, Double), msgSum: Double): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      val newPR = oldPR + (1.0 - resetProb) * msgSum
      (newPR, newPR - oldPR)
    }
    
    // 每个顶点接受到的初始传递信息值不是0，而是resetProb / (1.0 - resetProb)  
    val initialMessage = resetProb / (1.0 - resetProb)

    // 动态执行 Pregel 模型（直至结果最终收敛）  
    val pregelGraph = myPregel(pagerankGraph, initialMessage, numIter, activeDirection = EdgeDirection.Out)(
      vertexProgram, sendMessage, messageCombiner).mapVertices((vid, attr) => attr._1)
      
            
    pagerankGraph.unpersist(blocking=false)
    pregelGraph
  } 
}