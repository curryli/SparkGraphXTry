package Algorithm
import scala.reflect.ClassTag
import org.apache.spark.Logging
import org.apache.spark.graphx._
import org.apache.spark.storage._

//一般情况使用mapReduceTriplets可以解决很多问题，为什么Spark GraphX会提供Pregel API?主要是为了更方便地去做迭代操作。因为在GraphX里面，Graph这张图并没有自动cache，而是手动cache。但是为了每次迭代更快，需要手动去做cache，每次迭代完就需要把没用的删除掉而把有用的保留，这比较难以控制。因为Graph中的点和边是分开进行Cache的，而Pregel能够帮助我们。
//pregel 原理：  每一个节点有两种状态：活跃与不活跃，刚開始计算的时候，每一个节点都处于活跃状态，随着计算的进行，某些节点完毕计算任务转为不活跃状态，假设处于不活跃状态的节点接收到新的消息，则再次转为活跃，假设图中全部的节点都处于不活跃状态，则计算任务完毕，Pregel输出计算结果。
//在pregel中顶点有两种状态：活跃状态（active）和不活跃状态（halt）。如果某一个顶点接收到了消息并且需要执行计算那么它就会将自己设置为活跃状态。如果没有接收到消息或者接收到消息，但是发现自己不需要进行计算，那么就会将自己设置为不活跃状态。

//两个关于persist的最佳实践
//http://blog.csdn.net/yirenboy/article/details/47844677
//http://www.ppvke.com/Blog/archives/34829
object myPregel {

   
  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
     (graph: Graph[VD, ED],
      initialMsg: A,
      maxIterations: Int = Int.MaxValue,
      activeDirection: EdgeDirection = EdgeDirection.Either)
     (vprog: (VertexId, VD, A) => VD,     // 参数类型是函数， 输入类型是(VertexId, VD, A)  返回类型是VD 也就是说新返回的节点属性类型与原来节点属性类型要保持一致
      sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
      mergeMsg: (A, A) => A)
    : Graph[VD, ED] =
  {
    //第一轮迭代，对每个节点用vprog函数计算。 
    var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg))//.cache()

    // 根据发送、聚合信息的函数计算下次迭代用的信息。  
    //mapReduceTriplets操作返回VertexRDD [A] ，包含所有以每个顶点作为目标节点集合消息（类型A）。没有收到消息的顶点不包含在返回VertexRDD。
    var messages = g.mapReduceTriplets(sendMsg, mergeMsg)
    
    //节点活跃 的个数
    var activeMessages = messages.count()
 
    // 下面进入循环迭代  
    var prevG: Graph[VD, ED] = null
    var i = 0
 
  //while (activeMessages > 0 && i < maxIterations &&flagCount<3) {
    while (activeMessages > 0 && i < maxIterations) {
      // Receive the messages and update the vertices.
      prevG = g
      g = g.joinVertices(messages)(vprog).cache

      val oldMessages = messages
 
      // Send new messages, skipping edges where neither side received a message.
      //mapReduceTriplets 有一个附加的可选activeSet: Option[(VertexRDD[_], EdgeDirection)] 
      //该EdgeDirection指定了哪些和顶点相邻的边包含在map阶段。
      //如果该方向是in，则用户定义的map函数将仅仅作用目标顶点在活跃集中。
      //如果方向是 out，则该map函数将仅仅作用在源顶点在活跃集中的边。
      //如果方向是 both，则map函数将仅作用在两个顶点都在活跃集中。
      //如果方向是either，则map 函数将在任一顶点在活动集中的边。

      //We must cache messages so it can be materialized on the next line, allowing us to uncache the previous iteration.
      messages = g.mapReduceTriplets(
        sendMsg, mergeMsg, Some((oldMessages, activeDirection))).cache
        
        
      // The call to count() materializes `messages` and the vertices of `g`. This hides oldMessages
      // (depended on by the vertices of g) and the vertices of prevG (depended on by oldMessages
      // and the vertices of g).
      activeMessages = messages.count()
 
      println("current activeMessages count is: "  + activeMessages)
       
      oldMessages.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)
      // count the iteration
      i += 1
    }

    messages.unpersist(blocking=false)
    g
  }  

} 
