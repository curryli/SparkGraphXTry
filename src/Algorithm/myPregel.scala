package Algorithm
import scala.reflect.ClassTag
import org.apache.spark.Logging
import org.apache.spark.graphx._
import org.apache.spark.storage._


 
object myPregel {

   
  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
     (graph: Graph[VD, ED],
      initialMsg: A,
      maxIterations: Int = Int.MaxValue,
      activeDirection: EdgeDirection = EdgeDirection.Either)
     (vprog: (VertexId, VD, A) => VD,
      sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
      mergeMsg: (A, A) => A)
    : Graph[VD, ED] =
  {
    var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).cache()
    // compute the messages
    var messages = g.mapReduceTriplets(sendMsg, mergeMsg)
    var activeMessages = messages.count()
//      println("Messages count:\t" + activeMessages)
    // Loop
    var prevG: Graph[VD, ED] = null
    var i = 0
    var ls_count=0l
    var flagCount=0l //count 计数一样count
    while (activeMessages > 0 && i < maxIterations &&flagCount<3) {
      // Receive the messages and update the vertices.
//          println("Pregel start iteration " + i)
      prevG = g
      g = g.joinVertices(messages)(vprog).cache

      val oldMessages = messages
      ls_count=activeMessages
      // Send new messages, skipping edges where neither side received a message. We must cache
      // messages so it can be materialized on the next line, allowing us to uncache the previous
      // iteration.
      messages = g.mapReduceTriplets(
        sendMsg, mergeMsg, Some((oldMessages, activeDirection))).cache
      // The call to count() materializes `messages` and the vertices of `g`. This hides oldMessages
      // (depended on by the vertices of g) and the vertices of prevG (depended on by oldMessages
      // and the vertices of g).
      activeMessages = messages.count()
//      println("Messages count:\t" + activeMessages)
//      if(activeMessages<100){
//        println(messages.collect().mkString("\n"))
//      }
      if(ls_count==activeMessages){
        flagCount=flagCount+1
      }else{
        flagCount=0
      }
       
//      println("Pregel finished iteration " + i)

      // Unpersist the RDDs hidden by newly-materialized RDDs
      oldMessages.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)
      // count the iteration
      i += 1
    }

    messages.unpersist(blocking=false)
    g
  } // end of apply

} // end of class Pregel
