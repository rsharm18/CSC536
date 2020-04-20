package util.message

import akka.actor.ActorRef
import akka.routing.ConsistentHashingRouter.ConsistentHashable


case class Init_Map(title:String,url:String,reduceRouter:ActorRef)

case class ReduceNameTitlePair(name:String, title:String)

case class FlushReduce(msg:String)

case object Flush
case object Done extends ConsistentHashable {
override def consistentHashKey: Any = "Done"
}

case class AddMapActors(actors: List[ActorRef])
case class AddReducerActors(actors: List[ActorRef])

case object InitActors
case object START
case object SHUTDOWN
case object FINISHED_MAPPING

case object FLUSH_OUTPUT extends ConsistentHashable {
  override def consistentHashKey: Any = "FLUSH_OUTPUT"
}