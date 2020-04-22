package util.message

import akka.actor.ActorRef
import akka.routing.ConsistentHashingRouter.ConsistentHashable


case class Init_Map(title: String, url: String)

case class ReduceNameTitlePair(name: String, title: String)

case class FlushReduce(msg: String)

case object Flush

case class AddMapActors(actors: List[ActorRef])

case class AddReducerActors(actors: List[ActorRef])

case class Send_ConsistentHashRouter(route: ActorRef)

case object START

case object SHUTDOWN

//messages supporting consistent hash routing
case object Done extends ConsistentHashable {
  override def consistentHashKey: Any = "Done"
}

case class NumberOfMapActors(numberOfMapActor:Int) extends ConsistentHashable {
  override def consistentHashKey: Any = "Update the Map Actor Counter"+numberOfMapActor
}
