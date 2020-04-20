package util.message

import akka.actor.ActorRef


case class Init_Map(title:String,url:String)

case class ReduceNameTitlePair(name:String, title:String)

case object Flush
case object Done

case class AddReduce(actor: ActorRef)
case class AddMapper(actor: ActorRef)

case class AddReducers(actors: List[ActorRef])

case object InitActors

