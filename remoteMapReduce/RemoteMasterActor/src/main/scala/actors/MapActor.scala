package actors

import akka.actor.{Actor, ActorRef, ActorSelection, Props}
import akka.remote.routing.RemoteRouterConfig
import akka.routing.ConsistentHashingRouter.ConsistentHashMapping
import akka.routing.{Broadcast, ConsistentHashingPool, RoundRobinPool}
import util.helper.MapperHelper
import util.message.{AddReducerActors, FINISHED_MAPPING, Flush, FlushReduce, Init_Map, ReduceNameTitlePair}


class MapActor(reduceActors:List[ActorRef]) extends Actor {

  val STOP_WORDS_LIST = List("a", "am", "an", "and", "are", "as", "at", "be",
    "do", "go", "if", "in", "is", "it", "of", "on", "the", "to")

  var numReducers = reduceActors.size

  val helper = MapperHelper

  var actorReduceRouter:ActorSelection = Option.empty[ActorSelection].orNull

  def hashMapping: ConsistentHashMapping = {
    case ReduceNameTitlePair(name,title) => {
      //println(s"\n *** Using ${name} as the key")
      name
    }
    case FlushReduce(msg) =>
      {
        msg.hashCode
      }
  }

//  def createHashingPoolRoute() = {
//    var addresses = reduceActors.map(e => e.path.address).toList
//    actorReduceRouter = context.actorOf(RemoteRouterConfig(ConsistentHashingPool(numReducers,hashMapping = hashMapping), addresses).props(Props(classOf[ReduceActor])))
//
//  }

  def receive = {

    case Init_Map(title,url,reduceRouter) =>
    {
      //createHashingPoolRoute();
      //print(s"\n  ********* Map - Received init map from ${sender()} *********")

      println(s"\n @@@@ reduceRouter =${reduceRouter.path} ")
      var nameTitlePair = helper.extractProperName(title,helper.readFromFile(url))

      actorReduceRouter = context.actorSelection(reduceRouter.path)
      //print(s"\n  ********* MapActor ${self.path.name} ${nameTitlePair.size} - Ready to send data to Reduce *********")
      nameTitlePair.foreach((dataSet:(String,String))=>{

        actorReduceRouter ! ReduceNameTitlePair(dataSet._1,dataSet._2)

//        var index = getReducerIndex(dataSet._1)
//        reduceActors(index) ! ReduceNameTitlePair(dataSet._1,dataSet._2)
      })

    }
    case Flush =>

      //print(s"\n\n  *********************  ${self.path.name} received flush frmom ${sender().path.name} ********************* ")

      actorReduceRouter ! Broadcast(FlushReduce(sender.toString()))

//      for (i <- 0 until numReducers) {
//        reduceActors(i) ! Flush
//      }
    case msg:String =>
      print(s"Mapper received a message ${msg} from ${sender}")
    case _ => print("\n ^^^^^^^^^^^^^ UNKNOWN MESSAGE 2 ^^^^^^^^^^^^^^^^^^ ")
  }
  def getReducerIndex(word: String) = {

    Math.abs((word.hashCode)%numReducers)
  }


}
