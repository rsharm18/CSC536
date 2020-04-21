package actors

import akka.actor.{Actor, ActorRef, ActorSelection}
import akka.routing.Broadcast
import util.helper.MapperHelper
import util.message.{Flush, FlushReduce, Init_Map, ReduceNameTitlePair}


class MapActor(reduceActors: List[ActorRef]) extends Actor {

  val STOP_WORDS_LIST = List("a", "am", "an", "and", "are", "as", "at", "be",
    "do", "go", "if", "in", "is", "it", "of", "on", "the", "to")
  val helper = MapperHelper
  var numReducers = reduceActors.size
  var actorReduceRouter: ActorSelection = Option.empty[ActorSelection].orNull

  //  def createHashingPoolRoute() = {
  //    var addresses = reduceActors.map(e => e.path.address).toList
  //    actorReduceRouter = context.actorOf(RemoteRouterConfig(ConsistentHashingPool(numReducers,hashMapping = hashMapping), addresses).props(Props(classOf[ReduceActor])))
  //
  //  }

  def receive = {

    case Init_Map(title, url) => {
      print(s"\n  ********* Map Actor- Received init map from ${sender()} *********")

      //println(s"\n @@@@ reduceRouter =${MapperHelper.getReduceActorRouter().path} ")
      var nameTitlePair = helper.extractProperName(title, helper.readFromFile(url))

      actorReduceRouter = context.actorSelection(helper.getReduceActorRouter().path)
      //print(s"\n  ********* MapActor ${self.path.name} ${nameTitlePair.size} - Ready to send data to Reduce *********")

      nameTitlePair.foreach((dataSet: (String, String)) => {
        //send task to reduce actor using the consistenthashrouting
        actorReduceRouter ! ReduceNameTitlePair(dataSet._1, dataSet._2)
      })

    }
    case Flush =>
      //print(s"\n\n  *********************  ${self.path.name} received flush frmom ${sender().path.name} ********************* ")
      //println(s"\n actorReduceRouter - ${actorReduceRouter} && ${helper.getReduceActorRouter()}")
      if(actorReduceRouter == null)
         actorReduceRouter = context.actorSelection(helper.getReduceActorRouter().path)

      actorReduceRouter ! Broadcast(FlushReduce(sender.toString()))
  }


}
