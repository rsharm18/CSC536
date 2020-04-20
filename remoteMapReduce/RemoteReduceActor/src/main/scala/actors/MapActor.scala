package actors

import akka.actor.{Actor, ActorRef}
import util.helper.MapperHelper
import util.message.{AddReducers, Flush, Init_Map, ReduceNameTitlePair}


class MapActor extends Actor {

  val STOP_WORDS_LIST = List("a", "am", "an", "and", "are", "as", "at", "be",
    "do", "go", "if", "in", "is", "it", "of", "on", "the", "to")

  var reduceActors:List[ActorRef] = Option.empty[List[ActorRef]].orNull

  var numReducers = 0


  val helper = MapperHelper

  def receive = {
    case AddReducers(reducers) =>
      {

        reducers.foreach((reducer:ActorRef)=>{
          reduceActors = reducer :: reduceActors
        })

        numReducers = reduceActors.size
      }
    case Init_Map(title,url) =>
      {
        var nameTitlePair = helper.extractProperName(title,helper.readFromFile(url))
        nameTitlePair.foreach((dataSet:(String,String))=>{
          var index = getReducerIndex(dataSet._1)
          reduceActors(index) ! ReduceNameTitlePair(dataSet._1,dataSet._2)
        })
     }
    case Flush =>
      for (i <- 0 until numReducers) {
        reduceActors(i) ! Flush
      }
    case msg:String =>
      print(s"Mapper received a message ${msg} from ${sender}")

  }
  def getReducerIndex(word: String) = {

    Math.abs((word.hashCode)%numReducers)
  }


}
