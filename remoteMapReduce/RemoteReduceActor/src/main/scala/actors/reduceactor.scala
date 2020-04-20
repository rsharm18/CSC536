package actors

import akka.actor.Actor
import com.typesafe.config.ConfigFactory
import util.message.{AddMapper, AddReduce, Done, Flush, ReduceNameTitlePair}

import scala.collection.mutable.HashMap

class ReduceActor extends Actor {
  var remainingMappers = ConfigFactory.load.getInt("number-mappers")
  var reduceMap = HashMap[String, Int]()

  var reduceMapTitle = HashMap[String, List[String]]()

  val master = context.actorSelection("akka://RemoteMapReduce@127.0.0.1:25535/user/masterActor")
  master ! "from reduce actor"

  master ! AddReduce(self) //notify the master actor about myself - the new reduce

  def receive = {
    case ReduceNameTitlePair(name,title) =>
      {
        produceNameTitleData(name,title)
      }
    case Flush =>
      remainingMappers -= 1

      if (remainingMappers == 0) {
        var stringBuilder = new StringBuilder();

        stringBuilder
          .append(s"\n *** ${self.path.toStringWithoutAddress} :: START ***\n")
          //.append(s"\n \t ${self.path.toStringWithoutAddress} :: Proper Name(Count) : ${reduceMap.toString()}")
          //.append(s"\n\n \t ${self.path.toStringWithoutAddress} :: Word(Title)        : ${reduceMapTitle.toString()}")
        reduceMapTitle.foreach((data:(String,List[String]))=>{

          var iCount = data._2.size
          stringBuilder.append("(").append(data._1).append(",[")
          data._2.foreach((title:String)=>{
           stringBuilder.append(title)
            if(iCount>1){
              stringBuilder.append(",")
            }
            iCount -=1

          })
          stringBuilder.append("]").append(")  ")
        })
        stringBuilder.append(s"\n\n **** ${self.path.toStringWithoutAddress} :: END ***\n")
        println(stringBuilder.toString)
        context.parent ! Done
      }

    case msg:String =>
      print(s"Reducer received a message ${msg} from ${sender}")
  }

  def produceNameTitleData(word:String,title:String) = {
    if (reduceMap.contains(word))
    {
      reduceMap += (word -> (reduceMap(word) + 1))
      reduceMapTitle put ( word,  title :: reduceMapTitle(word))
    }
    else {
      reduceMap += (word -> 1)
      reduceMapTitle.put( word,List(title))
    }
  }
}
