package actors

import akka.actor.Actor
import util.helper.MapperHelper
import util.message.{Done, FlushReduce, ReduceNameTitlePair}

import scala.collection.mutable

class ReduceActor() extends Actor {
  var remainingMappers = 4 //ConfigFactory.load.getInt("number-mappers")
  var reduceMap = mutable.HashMap[String, Int]()

  var reduceMapTitle = mutable.HashMap[String, List[String]]()


  def receive = {
    case ReduceNameTitlePair(name, title) => {
      produceNameTitleData(name, title)
    }
    case FlushReduce(msg) =>

      remainingMappers -= 1
      //println(s"\n\n ~~~~~~~~  Sender - ${sender.path}  -- self = ${self.path} - remainingMappers? ${remainingMappers} received flush ")
      if (remainingMappers == 0) {
        var stringBuilder = new StringBuilder();

        var fileOutput = new StringBuilder();

        stringBuilder
          .append(s"\n *** ${self.path.toStringWithoutAddress} :: START ***\n")
        //.append(s"\n \t ${self.path.toStringWithoutAddress} :: Proper Name(Count) : ${reduceMap.toString()}")
        //.append(s"\n\n \t ${self.path.toStringWithoutAddress} :: Word(Title)        : ${reduceMapTitle.toString()}")
        reduceMapTitle.foreach((data: (String, List[String])) => {

          var iCount = data._2.size
          stringBuilder.append("(").append(data._1).append("Titles:[")
          fileOutput.append("(").append("name: "+data._1).append(s", { Title_Count: ${iCount}, Titles:[")

          data._2.foreach((title: String) => {
            stringBuilder.append(title)
            fileOutput.append(title)
            if (iCount > 1) {
              stringBuilder.append(" , ")
              fileOutput.append(" , ")
            }
            iCount -= 1

          })
          stringBuilder.append("]").append(") ")
          fileOutput.append("] } ").append(") \n ")
        })
        stringBuilder.append(s"\n\n **** ${self.path.toStringWithoutAddress} :: END ***\n")
        println(stringBuilder.toString)

        MapperHelper.writeOutputToFile("Reduce_Actor_"+self.path.name,fileOutput.toString())

        println(s"\n @@@@@ ${self.path.name} is finished processing. \n\n @@@@@@ Generated Output File ${self.path.name}.txt in the root folder ")
        MapperHelper.getContextMasterActor() ! Done
      }
  }


  def produceNameTitleData(word: String, title: String) = {
    if (reduceMap.contains(word)) {
      reduceMap += (word -> (reduceMap(word) + 1))
      reduceMapTitle put(word, title :: reduceMapTitle(word))
    }
    else {
      reduceMap += (word -> 1)
      reduceMapTitle.put(word, List(title))
    }
  }
}
