package mapreduce

import akka.actor.Actor
import com.typesafe.config.ConfigFactory
import util.helper.MapperHelper
import util.message.{Done, Flush, ReduceNameTitlePair}

import scala.collection.mutable.HashMap

class ReduceActor extends Actor {
  var remainingMappers = ConfigFactory.load.getInt("number-mappers")
  
  var show_reduce_actor_output_inConsole = ConfigFactory.load.getBoolean("show_reduce_actor_output_inConsole")
  
  var reduceMap = HashMap[String, Int]()

  var reduceMapTitle = HashMap[String, List[String]]()


  def receive = {
    case ReduceNameTitlePair(name,title) =>
      {
        produceNameTitleData(name,title)
      }
    case Flush =>
      remainingMappers -= 1

      if (remainingMappers == 0) {
        var stringBuilder = new StringBuilder();

        var fileOutput = new StringBuilder();

        stringBuilder
          .append(s"\n *** ${self.path.toStringWithoutAddress} :: START ***\n")
          //.append(s"\n \t ${self.path.toStringWithoutAddress} :: Proper Name(Count) : ${reduceMap.toString()}")
          //.append(s"\n\n \t ${self.path.toStringWithoutAddress} :: Word(Title)        : ${reduceMapTitle.toString()}")
        reduceMapTitle.foreach((data:(String,List[String]))=>{

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
            iCount -=1

          })
          stringBuilder.append("]").append(") ")
          fileOutput.append("] } ").append(") \n ")
        })
        stringBuilder.append(s"\n\n **** ${self.path.toStringWithoutAddress} :: END ***\n")
        if(show_reduce_actor_output_inConsole)
        println(stringBuilder.toString)
        else
        {
          println(s"\n show_reduce_actor_output_inConsole=false. \n\n Please either refer to the ${self.path.name}.txt for my output or set show_reduce_actor_output_inConsole=true in conf file \n")
        }
	      MapperHelper.writeOutputToFile(self.path.name,fileOutput.toString())
	
        println(s"\n @@@@@ ${self.path.name} is finished processing. \n\n @@@@@@ Generated Output File ${self.path.name}.txt in the root folder ")
        context.parent ! Done
      }
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
