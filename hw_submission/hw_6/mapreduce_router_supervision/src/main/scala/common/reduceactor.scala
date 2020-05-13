package common

import scala.collection.mutable.HashMap

import akka.actor.Actor
import com.typesafe.config.ConfigFactory
import java.io.FileWriter

class ReduceActor extends Actor {

  var printOutput = 
  println(self.path)

  Thread sleep 2000

  var remainingMappers = ConfigFactory.load.getInt("number-mappers")
  var reduceMap = HashMap[String, List[String]]()

  def receive = {
    case Word(word, title) =>
      if (reduceMap.contains(word)) {
        if (!reduceMap(word).contains(title))
	  reduceMap += (word -> (title :: reduceMap(word)))
      }
      else
		reduceMap += (word -> List(title))
		case Flush =>
		  remainingMappers -= 1
		  if (remainingMappers == 0) {
		  
		  
			var fileOutput = new StringBuilder();
			var iCount = 0;
      
			reduceMap.foreach((data: (String, List[String])) => {

			 fileOutput.append("(").append("name: "+data._1).append(s", { Title_Count: ${iCount}, Titles:[")

			  data._2.foreach((title: String) => {
				fileOutput.append(title)
				if (iCount > 1) {
				  fileOutput.append(" , ")
				}
				iCount -= 1

			  })
			  fileOutput.append("] } ").append(") \n ")
			})
			fileOutput.append(s"\n\n **** ${self.path.toStringWithoutAddress} :: END ***\n")
		
	  
			writeOutputToFile(self.path.name , fileOutput.toString)
			println(s"\n ============= The output will be printed in ${self.path.toStringWithoutAddress}.txt ============= ")
			context.actorSelection("../..") ! Done
	//        context stop self
      }
  }
  
  
  // write the data to file
  def writeOutputToFile(actorName:String,data:String): Unit =
  {
	try {
	  var fw = new FileWriter(s"${actorName}.txt", false);
	  fw.write(data);
	}
    catch
      {
        case e:Exception => {
          print(s"Error creating/writing to the file ${actorName}.txt. Error Message = ${e.getMessage}")
        }
      }

  }
  
}
