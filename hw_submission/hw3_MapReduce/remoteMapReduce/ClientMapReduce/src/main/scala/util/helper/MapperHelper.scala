package util.helper


import java.io.FileWriter

import akka.actor.ActorRef
import akka.routing.ConsistentHashingRouter.ConsistentHashMapping
import util.message.{FlushReduce, ReduceNameTitlePair}

import scala.collection.mutable
import scala.io.Source

object MapperHelper {


  private var mapActorRouter: ActorRef = Option.empty[ActorRef].orNull
  private var reduceActorRouter: ActorRef = Option.empty[ActorRef].orNull

  private var contextMasterActor: ActorRef = Option.empty[ActorRef].orNull

  def getMapActorRouter() = {
    mapActorRouter
  }


  def setMapActorRouter(actor: ActorRef) = {
    mapActorRouter = actor
  }

  def getReduceActorRouter() = {
    reduceActorRouter
  }

  def setReduceActorRouter(actor: ActorRef) = {
    reduceActorRouter = actor
  }

  def getContextMasterActor() = {
    contextMasterActor
  }

  def setContextMasterActor(actor: ActorRef) = {
    contextMasterActor = actor
  }

  def readFromFile(url: String): String = {

    //    val strBuilder =new  StringBuilder();
    //    try {
    //      Source.fromURL(url).getLines().foreach((f: String) => {
    //        strBuilder.append(f)
    //      }
    //      )
    //    }
    //    catch
    //      {
    //        case e:FileNotFoundException => e.printStackTrace()
    //        case e:IOException =>e.printStackTrace()
    //        case _:Throwable =>println("Unknown Error in reading file")
    //      }
    //    println(s"\n\n *** Finished Reading file from ${url} length ${strBuilder.size}")
    //
    //    strBuilder.toString()

    try {
      Source.fromURL(url).mkString
    }
    catch {
      case e: Exception => {
        println(s"Sorry, Could not read the file from ${url} due to below exception \n ${e.getMessage}")

        "" //return empty data
      }
    }


  }


  def extractProperName(title: String, content: String): mutable.HashMap[String, String] = {
    var nameTitlePair = new mutable.HashMap[String, String]()

    //split the word by space
    for (word <- content.split("\\s")) {
      //get Proper case - work starting with upper case and ends with lower case
      if (word.trim.matches("^[A-Z][a-z]+$")){
        //"\\b[A-Z][a-z]*\\b")) {
        nameTitlePair put(word.trim, title)
      }

    }

    nameTitlePair
  }


  //hash mapping for consistentHashRouting
  def hashMapping: ConsistentHashMapping = {
    case ReduceNameTitlePair(name, title) => {
      //println(s"\n *** Using ${name} as the key")
      name
    }
    case FlushReduce(msg) => {
      msg.hashCode
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
