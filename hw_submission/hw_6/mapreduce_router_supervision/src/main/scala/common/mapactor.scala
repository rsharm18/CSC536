package common

import scala.collection.mutable.HashSet

import scala.io.Source

import akka.actor.{Actor, ActorRef}
import akka.routing.Broadcast

class MapActor(reduceActors: ActorRef) extends Actor {

  println(s"******** Started MapActor ${self.path}")

  Thread sleep 2000

  val STOP_WORDS_LIST = List("a", "am", "an", "and", "are", "as", "at", "be",
    "do", "go", "if", "in", "is", "it", "of", "on", "the", "to")

  def receive = {
    case Book(title, url) =>
      process(title, url)
    case Flush => 
      reduceActors ! Broadcast(Flush)
  }

  // Process book
  def process(title: String, url: String) = {
    //println(s"\n\n *** url : ${url} *** \n\n")
    try{
      val content = getContent(url)
      var namesFound = HashSet[String]()
      for (word <- content.split("[\\p{Punct}\\s]+")) {
        if ((!STOP_WORDS_LIST.contains(word)) && word(0).isUpper && !namesFound.contains(word)) {
          reduceActors ! Word(word, title)
          namesFound += word
        }
      }
    }
    catch {
      case e:Exception => {
        println(s"\n\n **** Map Actor ${self.path.name} encountered error. Throwing it so that router can catch")
        throw e;
      }
    }

  }

  // Get the content at the given URL and return it as a string
  def getContent( url: String ) = {
      Source.fromURL(url).mkString

  }
}
