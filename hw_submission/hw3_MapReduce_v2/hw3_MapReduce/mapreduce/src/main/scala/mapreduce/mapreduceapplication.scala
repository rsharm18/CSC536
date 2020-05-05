package mapreduce

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import util.message.{Flush, Init_Map}

import scala.collection.mutable
import scala.io.Source

object MapReduceApplication extends App {

  val system = ActorSystem("MapReduceApp")
  val master = system.actorOf(Props[MasterActor], name = "master")
  val numberOFFiles=ConfigFactory.load().getInt("number-source-url")

  var strBuilder = new StringBuilder;

  
  for(i <- 1 to numberOFFiles){
    println("source-URL."+i)
    var entry = ConfigFactory.load().getString("source-URL."+i)
    var data = entry.split("\\|~\\|")
    println(data(0)+" <--> "+data(data.length - 1))
    master ! Init_Map(data(0),data(data.length - 1))
  }

  
  master ! Flush
}
