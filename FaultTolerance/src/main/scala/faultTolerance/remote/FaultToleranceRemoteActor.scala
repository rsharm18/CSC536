package remote;

import akka.actor.{Actor, ActorRef, ActorSystem, AllForOneStrategy, OneForOneStrategy, Props, Terminated}
import com.typesafe.config.ConfigFactory

class Worker extends Actor {

  def receive = {
    case "HI" =>
      println(s" ***** Received Message Hi from [${sender}] ")
      sender ! "HELLO"
    case msg: String =>
        println(s" ***** Received Message [$msg] from [${sender}] ")
        sender ! "Good to see you too!"
  }

}

object FaultToleranceRemoteActor extends App {
  println("Starting FaultToleranceRemoteActor")

  val system = ActorSystem("Supervisor", ConfigFactory.load.getConfig("remote"))
  println("Server ready")
  val master = system.actorOf(Props[Worker], name = "remoteActor")

  println(s" master : ${master}")
  
}
