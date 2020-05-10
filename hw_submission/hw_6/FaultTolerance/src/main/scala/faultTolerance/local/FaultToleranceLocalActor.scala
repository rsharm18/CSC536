package local

import akka.actor.{Actor, ActorRef, ActorSystem, OneForOneStrategy, Props, Terminated}
import akka.actor.SupervisorStrategy.{Restart, Resume, Stop}
import com.typesafe.config.ConfigFactory

class ClientSupervisor extends Actor {

  import context._

  val remoteClient = actorSelection("akka://Supervisor@127.0.0.1:25535/user/remoteActor")
  remoteClient ! "HI"



  def receive = {
    case "HELLO" =>
      context.watch(sender)
     
	 //context.become(maybeActive(sender))

      println(s"\n\n Received HELLO from ${sender}\n\n")

    case "finished"   => println("\n *********** done *********** \n ")

    case Terminated(actorRef) =>
      println(s"\n\n *********** Actor ${actorRef} terminated. \n ***removing it from watchlist ***********\n\n")
      self ! "finished"
      context.unwatch(actorRef)

  }

//  def maybeActive(actor: ActorRef): Receive = {
//    case Terminated(actorRef) =>
//      println(s"\n\n *********** Actor ${actorRef} terminated. \n ***removing it from watchlist ***********\n\n")
//      self ! "finished"
//      context.unwatch(actorRef)
//
//  }
}

object FaultToleranceLocalActor extends App {
  val system = ActorSystem("FaultToleranceSample",ConfigFactory.load.getConfig("local"))
  val supervisor = system.actorOf(Props[ClientSupervisor],"local")
  //supervisor! "HELLO"
  println("Remote client is ready")

}
