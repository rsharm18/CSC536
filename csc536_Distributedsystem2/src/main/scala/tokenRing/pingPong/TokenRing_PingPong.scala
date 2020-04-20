package tokenRing.pingPong;

import akka.actor.{Actor, ActorRef, ActorSystem, Props}

private case class Start(left: ActorRef, right: ActorRef)

private case class PING()
private case class PONG()
private case class INIT_TOKEN()

private class PingPongToken extends Actor {
  var numberOfCounter1 = 0;
  var numberOfCounter2 = 0;
  var myClockwiseNeighbour: ActorRef = null;
  var myAntiClockwiseNeighbour: ActorRef = null;

  def receive = {

    case PING =>

      var strSender = sender.path.toString.split("/")
      var senderLen = strSender.length

      if (!sender.equals(self)) {
        numberOfCounter1 += 1;
        println(s"PING ${(strSender(senderLen - 1))} -> ${self.path.name}  # of Pings at ${self.path.name}  ${numberOfCounter1}")
        Thread.sleep(1000)

      }
      //val next = context.actorSelection(myClockwiseNeighbour)
      myClockwiseNeighbour ! PING

    case PONG =>

      var strSender = sender.path.toString.split("/")
      var senderLen=strSender.length

      if(!sender.equals(self)) {
        numberOfCounter2 += 1;

        println(s"                        PONG ${(strSender(senderLen - 1))} -> ${self.path.name}  # of Pongs at ${self.path.name}  ${numberOfCounter2}")

        Thread.sleep(2000)
      }

      //val next = context.actorSelection(myAntiClockwiseNeighbour)

      myAntiClockwiseNeighbour ! PONG

    case Start(clockWiseActor,antiClockWiseActor) =>

      myClockwiseNeighbour = clockWiseActor;//temp(temp.length - 1)
      myAntiClockwiseNeighbour  = antiClockWiseActor;//temp(temp.length - 1)

      println(s"I am ${self.path.name} and myClockwiseNeighbour ${myClockwiseNeighbour} myAntiClockwiseNeighbour : ${myAntiClockwiseNeighbour}")

    case INIT_TOKEN =>

      numberOfCounter1 += 1;
      numberOfCounter2 += 1;

      println(s"'${self.path.name}' initiates the token with # of PING=${numberOfCounter1} and # of of PONG=${numberOfCounter2}")


      var rightNode = myClockwiseNeighbour
      rightNode ! PING


      var leftNode = myAntiClockwiseNeighbour
      leftNode ! PONG


  }
}

//ping flows clockwise and pong goes anticlockwise
object TokenRingPingPong {

  def initTokenRing() =  {
    val system = ActorSystem("PingPongToken")
    val actor1 = system.actorOf(Props[PingPongToken], name = "P")
    val actor2 = system.actorOf(Props[PingPongToken], name = "Q")
    val actor3 = system.actorOf(Props[PingPongToken], name = "R")

    println(s"\t	       ${actor1.path.name}")
    println("\t         /   \\ ")
    println(" 		    /     \\")
    println("		   /	    \\")
    println("		  /	         \\")
    println(s"	      ${actor3.path.name} - - - - - ${actor2.path.name}")

    actor1 ! Start(actor2, actor3)
    actor2 ! Start(actor3, actor1)
    actor3 ! Start(actor1, actor2)

    Thread.sleep(500)
    println("Server ready..Calling init Token")
    actor1 ! INIT_TOKEN


  }
}
