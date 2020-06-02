package com.raft.client

import java.util.concurrent.TimeUnit
import java.util.{Calendar, Date}

import com.raft.util.{Command, INIT, READY_FOR_INPUT, RECEIVED_INPUT}
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, PoisonPill, Props, ReceiveTimeout, Terminated}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration.Duration



class Worker extends Actor{

  override def receive = {

    case READY_FOR_INPUT =>
      print(" \n \n Please enter Data/Comamnd. Enter SHUTDOWN to stop me \n ")
      var data = scala.io.StdIn.readLine()
      sender ! RECEIVED_INPUT(Command(data))
  }

}

class RAFT_Client(RAFT_participant_path:String) extends  Actor with ActorLogging{



//  clusterActor ! "Hello"
//
//  val master = context.actorSelection("akka://RaftClient@127.0.0.1:25530/user/Client")
//  master ! "Hello"

  override def receive = {

    case RECEIVED_INPUT(cmd: Command) =>
      if (cmd.data.equalsIgnoreCase("SHUTDOWN")) {
        print("\n about to shutdown myself...")
        print("Bye Bye!.. you wish!")

        Thread.sleep(200)
        //clientWorker ! READY_FOR_INPUT
        //context.unwatch(clientWorker)
        //clientWorker ! PoisonPill
        context.system.terminate
      } else {

        Thread.sleep(1000)

        val msgID = ClientID+"-"+Calendar.getInstance().getTimeInMillis()

        context.setReceiveTimeout(Duration.create(30*(if(iCount==0)  100 else 1),TimeUnit.MILLISECONDS))
        if(iCount == 0)
          iCount = 10

        clusterActor ! cmd; //ClientCommand(msgID,cmd)

        println(s"Data Sent to  ${clusterActor}")

     }
    case msg: ReceiveTimeout =>
      {
        println("Reached timeout")

        initInput(true)
      }

    case "OK" =>
      {
        print(s" ${sender} senderpath=${sender.path} address = ${sender.path.address} Data sent to ${clusterActor} successfully! \n Asking worker to accept new input")

        initInput()


      }

    case "TRY AGAIN!" =>
      {
        println("\n !!!!!!!!!!!!!! OOPS SOMETHING WENT WRONG !!!!!!!!!!!!")
        initInput(true)
      }
  case Terminated => {
        println(s"\n\n *********** ${sender} worker terminated abruptly")
    init();
  }
    case INIT =>
      init()

  }

  def initInput(askWorker:Boolean= false)={
    if(pickRandomInput)
      self ! RECEIVED_INPUT(Command(randomInputs(generator.nextInt(randomInputs.length -1))))
    else
      {
        clientWorker ! READY_FOR_INPUT
      }
  }

  def init() = {
    clientWorker = context.actorOf(Props[Worker], name = "myWorker")
    context.watch(clientWorker)
    clientWorker ! READY_FOR_INPUT
  }

  val ClientID="C1"
  val pickRandomInput = true
  var randomInputs = List("HI","HOW ARE YOU?","X=4","DO THIS","I LOVE CSC 536","RAFT IS AWESOME","OMG!","2020","KEEP SMILING","YOU're awesome","YO","ROCKSTAR")

  var clientWorker:ActorRef = Option.empty[ActorRef].orNull

  var iCount=10;

  println(s" RAFT_participant_path ${RAFT_participant_path}")
  var clusterActor = context.actorSelection(RAFT_participant_path);
  val generator = new scala.util.Random
}
object RAFT_Client {

  def main(args: Array[String]): Unit = {

    var actorPath:String=
      if (args.isEmpty || args.length >1)
        "akka://RaftSystem@127.0.0.1:25251/user/RAFT_SEED_25251"
        //"akka://RaftSystem@127.0.0.1:25251/user/RAFT_SEED_25251"
        //"akka://RaftSystem@127.0.0.1:25251/user/RAFT_SEED_25251
       // "akka://RaftSystem@127.0.0.1:25252/user/RAFT_SEED"
      else
        args(0)

    startUpRaftClient(actorPath)
  }

  def startUpRaftClient(RAFT_participant_path:String): Unit = {

    var port=0

    val config = ConfigFactory.parseString(s"""
      akka.remote.artery.canonical.port=$port
      """)
      .withFallback(ConfigFactory.parseString("akka.cluster.roles = [client]"))
      .withFallback(ConfigFactory.load("RAFT_CLIENT"))

    //akka://RaftSystem@127.0.0.1:25251
    val system = ActorSystem("RaftClient",ConfigFactory.load("RAFT_CLIENT"))




    val client = system.actorOf(Props(classOf[RAFT_Client],RAFT_participant_path), name = "Client")
    println(s"I am ${client.path}")
    client ! INIT
  }



}


