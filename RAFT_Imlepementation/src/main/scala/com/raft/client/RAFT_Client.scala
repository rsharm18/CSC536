package com.raft.client

import java.util.Date

import com.raft.util.{Command, READY_FOR_INPUT, RECEIVED_INPUT,INIT}
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, PoisonPill, Props, Terminated}
import com.typesafe.config.ConfigFactory



class Worker extends Actor{

  override def receive = {

    case READY_FOR_INPUT =>
      print(" \n \n Please enter Data/Comamnd. Enter SHUTDOWN to stop me \n ")
      var data = scala.io.StdIn.readLine()
      sender ! RECEIVED_INPUT(Command(data))
  }

}

class RAFT_Client(RAFT_participant_path:String) extends  Actor with ActorLogging{

  var clientWorker:ActorRef = Option.empty[ActorRef].orNull

  println(s" RAFT_participant_path ${RAFT_participant_path}")
  var clusterActor = context.actorSelection(RAFT_participant_path);

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
        clusterActor ! cmd

        println(s"Data Sent to  ${clusterActor}")

     }

    case "OK" =>
      {
        print(s" ${sender} senderpath=${sender.path} address = ${sender.path.address} Data sent to ${clusterActor} successfully! \n Asking worker to accept new input")

        clientWorker ! READY_FOR_INPUT

      }

    case "TRY AGAIN!" =>
      {
        println("\n !!!!!!!!!!!!!! OOPS SOMETHING WENT WRONG !!!!!!!!!!!!")
        clientWorker ! READY_FOR_INPUT
      }
  case Terminated => {
        println(s"\n\n *********** ${sender} worker terminated abruptly")
    init();
  }
    case INIT =>
      init()

  }


  def init() = {
    clientWorker = context.actorOf(Props[Worker], name = "myWorker")
    context.watch(clientWorker)
    clientWorker ! READY_FOR_INPUT
  }
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


