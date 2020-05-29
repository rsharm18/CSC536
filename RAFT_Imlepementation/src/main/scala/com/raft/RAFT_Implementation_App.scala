package com.raft

import java.util.Date

import akka.actor.{ActorSystem, Props}

import com.typesafe.config.ConfigFactory
import com.raft.members.Raft_Participants

import scala.concurrent.{ExecutionContext, Future}

object RAFT_Implementation_App  {

  def main(args: Array[String]): Unit = {
    val ports =
      if (args.isEmpty)
        Seq(25251, 25252, 0)
      else
        args.toSeq.map(_.toInt)
    ports.foreach(startUpRaftParticipants)
  }

  def startUpRaftParticipants(port: Int): Unit = {
    // Override the configuration of the port
    val config = ConfigFactory.parseString(s"""
      akka.remote.artery.canonical.port=$port
      """)
      .withFallback(ConfigFactory.parseString("akka.cluster.roles = [Participant]"))
      .withFallback(ConfigFactory.load("RAFT_CLUSTER"))

    val system = ActorSystem("RaftSystem", config)

    print(s"\n\n port ${port} (port==0)? ${(port==0)}\n\n" )

    val actorName = if(port==0) s"Node_${port}_${new Date().getTime}" else s"RAFT_SEED_${port}";

    // Create an actor that handles cluster domain events
    var newActor = system.actorOf(Props(classOf[Raft_Participants],actorName), name = s"${actorName}")



    //new RAFT_Implementation_App(system).run()
  }



}
