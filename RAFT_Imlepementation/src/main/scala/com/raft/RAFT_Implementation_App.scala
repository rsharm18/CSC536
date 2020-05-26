package com.raft

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import com.raft.members.Raft_Participants

object RAFT_Implementation_App {

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
      .withFallback(ConfigFactory.load())

    val system = ActorSystem("RaftSystem", config)
    // Create an actor that handles cluster domain events
    system.actorOf(Props[Raft_Participants], name = "RaftParticipants")
  }



}


