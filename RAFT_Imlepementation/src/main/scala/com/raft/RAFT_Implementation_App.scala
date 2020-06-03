package com.raft

import java.io.FileWriter
import java.util.Date

import akka.actor.{ActorSystem, Props}
import com.google.gson.{Gson, JsonParser}
import com.raft.members.Raft_Participants
import com.raft.util.LogEntry
import com.typesafe.config.ConfigFactory

import scala.collection.immutable.ListSet

object RAFT_Implementation_App  {
def getJSONObject() = {
  val gson = new Gson
  val data = "{\"term\":2,\"currentIndex\":0,\"command\":{\"data\":\"START\"}}"

  val jsonStringAsObject= new JsonParser().parse(data).getAsJsonObject
  val myObj:LogEntry = gson.fromJson(jsonStringAsObject, classOf[LogEntry])

  println(s" data=$data object= ${myObj}")


}

  def test() = {
    val gson = new Gson

    for (line <- io.Source.fromFile("RAFT_SEED_25252.json").getLines) {

      println(s"\n line = ${line}")
      var jsonStringAsObject = new JsonParser().parse(line).getAsJsonObject
      println(s"jsonStringAsObject ${jsonStringAsObject}")

      var logEntry: LogEntry = gson.fromJson(jsonStringAsObject, classOf[LogEntry])

      println(s" logEntry =${logEntry}")

    }
  }
  def writeToFile()= {
    var fw:FileWriter = null

    try {
      fw = new FileWriter(s"a.txt", false)
      fw.write("abc" + "\n")
    } catch {
      case e: Exception => print("Error")
    } finally {
      if (fw != null)
        fw.close()
    }
  }
  def main(args: Array[String]): Unit = {
    val ports =
      if (args.isEmpty)
        Seq(25251, 25252, 0)
      else
        args.toSeq.map(_.toInt)
    ports.foreach(startUpRaftParticipants)
    //getJSONObject
    //writeToFile
    //test
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

