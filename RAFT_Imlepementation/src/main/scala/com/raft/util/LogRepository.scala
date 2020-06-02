package com.raft.util

import java.io.File

import akka.actor.Actor
import com.google.gson.JsonParser

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class LogRepository extends  Actor{

  var data:ListBuffer[LogEntry] = ListBuffer[LogEntry]()

  override def receive = {
    case ADD_Entries(newEntry:LogEntry) =>

      var currentIndex = if(newEntry.currentIndex == -1){data.length +1} else {newEntry.currentIndex}
      var newLE:LogEntry = LogEntry(newEntry.term,currentIndex,newEntry.command)

      data +=newLE
    case Get_Entries =>
      //println(s" data ${data}")
      sender !(data)

    case RemoveEntry(index) =>
      {
        data.remove(index - 1 )

        sender ! data
      }

    case refresh:REFRESH_LOCAL_LOG =>
      data = refresh.committedEntries

    case msg:LOAD_FROM_FILE =>
      {
        readFromInputFile(msg.stateMachineName)
      }
  }

  def readFromInputFile(filename:String) = {
    var file = new File(filename)
    import com.google.gson.Gson
    val gson = new Gson

    var buffer= ListBuffer[LogEntry]();

    //println(s"\n\n File $file exists? ${file.exists()}")

    if(file.exists())
      {
        for (line <- io.Source.fromFile(filename).getLines) {

          //println(s"\n line = ${line}")
          var jsonStringAsObject= new JsonParser().parse(line).getAsJsonObject
          var logEntry:LogEntry = gson.fromJson(jsonStringAsObject, classOf[LogEntry])

          buffer.addOne(logEntry)

        }

        //println(s" buffer = ${buffer}")
        data = buffer

      }



  }
}
