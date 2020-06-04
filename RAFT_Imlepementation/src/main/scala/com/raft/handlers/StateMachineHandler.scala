package com.raft.handlers

import java.io.{File, FileWriter}

import akka.actor.Actor
import com.google.gson.{Gson, JsonParser}
import com.raft.util.{COMMIT_STATUS, Get_Entries, LOAD_FROM_FILE, LogEntry, PersistState, PurgeInvalidEntries, Simplified_LogEntry, StateMachine_Update_Result}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class StateMachineHandler extends Actor {

  var commitStatus = false
  var data = new mutable.HashMap[Int,Simplified_LogEntry]();
  var result = mutable.HashMap[String,Object]();

  override def receive: Receive = {

    case COMMIT_STATUS =>
      //print(s"\n\n Sending commitStatus ${commitStatus} and data ${data} ")
      sender ! (StateMachine_Update_Result(commitStatus,data))

    case Get_Entries =>
      sender ! data

    case msg:LOAD_FROM_FILE =>
      readFromInputFile(msg.stateMachineName)

    case state:PersistState =>
      commitStatus = false
      println(s"\n\n ================> Trying to persiste Data ${state.committedEntries.size} entries= ${state.committedEntries}")
      commitDataToFile(state.candidateID,state.committedEntries)
      readFromInputFile(state.candidateID)

    case purge:PurgeInvalidEntries =>

      println(s" \n\n *** Delete Logs....** purge index: ${purge.purgeIndex} - data = $data \n\n ")
      var reWriteTheLog = false
      var iDeleteStartIndex = purge.purgeIndex;
      var dataSize = data.size
      while(iDeleteStartIndex<=dataSize) {
        if(!reWriteTheLog)
          reWriteTheLog = true

        data.remove(iDeleteStartIndex)
        iDeleteStartIndex +=1
      }

      if(reWriteTheLog){
        commitDataToFile(purge.candidateID,committedEntries = data )
      }

  }

  def readFromInputFile(filename:String) = {
    var file = new File(s"${filename}.json")
    import com.google.gson.Gson
    val gson = new Gson

    var buffer= new mutable.HashMap[Int,Simplified_LogEntry]();

    println(s"\n\n File $file exists? ${file.exists()}")

    if(file.exists())
    {

      for (line <- io.Source.fromFile(s"${filename}.json").getLines) {

        println(s"\n line = ${line}")
        var jsonStringAsObject= new JsonParser().parse(line).getAsJsonObject
        //println(s"jsonStringAsObject ${jsonStringAsObject}")

        var logEntry:Simplified_LogEntry = gson.fromJson(jsonStringAsObject, classOf[Simplified_LogEntry])

        buffer +=((logEntry.index,logEntry))

      }

      //println(s" buffer = ${buffer}")
      data = buffer

    }



  }
  // write the data to file
  def commitDataToFile( stateMachineName:String, committedEntries:mutable.HashMap[Int,Simplified_LogEntry]): Boolean =
  {
    var fw:FileWriter = null
    var success = false

    try {
      val gson = new Gson()
      val jsonString = new StringBuilder();
      println(s"Writing ${committedEntries.size} to file")
      committedEntries.foreach(data=>{
        jsonString.append(gson.toJson(data._2)).append("\n")
      })

      println(s" writing jsonString= $jsonString")
      fw = new FileWriter(s"${stateMachineName}.json", false)
      fw.write(jsonString.toString())
      commitStatus = true
    }
    catch
      {
        case e:Exception => {
          print(s"Error creating/writing to the file $stateMachineName.json. Error Message = ${e.getMessage}")
          success = false
        }
      }finally {
      if(fw!=null)
        fw.close()
    }
    success
  }
}
