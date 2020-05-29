package com.raft.util

import akka.actor.Actor

import scala.collection.mutable.ListBuffer


class LogRepository extends  Actor{

  var data:ListBuffer[LogEntry] = new ListBuffer[LogEntry]

  override def receive = {
    case ADD_Entries(newEntry:LogEntry) =>

      var currentIndex = if(newEntry.currentIndex == -1){data.length +1} else {newEntry.currentIndex}
      var newLE:LogEntry = LogEntry(newEntry.termId,currentIndex,newEntry.data)

      data +=newLE
    case Get_Entries =>
      sender ! data

    case RemoveEntry(index) =>
      {
        data.remove(index - 1 )

        sender ! data
      }
  }
}
