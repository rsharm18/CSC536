package com.raft.util

import akka.actor.Actor

import scala.collection.mutable.ListBuffer


class LogRepository extends  Actor{

  var data:ListBuffer[LogEntry] = new ListBuffer[LogEntry]

  override def receive = {
    case ADD_Entries(newEntry) =>
      data +=newEntry
    case Get_Entries =>
      data

  }
}
