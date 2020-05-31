package com.raft.members

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Timers}
import com.raft.util.{ELECTION_TIMEOUT, INIT_TIMER, LEADER, SEND_HEARTBEAT, STATE, TIMER_UP}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration.Duration

class RAFT_Timer(parentState:STATE) extends Actor  with ActorLogging with Timers{

  val minValue = ConfigFactory.load("RAFT_CLUSTER").getInt("raft.timer.minValue")
  val maxValue = ConfigFactory.load("RAFT_CLUSTER").getInt("raft.timer.maxValue")
  val heartBeatTimeout = ConfigFactory.load("RAFT_CLUSTER").getInt("raft.timer.heartBeatTimeout")

  val generator = new scala.util.Random

  var timeOut:Int =  0

  var parent:ActorRef = Option.empty.orNull

  override def receive: Receive = {

    case INIT_TIMER =>

      parent = sender()
      startTimer();

    case TIMER_UP => {
      //println("\n reached Election timeout ")
      if(parentState == LEADER)
        parent ! SEND_HEARTBEAT
      else
        parent ! ELECTION_TIMEOUT(timeOut)
    }
  }

  override def preStart(): Unit = {
    super.preStart()
    //println("\n starting the timer actor ")
  }

  override def postStop(): Unit = {
    super.postStop()
    //println(" stopped the timer actor ")
  }
  def startTimer(): Unit ={


   timeOut =  if(parentState == LEADER) {( heartBeatTimeout + generator.nextInt(heartBeatTimeout))} else { (minValue + generator.nextInt((maxValue - minValue) + 1))}

   //println(s"\n Parent state is ${parentState} and Time out duration is ${timeOut} ==> ${Duration(timeOut,TimeUnit.MILLISECONDS)} ms")

    timers.startSingleTimer("Election_TimeOut",TIMER_UP,Duration(timeOut,TimeUnit.MILLISECONDS))
  }

}
