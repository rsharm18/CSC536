package com.raft.util

import akka.actor.Address

import scala.collection.mutable.ListBuffer

// case classes related to leader election - START
sealed trait RAFT_MESSAGES
sealed trait Timer

sealed trait STATE
case object FOLLOWER extends STATE
case object CANDIDATE extends STATE
case object LEADER extends STATE



case class INIT_TIMER() extends Timer
case class END_TIMER() extends  Timer
case class ELECTION_TIMEOUT(timeOut:Int) extends  Timer
case object TIMER_UP extends  Timer
case class RECEIVED_HEARTBEAT(term:Int) extends Timer
case object SEND_HEARTBEAT extends Timer

//case classes related to log handling - Start
case class Command(data:String)
case class LogEntry(termId:Int, data:Command)
case class ADD_Entries(data:LogEntry)
case class Get_Entries()
//case classes related to log handling - End




case class Voted(decision:Boolean) extends RAFT_MESSAGES
case class RequestVote(candidateId:Address, term:Int, lastLogIndex:Int, lastLogTerm:Int) extends RAFT_MESSAGES

// local vote record for each participant
case class Participant_VoteRecord(candidateID:Address,myDecision:Boolean) extends RAFT_MESSAGES

// not sure on below
case class RegisterVote(term:Int,vote:Voted) extends RAFT_MESSAGES

/**
 * Arguments:
 * term leader’s term
 * leaderId so follower can redirect clients
 * prevLogIndex index of log entry immediately preceding new ones
 * prevLogTerm term of prevLogIndex entry
 * entries[] log entries to store (empty for heartbeat * may send more than one for efficiency)
 * leaderCommit leader’s commitIndex
 *
 * @param term -
 * @param leaderID
 * @param prevLogIndex
 * @param prevLogTerm
 * @param data
 * @param leaderCommitIndex
 */
case class APPEND_ENTRIES(term:Int,leaderID:Int,prevLogIndex:Int,prevLogTerm:Int,data:ListBuffer[LogEntry], leaderCommitIndex:Int) extends RAFT_MESSAGES


// case classes related to leader election - END