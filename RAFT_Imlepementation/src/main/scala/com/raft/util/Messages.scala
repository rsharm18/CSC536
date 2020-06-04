package com.raft.util

import akka.actor.{ActorRef, Address}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

// case classes related to leader election - START
sealed trait RAFT_MESSAGES
sealed trait Timer

sealed trait STATE
case object FOLLOWER extends STATE
case object CANDIDATE extends STATE
case object LEADER extends STATE

sealed  trait CLIENT_MSG
case object READY_FOR_INPUT extends  CLIENT_MSG
case class RECEIVED_INPUT(command: Command) extends  CLIENT_MSG
case object INIT extends CLIENT_MSG


case class INIT_TIMER() extends Timer
case class END_TIMER() extends  Timer
case class ELECTION_TIMEOUT(timeOut:Int) extends  Timer
case object TIMER_UP extends  Timer
case class RECEIVED_HEARTBEAT(term:Int) extends Timer
case object SEND_HEARTBEAT extends Timer

//case classes related to log handling - Start

case class Command(data:String)

case class Command_Message (sender:ActorRef, clientCommand:Command)

sealed trait LOGMESSAGES
case class LogEntry(term:Int, currentIndex:Int = -1, command:Command,sender:ActorRef) extends LOGMESSAGES
case class Simplified_LogEntry(term:Int, index:Int = -1, data:Command) extends LOGMESSAGES //used for persistence
case class ADD_Entries(data:LogEntry) extends LOGMESSAGES
case class Get_Entries() extends LOGMESSAGES
case class RemoveEntry(index:Int)  extends LOGMESSAGES
case class LOAD_FROM_FILE(stateMachineName:String)  extends LOGMESSAGES

//class file used to refresh locallog from the commited entries
case class REFRESH_LOCAL_LOG(committedEntries:ListBuffer[LogEntry]) extends LOGMESSAGES

//case classes related to log handling - End

case class Voted(decision:Boolean) extends RAFT_MESSAGES
case class RequestVote(candidateId:String, term:Int, lastLogIndex:Int, lastLogTerm:Int) extends RAFT_MESSAGES

// local vote record for each participant
case class Participant_VoteRecord(candidateID:String,myDecision:Boolean) extends RAFT_MESSAGES

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
 * @param prevLogEntry
 * @param data
 * @param leaderCommitIndex
 */
case class APPEND_ENTRIES(term:Int,prevLogEntry:LogEntry
                          ,data:ListBuffer[LogEntry],
                          leaderCommitIndex:Int) extends RAFT_MESSAGES

case class RESULT_APPEND_ENTRIES(term:Int,decision:Boolean) extends  RAFT_MESSAGES
case class APPEND_ENTRIES_LOG_Inconsistency(term:Int,conflictIndex:Int,conflictTerm:Int,success:Boolean)

case class PersistState(candidateID:String,committedEntries:mutable.HashMap[Int,Simplified_LogEntry])
case class StateMachine_Update_Result(result:Boolean,committedEntries:mutable.HashMap[Int,Simplified_LogEntry])
case class PurgeInvalidEntries(candidateID:String,purgeIndex:Int)
case object COMMIT_STATUS
// case classes related to leader election - END