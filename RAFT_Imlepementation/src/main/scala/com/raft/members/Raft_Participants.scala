package com.raft.members

import java.io.FileWriter
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorInitializationException, ActorLogging, ActorRef, Address, DeathPactException, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated, Timers}
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberEvent, MemberRemoved, MemberUp}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe}
import akka.cluster.{Cluster, MemberStatus}
import akka.pattern.ask
import akka.util.Timeout
import com.google.gson.Gson
import com.raft.handlers.{RAFT_TimerHandler, StateMachineHandler}
import com.raft.util._
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.collection.mutable._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class Raft_Participants(candidateID:String) extends Actor with ActorLogging with Timers{


  override def preStart(): Unit = {
    super.preStart()
    //subscribe to cluster member events
    cluster.subscribe(self,classOf[MemberEvent])
  }

  override def postStop(): Unit = {
    super.postStop()
    cluster unsubscribe self
  }


  override def receive: Receive = {

    // initialize the timer
    case INIT_TIMER =>
      if(!IS_TIMER_IN_PROGRESS){


        IS_TIMER_IN_PROGRESS = true

      //println("\n Received request to start the election timer")
      resetTimer()
      }
      else
        {
          logger("\n election timeout is already in progress")
        }

    case ELECTION_TIMEOUT(timeOut) =>

      celebrated_leadership = false


      if (currentLeader == self && myState == LEADER) {
        sendAliveMessage()
      }
      else {
        println(s"\n My State is $myState and The leader : $currentLeader is probably down and I reached election timeout $timeOut for term $currentTerm. Initiating election")

        votedNodes = mutable.Set.empty[ActorRef]
        myState = CANDIDATE //change myself to candidate
        currentTerm += 1 //increase the term#
        votedNodes += self //add my vote

        //myStateMachineHandler ! REFRESH_LOCAL_LOG(commited_Entries)

        myVoteRegistry.+=((currentTerm, Participant_VoteRecord(candidateID, myDecision = true)))


        myLastLogIndex = commited_Entries.size

        println(s"Initiating election for term ${currentTerm}")
        mediator ! Publish(topic, RequestVote(candidateID, currentTerm, if(lastCommitedEntry!=null)lastCommitedEntry.currentIndex else myLastLogIndex, if(lastCommitedEntry!=null)lastCommitedEntry.term else myLastLogTerm))

        resetTimer()
      }

    case RequestVote(candidateId, term, lastLogIndex, lastLogTerm) =>

      //readFromStateMachine()


      if(sender!=self) {
        decideAndVote(sender, candidateId, term, lastLogIndex, lastLogTerm)
      }

   //received append Entries from leader
    case leaderEntry : APPEND_ENTRIES =>

      resetTimer()

      if (sender() != self){
        logger(s" *** $self Received append entry from leader $currentLeader - Entry => $leaderEntry")

        val appendEntryResult = handleAppendEntryRequest(leaderEntry)

        //print(" Ready with the append_entry_result "+appendEntryResult)

        sender ! appendEntryResult
      }


    //received response from the followers
    case result:RESULT_APPEND_ENTRIES =>

      respondedNodes +=sender()

      if (result.decision && myState!=FOLLOWER)
        {
          votedNodes += sender

          //get quorum of maximum votes
          if (votedNodes.size >= (1 + (activeNodes.size) / 2)) {

            currentLeader = self
            if(myState == CANDIDATE) {
              myState = LEADER

              //initialize the indexes
              initIndexes()

              val leaderElectionSet = mutable.HashSet[String]()
              if (leaderElectionSet.add(s"${self.path.name} is the new leader for term=$currentTerm \t votedNodes $votedNodes \t activeNodes $activeNodes  \n ")) {
                writeOutputToFile(self.toString(), s"${self.path.name} is the new leader for term=$currentTerm \t votedNodes $votedNodes}\t activeNodes $activeNodes  \n ")

                println(s"\n\n \t ********** I am the leader  for term ${currentTerm}!!  ********** \n\n")
                logger(s"\n\n votedNodes ${votedNodes} \n activeNodes ${activeNodes} \n\n")
              }
            }


          //if received the consensus and if there is item in the processingQueue then add it to the state machine
          if(processingCommand.size>0){

            val data:Command_Message = processingCommand(0)

            addToCommitEntryQueue(data.clientCommand)
            myStateMachineHandler ! PersistState(candidateID, commited_Entries)
            implicit val timeout = Timeout(4 seconds)
            val future2 = ask(myStateMachineHandler, COMMIT_STATUS).mapTo[StateMachine_Update_Result]
            val commitResult = Await.result(future2, timeout.duration)
            val commitStatus:Boolean = commitResult.result
            commited_Entries = commitResult.committedEntries
            //add the command to the state machine and respond to the client/sender
            if(commitStatus)
              {

                val persistEntry = LogEntry(currentTerm,-1,data.clientCommand)
                val msg = CommitEntry(persistEntry,commitIndex)

                //mediator ! Publish(data.clientCommand.toString,msg)

                votedNodes.foreach((actor:ActorRef)=>{
                  actor ! msg
                })

                data.sender ! "OK"
                resetTimer()

              }
            else
              {
                data.sender ! "TRY AGAIN!"
              }

            processingCommand.remove(0)

          }

          //timers.startSingleTimer("Heartbeat",TIMER_UP,Duration(induceSleepValue,TimeUnit.MILLISECONDS))
          self ! SEND_HEARTBEAT

      }
      }else
        {
          if(result.term>currentTerm){
            myState = FOLLOWER
            initIndexes()
            currentTerm = result.term
          }
          else if (respondedNodes.size >=((activeNodes.size) / 2))
          {
            myState = FOLLOWER
            initIndexes()
            resetTimer()
          }
        }

    case commitEntry:CommitEntry =>
      println(s" Received the commitEntry ${commitEntry}")

      //mediator ! Unsubscribe(commitEntry.logEntry.data.toString,self)

      if(sender() != self) {
        val entry = commitEntry.logEntry

        //if(currentTerm<entry.term)
        currentTerm = entry.term
        currentLeader = sender()

        resetTimer()
        addToCommitEntryQueue(commitEntry.logEntry.command)
        myStateMachineHandler ! PersistState(candidateID, commited_Entries)

        sender ! RESULT_APPEND_ENTRIES(currentTerm, true)
      }

    case cmd:Command =>
    if (myState == LEADER)
    {
      println(s" \n\n &&&&&&&&&&&&&&&& Received Data ${cmd} from sender ${sender()} activeNodes.size= ${activeNodes.size} myState=${myState}")

      //addToLocalLog(cmd)

      val cmdEntry = Command_Message(sender(),cmd)
      pendingCommands += cmdEntry
      sendAliveMessage()
    }
    else
    {
      print(s" \n ~~~~~~~~~~~~~~ Forwarding the command to leader ${currentLeader}")
      if(currentLeader==null || (currentLeader==self && myState!=LEADER)){
        println("\n The leader is not avaialable. Please try again later! \n")
        sender() ! "TRY AGAIN!"
      }
      else {
        currentLeader.forward(cmd)
      }
    }
    case logInconsistency : APPEND_ENTRIES_LOG_Inconsistency =>

      if(myState == LEADER) {

        //read the committed entries
        val logData = commited_Entries

        val dataSet = ListBuffer[LogEntry]()

        logger(s"\n - Sender ${sender()} logInconsistency ${logInconsistency}",true)
        nextIndex += ((sender.path.address, if(logInconsistency.conflictIndex<=0) 1 else { logInconsistency.conflictIndex + (if(logInconsistency.conflictIndex == logData.size) 0 else  1)}))



        var iCount = nextIndex(sender.path.address)
        while (iCount > -1 && iCount <= logData.size) {
          dataSet += (logData(iCount))
          iCount += 1
        }

        var previousEntry = if (logData.size > 1) logData(logData.size) else emptyLogEntry

        println(s"\n APPEND_ENTRIES_LOG_Inconsistency => logData : ${logData} ${logInconsistency.conflictIndex}")
        if(logInconsistency.conflictIndex > 1){
          previousEntry = logData(logInconsistency.conflictIndex)
        }
        val msg = APPEND_ENTRIES(currentTerm, previousEntry, dataSet, commitIndex)
        println(s" Data=${dataSet.size} ==> Sending msg=APPEND_ENTRIES in response to APPEND_ENTRIES_LOG_Inconsistency to ${sender}",true)


        sender ! msg
      }

    case "Heartbeat" =>
      self ! SEND_HEARTBEAT

    case TIMER_UP =>
      //println("Resuming Again!!")
      resetTimer()
    // SEND_HEARTBEAT is triggered by RAFT_TimerHandler
    case SEND_HEARTBEAT =>
      logger( " SEND_HEARTBEAT : ${getLogEntryData(print = true)}")

      sendAliveMessage
    case MemberUp(member) =>
      activeNodes +=member.address

      println(s"\n ${member} joined with role ${member.roles}. ${activeNodes.size} is the cluster size")

      if(activeNodes.size>2){
        println("\n starting the election timer")
        self !INIT_TIMER
      }

    case MemberRemoved(member,_) =>
      activeNodes -= member.address

      println(s"\n ${member} left with role ${member.getRoles}. ${activeNodes.size} is the cluster size")

    case state: CurrentClusterState =>
      activeNodes = state.members.collect {
        case m if m.status == MemberStatus.Up => m.address
      }

    case Terminated =>
      println(s"\n ${sender() } died. resetting the timer")
      resetTimer()

    case msg:Object => //println(s"Default message $msg")

  }

  def handleAppendEntryRequest(appendEntryFromLeader: APPEND_ENTRIES) =
  {

    //val buffer =new  StringBuilder()

    var success=false
    var alreadyReplied = false
    var process = true
    var iCount = 0
    var index = 0
    var count = 0

    var myLogData = commited_Entries
    var myLoglength = myLogData.size

    //print(s" appendEntryFromLeader ${appendEntryFromLeader} currentTerm ${currentTerm}")

    // The follower will reject the request if the leaders term is lower than the follower’s current term — there is a newer leader!

    val term_Leader = appendEntryFromLeader.term
    val prevLeaderLogEntry_Leader = appendEntryFromLeader.prevLogEntry
    val prevLogIndex_Leader = appendEntryFromLeader.prevLogEntry.currentIndex
    val prevLogTerm_Leader = appendEntryFromLeader.prevLogEntry.term
    val leaderCommitIndex_Leader = appendEntryFromLeader.leaderCommitIndex
    val leaderLogEntries = appendEntryFromLeader.data


    if(leaderLogEntries.size > 0 || prevLeaderLogEntry_Leader!=emptyLogEntry)
      DEBUG = true

    logger(s" Leader Data ==> term ${term_Leader} prevLeaderLogEntry_Leader : ${prevLeaderLogEntry_Leader} leaderCommitIndex ${leaderCommitIndex_Leader} leaderLogEntries.size = ${leaderLogEntries.size} leaderLogEntries = ${leaderLogEntries}")
    logger(s" myLoglength $myLoglength lastCommitedEntry=$lastCommitedEntry mycurrentTerm= ${currentTerm}")

    var conflictIndex = 0
    var conflictTerm = 0

    if(term_Leader >= currentTerm)
      {

        if(currentLeader!=sender())
        {
            println(s"\n\n !!!!!!!!!!!!!!!! ${sender.path.name} is the leader for term ${term_Leader} !!!!!!!!!!!!!\n\n")
        }

        currentLeader = sender()
        myState = FOLLOWER
        currentTerm = term_Leader

        //reset the timer
        resetTimer()

        // leaderLogEntries size = 0 indicates heartbeat
//        if( leaderLogEntries.size == 0 && myLoglength==0)
//          {
//            //my log length is zero
//            if(myLoglength ==0) {
//              if (prevLeaderLogEntry != emptyLogEntry)
//                {
//
//                }
//                success = true
//            }
//            else if(leaderCommitIndex == commitIndex)
//              success = true
//
//          }
//          else if (leaderLogEntries.size == 0 && myLoglength>=0){
//
//          //if my last commit entry is same as leader's prev en
//          if(prevLeaderLogEntry == lastCommitedEntry){
//            success = true
//          }
//        }

        //leader's previous entry is valid and not empty
        if(leaderLogEntries.size == 0 && prevLeaderLogEntry_Leader!=emptyLogEntry){

          // I am missing some of the latest log entries. Asking the sender to send the data after myloglength
          if(prevLeaderLogEntry_Leader.currentIndex >= myLoglength && prevLeaderLogEntry_Leader!=lastCommitedEntry){
            success = false

            alreadyReplied = true

            //sender ! RESULT_APPEND_ENTRIES(currentTerm,success)
            logger(s"\n\n Sending response ${APPEND_ENTRIES_LOG_Inconsistency(currentTerm, myLoglength, -1, false)}")
            sender ! APPEND_ENTRIES_LOG_Inconsistency(currentTerm, myLoglength, -1, false)
          }

        }

        if(!alreadyReplied) {
          logger(s"My Data Size ${myLoglength}")

          logger(s""" prevLogIndex_Leader= $prevLogIndex_Leader,  myloglength= $myLoglength prevLogIndex > loglength? ${prevLogIndex_Leader > myLoglength}""")

            myPrevLogIndex = if(lastCommitedEntry!= null) lastCommitedEntry.currentIndex else -1
            myPrevLogTerm = if(lastCommitedEntry!= null) lastCommitedEntry.term else -1

            logger(s"myPrevLogIndex = ${myPrevLogIndex}, myPrevLogTerm= ${myPrevLogTerm} prevLogTerm_Leader=${prevLogTerm_Leader}")

            // if myprevLogterm and leader's  previous log term dont match, then travel down my log to get the index and term of the entry for which my previous log term does not match
            if (myPrevLogIndex > 0 && myPrevLogTerm != prevLogTerm_Leader) {
              conflictIndex = prevLogIndex_Leader
              if(myLoglength == 0)
                process = false

              iCount = myLogData.size
              while (process && iCount >= 0) {

                if (myLogData(iCount).term != myPrevLogTerm) {
                  process = false
                }
                if (process) {
                  conflictIndex = iCount
                }

                iCount -= 1

                if (iCount < 1)
                  process = false

              }
              conflictTerm = myPrevLogTerm


              logger(s"currentTerm ${currentTerm} conflictIndex=${conflictIndex + 1} myPrevLogTerm=${myPrevLogTerm}")

              alreadyReplied = true

              logger(s"\n\n Sending response ${APPEND_ENTRIES_LOG_Inconsistency(currentTerm, conflictIndex + 1, myPrevLogTerm, false)}")
              sender ! APPEND_ENTRIES_LOG_Inconsistency(currentTerm, conflictIndex + 1, myPrevLogTerm, false)

            } // End of if (myPrevLogIndex > 0 && myPrevLogTerm != prevLogTerm_Leader)
            else {

              logger(s"Already Replied?  ${alreadyReplied}")
              logger(s"conflictTerm ${conflictTerm} conflictIndex=${conflictIndex} ")

              logger(s"leaderLogEntries.size ${leaderLogEntries.size}")



              if (leaderLogEntries.size == 0)
                process = false
              else
                process = true


              if (leaderCommitIndex_Leader > commitIndex) {
                //my previous commit index
                commitIndex = if (lastCommitedEntry !=null) lastCommitedEntry.currentIndex  else -1

                if (commitIndex < 0 || commitIndex > leaderCommitIndex_Leader) {
                  commitIndex = leaderCommitIndex_Leader
                }


              }
              //logger(s"2.  leaderCommitIndex ${leaderCommitIndex_Leader} mycommitIndex ${commitIndex}")

              logger(s" commitIndex ${commitIndex} lastApplied ${lastApplied} commitIndex > lastApplied ${commitIndex > lastApplied} leaderLogEntries.size = ${leaderLogEntries.size}")


              //commit pending entries
              if (leaderCommitIndex_Leader > lastApplied && leaderLogEntries.size > 0) {
                val i = lastApplied + 1
                process = true
                if(process)
                  {
                    conflictIndex = -1
                    conflictTerm = -1
                  }
                var commitChanges=false
                while (process && leaderLogEntries.size > 0) {
                  lastApplied = i

                  logger(s"1. iCount= ${iCount}  leaderLogEntries(iCount) ${leaderLogEntries(iCount)} ${commited_Entries.size}")

                  //addToLocalLog(leaderLogEntries(iCount).command)
                  logger(s"2. iCount= ${iCount}  leaderLogEntries(iCount) ${leaderLogEntries(iCount)} ${commited_Entries.size} ${commited_Entries}")
                  //write leaders data as it is without increamenting my commit index
                  addToCommitEntryQueue(leaderLogEntries(iCount).command,LeaderLogEntry=leaderLogEntries(iCount), skipCommitIndexIncreament = true)
                  commitChanges = true
                  logger(s"3. iCount= ${iCount}  leaderLogEntries(iCount) ${leaderLogEntries(iCount)} ${commited_Entries.size}")

                  iCount += 1
                  if (i > commitIndex || iCount >= leaderLogEntries.size)
                    process = false
                }

                if(commitChanges) {
                   myStateMachineHandler ! PersistState(candidateID, commited_Entries)
                }
              }
              success = true
              //alreadyReplied = true
              //sender ! APPEND_ENTRIES_LOG_Inconsistency(currentTerm,-1,-1,true)
            }
          //}
        }
      }
    else
      {
        success = false
      }



  if(!alreadyReplied) {
    logger(s"\n\n @@@@@@@@@@@@ Sending HEartbeat response as $success - my term ${currentTerm} commited_Entries.size = ${commited_Entries.size}\n\n ")
    sender ! RESULT_APPEND_ENTRIES(currentTerm, success)
  }
    DEBUG=false
  }



  def initIndexes() = {

    seenCommands = mutable.Set.empty[Command]

    val data = commited_Entries

    data.foreach(rowData =>
    {
      seenCommands +=((rowData._2.command))
    })
    activeNodes.foreach((add:Address) =>
    {
      nextIndex += ((add,data.size))
      matchIndex += ((add,-1))

    })

    pendingCommands = ListBuffer[Command_Message]()
    processingCommand = ListBuffer[Command_Message]()

  }



  def decideAndVote(sender:ActorRef, candidateId:String,candidate_Term:Int,lastLogIndex:Int,lastLogTerm:Int) = {
    var myDecision = false

    var continueProcessing = true

    //do not vote/make any changes if already voted for the term
    if(myVoteRegistry.contains(candidate_Term) && sender!=self)
    {

      var votedCandidate = myVoteRegistry.get(candidate_Term).getOrElse(null)
      println(s" MY ID $candidateID and voted contestants ID = ${if(votedCandidate!=null)votedCandidate.candidateID else "-1"}" )

      if(votedCandidate!=null && votedCandidate.candidateID == candidateID)
        {
          println(" lol...looks like I am counting my own votes.. I need some sleep")
          continueProcessing=true
          Thread.sleep(20)
        }
      else {
        continueProcessing=false

        //println(s"1. I $self already voted to ${myVoteRegistry.get(candidate_Term)} for the term=${candidate_Term}")
//        if (candidate_Term>currentTerm) {
//                currentTerm = candidate_Term
//                myState = FOLLOWER
//                myDecision=true
//                //println(s"2. I already voted to ${myVoteRegistry.get(candidate_Term)} for the term=${candidate_Term}")
//              }
      }
      //resetTimer()

    }

    if(continueProcessing)
    {

      resetTimer()

      if(currentTerm<candidate_Term)
      {
        myDecision = true

      }
      //val data = getLogEntryData()

      val logLength  = commited_Entries.size
      myLastLogIndex = logLength
      myLastLogTerm = -1

      println(s"Voting for election - My logSize is ${logLength}")
      //get my last log term
      if(logLength>0 && lastCommitedEntry!=null){
        myLastLogTerm = lastCommitedEntry.term  //commited_Entries(myLastLogIndex).term
        }


      // reject leaders with old logs
      if (lastLogTerm < myLastLogTerm) {
        myDecision= false
      }
        if(!myDecision)
          logger(s"\n\n lastLogTerm ${lastLogTerm} ${myLastLogTerm}")

          // reject leaders with short logs
      else if (lastLogTerm == myLastLogTerm && lastLogIndex < myLastLogIndex) {
        myDecision = false
      }
      if(!myDecision)
        println(s"\n\n lastLogTerm ${lastLogTerm} ${myLastLogTerm} lastLogIndex=${lastLogIndex} and myLastLogIndex ${myLastLogIndex}")
      myVoteRegistry +=((candidate_Term , Participant_VoteRecord(candidateId,myDecision)))
      //sender ! Voted(myDecision)

      if(myDecision)
      {
        currentTerm = candidate_Term
        myState = FOLLOWER
      }

      logger(s"\n Received vote request from ${sender} for term $candidate_Term and myDecision ${myDecision}")

      sender !  RESULT_APPEND_ENTRIES(currentTerm,myDecision)
    }

  }

//  def addToLocalLog(cmd:Command): Unit ={
//    //print(s"\n a. ${commited_Entries.size}")
//    val data =    getLogEntryData(print=false)
//    //print(s"\n b. ${commited_Entries.size}")
//    if(seenCommands.add(cmd))
//      myLogger ! ADD_Entries(LogEntry(term = currentTerm,command=cmd))
//    print(s"\n c. ${commited_Entries.size}")
//  }


//  def getLogEntryData(print:Boolean = false) : ListBuffer[LogEntry] = {
//
//    implicit val timeout = Timeout(5 seconds)
//    val future2 = ask(myLogger, Get_Entries).mapTo[ListBuffer[LogEntry]]
//    val data = Await.result(future2, timeout.duration)
//
//    if(print)
//      logger(s"\n\n My Log entries ${data} \n\n")
//
//    data
//  }

  val iPrintCounter=0

  def sendAliveMessage() = {
    if(myState == LEADER) {

      resetTimer()

      votedNodes = Set.empty[ActorRef]

      if(pendingCommands.size > 0 && processingCommand.size ==0) {
        processingCommand += (pendingCommands(0))
        pendingCommands.remove(0)

      }
      //get the  CMD message from prcoessing queue
      val newCmd:Command = if(processingCommand.size > 0 )processingCommand(0).clientCommand else EMPTY_COMMAND

      //val data =    getLogEntryData(print=false)
      val data =    commited_Entries
      val logSize = data.size

      val prevLogEntry = if(logSize >0 ) data(logSize) else emptyLogEntry
      //val prevLogEntry = if(logSize >1 ) lastCommitedEntry else emptyLogEntry

     logger(s"sendAliveMessage-- prevLogEntry = $prevLogEntry ")
        //heartbeat message
      var msg = APPEND_ENTRIES(currentTerm,prevLogEntry,new ListBuffer[LogEntry],commitIndex)

      //check if the message is not the empty message (indicates heartbeat) and add the client command to the appendEntry msg
      if(newCmd!=EMPTY_COMMAND)
        {
          println(s" Sender ${sender} ~~~ Self ${self} ==> Try to persist the data in log file ")

          println(" Data persisted in the log ")

          logger(s"processingCommand ${processingCommand}")


          var currentDataSet = new ListBuffer[LogEntry]()
          currentDataSet +=(LogEntry(currentTerm,logSize + 1,newCmd))
//          if(logSize>0)
//            currentDataSet = ListBuffer(data(logSize - 1))
//          else
//            currentDataSet(0) = LogEntry(currentTerm,logSize + 1,newCmd)

          // add the new client input to dataset and broadcast it


          // mediator ! Subscribe(newCmd.toString,self)

          //        APPEND_ENTRIES(term:Int, prevLogEntry:LogEntry,data:ListBuffer[LogEntry],leaderCommitIndex:Int)
          msg = APPEND_ENTRIES(currentTerm, prevLogEntry , currentDataSet , commitIndex)
          //println(s" Data=${data} \n Sending msg=${} to all the nodes...")

//          mediator !Publish(topic,msg )
//
//          sender() ! "OK"

        }


      if(iRelinquishLeaderCount < 0)
      {
        //logger("Relinquishing my leadership.. pausing")
        iRelinquishLeaderCount = 10

        //timers.startSingleTimer("Relinquish",TIMER_UP,Duration(induceSleepValue,TimeUnit.MILLISECONDS))
        //resetTimer()
      }
      else {
        logger(s"\n sending Hearbeat for term $currentTerm")
        mediator ! Publish(topic,msg)
        iRelinquishLeaderCount -= 1
      }

    }
    else {
      logger(s"\n I am not the leader for term $currentTerm")

    }




  }

  def resetTimer(): Unit = {

    if(activeNodes.size>2) {
      if (timerActor != null && !timerActor.path.name.contains("dead")) {
        //println(s" timer actor name ${timerActor.path.name}")
        context.unwatch(timerActor)
        context.stop(timerActor)
        timerActor = null
      }

      createTimerActor()

      if (currentTerm >= maxTerm) {
        println(" reached MAX Term... Shutting Down!")
        mediator ! Publish(topic, PoisonPill)
        context.stop(self)
      }
    }
    else {
      IS_TIMER_IN_PROGRESS = false
      print("\n\n Cluster has only 2 nodes.. need more than 2 nodes for RAFT to work correctly..")
    }
  }

  def createTimerActor(START_SENDING_TIMER:Boolean =true): Unit =
  {
    IS_TIMER_IN_PROGRESS = false

    timerActor = context.actorOf(Props(classOf[RAFT_TimerHandler],myState), name="timer"+iTimerCount.incrementAndGet())
    context.watchWith(timerActor, INIT_TIMER)
    Thread.sleep(5)
    //println(s" START_SENDING_TIMER ${START_SENDING_TIMER}")
    if(START_SENDING_TIMER)
      timerActor ! INIT_TIMER
  }

  /**
   * persist the command to the state machine
   *
   * @param command
   * @return
   */
  def addToCommitEntryQueue(command: Command, LeaderLogEntry:LogEntry=emptyLogEntry, skipCommitIndexIncreament:Boolean=false):Unit = {
    var commited = false

    println(s"\n\nBEfore Commit -- commited_Entries.size ${commited_Entries.size} ${commited_Entries}")
    lastApplied = commitIndex
    if(commitIndex<0)
      commitIndex = 0

    //state being updated by leader for missing entries - so use the commitindex of leader's data
    if(!skipCommitIndexIncreament)
     commitIndex +=1

    var nextCommitIndex = commitIndex


    var finalEntry = Option.empty[LogEntry].orNull
    if(LeaderLogEntry !=emptyLogEntry) {
      finalEntry = LeaderLogEntry
      nextCommitIndex = LeaderLogEntry.currentIndex
    }
    else
    {
      finalEntry = LogEntry(currentTerm,commitIndex,command)

    }

    commited_Entries(nextCommitIndex) = finalEntry
    lastCommitedEntry = finalEntry

//    commited = myStateMachineHandler!PersistState(candidateID,finalEntry)
//
//
//println(s" finalEntry ${finalEntry} commited? ${commited}")
//    if(!commited)
//      {
//        //error saving data to the state machine so revert back the commit index
//
//        commitIndex -=1
//        lastApplied = lastApplied - (if(commitIndex == 0) 0 else 1)
//        println(s" ??????????? $command could not be saved to the state machine ????????????????")
//      }else{
//
//      lastCommitedEntry = finalEntry
//
//      commited_Entries addOne  finalEntry

      println(s" ************** $command is successfully added to the state machine ************** \n\n After commit commited_Entries.size ${commited_Entries.size} ${commited_Entries} ")
  }
  // write the data to file
  def writeOutputToFile(actorName:String, data:String): Unit =
  {
    var fw:FileWriter = null
    val fileName =  "RAFT_Leader_Election.txt"
    try {
      fw = new FileWriter(s"${fileName}", true)
      fw.write(data+"\n")
      logger(s"writing $data in file ")

    }
    catch
      {
        case e:Exception => {
          print(s"Error creating/writing to the file RAFT_Leader_Election.txt. Error Message = ${e.getMessage}")

        }
      }finally {
      if(fw!=null)
        fw.close()
    }

  }



  override val supervisorStrategy = OneForOneStrategy() {
    case _: IllegalArgumentException     => SupervisorStrategy.Resume
    case _: ActorInitializationException => SupervisorStrategy.Stop
    case _: DeathPactException           => SupervisorStrategy.Stop
    case _: Exception                    => SupervisorStrategy.Resume
  }

  def logger(data:String,forceDisplay:Boolean=false): Unit =
    if(DEBUG || forceDisplay) {
      println(s"\n ${self.path.name} $myState $data \n")
    }


def readFromStateMachine() = {
  myStateMachineHandler ! LOAD_FROM_FILE(candidateID)
  implicit val timeout = Timeout(60 seconds)
  val future2 = ask(myStateMachineHandler, Get_Entries).mapTo[mutable.HashMap[Int,LogEntry]]
  commited_Entries = Await.result(future2, timeout.duration)


  println(" Data "+commited_Entries)
  if(commited_Entries.size > 0) {
    var entry:LogEntry = commited_Entries(commited_Entries.size)


    lastCommitedEntry = entry

    currentTerm = entry.term
    commitIndex=entry.currentIndex
    lastApplied = commitIndex

    println(s"\n\n I ${self.path.name} starting with currentTerm=$currentTerm commitIndex=$commitIndex lastApplied=$lastApplied  lastCommitedEntry = $lastCommitedEntry data Size= ${commited_Entries.size}\n\n")

  }
}


  var currentTerm:Int=0
  var currentLeader = Option.empty[ActorRef].orNull

  //index of highest log entry known to be committed (initialized to 0, increases monotonically)
  var commitIndex: Int = -1

  //index of highest log entry applied to state machine (initialized to 0, increases monotonically)
  var lastApplied: Int = -1

  var conflictTerm: Int = 0
  var myLastConflictTerm: Int = 0
  var conflictIndex : Int= 0
  var prevLogIndex: Int = 0

  var myPrevLogIndex : Int= -1
  var myPrevLogTerm : Int= -1

  var nextIndex = mutable.HashMap[Address,Int]()
  var matchIndex = mutable.HashMap[Address,Int]()


  var myLastLogIndex:Int = -1
  var myLastLogTerm:Int = -1
  //join the cluster
  private val cluster: Cluster = Cluster(context.system)

  var timerActor:ActorRef = Option.empty[ActorRef].orNull
  var myState : STATE = FOLLOWER


  var activeNodes: collection.SortedSet[Address] = collection.SortedSet.empty[Address]
  var votedNodes = mutable.Set.empty[ActorRef]
  var respondedNodes  = mutable.Set.empty[ActorRef]

  var processedCMD = mutable.HashSet[Command]()

  var IS_TIMER_IN_PROGRESS:Boolean = false
  var myVoteRegistry:mutable.HashMap[Int,Participant_VoteRecord] = mutable.HashMap[Int,Participant_VoteRecord]()

  var iTimerCount:AtomicInteger = new AtomicInteger(0)

  var iRelinquishLeaderCount = 10
  val generator = new scala.util.Random

  val induceSleepValue = ConfigFactory.load("RAFT_CLUSTER").getInt("raft.timer.induceSleep")



  createTimerActor(false)

  var celebrated_leadership = false

  //var myLogger = context.actorOf(Props[LogRepository], name = "myLogger")



  //subscribing to the communication channel
  val mediator = DistributedPubSub(context.system).mediator

  var iReLeaderElectionCount=0

  val topic = "RAFT"
  var tempTopic=""
  mediator ! Subscribe(topic, self)

  println(s"\n\n *** ${self.path} joined RAFT algorithm  ***  \n\n")

  val EMPTY_COMMAND = Command("RAFT_EMPTY")
  val emptyLogEntry = LogEntry(currentTerm,-1,EMPTY_COMMAND )



  var pendingCommands = ListBuffer[Command_Message]()
  var processingCommand = ListBuffer[Command_Message]()



  var DEBUG=false


  var lastCommitedEntry : LogEntry  = Option.empty[LogEntry].orNull

  var myStateMachineHandler = context.actorOf(Props(classOf[StateMachineHandler]), name="StateMachineHandler")
  var seenCommands = mutable.Set.empty[Command]
  val maxTerm = 100
  val stateMachineName = s"RAFT_${self.path.name}_StateMachine.json"

  var commited_Entries = mutable.HashMap[Int,LogEntry]();

  readFromStateMachine()

  self ! INIT_TIMER()
}
