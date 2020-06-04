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
import com.raft.handlers.{RAFT_TimerHandler, StateMachineHandler}
import com.raft.util._
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.collection.mutable._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class Raft_Participants(candidateID: String) extends Actor with ActorLogging with Timers {


  override def preStart(): Unit = {
    super.preStart()
    //subscribe to cluster member events
    cluster.subscribe(self, classOf[MemberEvent])
    readFromStateMachine()
  }

  override def postStop(): Unit = {
    super.postStop()
    cluster unsubscribe self
  }


  //skip commiting mywall entries when I just became the leader.. first get consensus
  var skipCommit = false
  override def receive: Receive = {

    // initialize the timer
    case INIT_TIMER =>
      if (!IS_TIMER_IN_PROGRESS) {


        IS_TIMER_IN_PROGRESS = true

        //println("\n Received request to start the election timer")
        resetTimer()
      }
      else {
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
        mediator ! Publish(topic, RequestVote(candidateID, currentTerm, if (lastCommitedEntry != null) lastCommitedEntry.currentIndex else myLastLogIndex, if (lastCommitedEntry != null) lastCommitedEntry.term else myLastLogTerm))

        resetTimer()
      }

    case RequestVote(candidateId, term, lastLogIndex, lastLogTerm) =>

      //readFromStateMachine()


      if (sender != self) {
        decideAndVote(sender, candidateId, term, lastLogIndex, lastLogTerm)
      }

    //received append Entries from leader
    case leaderEntry: APPEND_ENTRIES =>

      resetTimer()

      if (sender() != self) {
        logger(s" *** $self Received append entry from leader $currentLeader - Entry => $leaderEntry")

        val appendEntryResult = handleAppendEntryRequest(leaderEntry)

        //print(" Ready with the append_entry_result "+appendEntryResult)

        sender ! appendEntryResult
      }


    //received response from the followers
    case result: RESULT_APPEND_ENTRIES =>

      respondedNodes += sender()

      if (result.decision && myState != FOLLOWER) {
        votedNodes += sender

        //get quorum of maximum votes
        if (votedNodes.size >= (1 + (activeNodes.size) / 2)) {

          currentLeader = self
          if (myState == CANDIDATE) {
            myState = LEADER
            skipCommit=true
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
          if (myWall.size > 0 && !skipCommit) {

            val data: Command_Message = myWall(0)

            addToCommitEntryQueue(data.sender,data.clientCommand)
            persistData()

            val commitResult = persistData
            val commitStatus: Boolean = commitResult.result
            commited_Entries = commitResult.committedEntries

            //add the command to the state machine and respond to the client/sender
            if (commitStatus) {
              data.sender ! "OK"
            }
            else {
              data.sender ! "TRY AGAIN!"
            }

            myWall.remove(0)

            println(s"\n\n  ============ commited_Entries after persis $commited_Entries =============== \n\n Sending the alive message")
            //timers.startSingleTimer("Heartbeat",TIMER_UP,Duration(induceSleepValue,TimeUnit.MILLISECONDS))
            // self ! SEND_HEARTBEAT
            sendAliveMessage(true)
          }
          else {
            sendAliveMessage()
          }


        }
      } else {
        if (result.term > currentTerm) {
          myState = FOLLOWER
          initIndexes()
          currentTerm = result.term
        }
        else if (respondedNodes.size >= ((activeNodes.size) / 2)) {
          myState = FOLLOWER
          initIndexes()
          resetTimer()
        }
      }
      skipCommit=false

    case cmd: Command =>
      if (myState == LEADER) {
        println(s" \n\n &&&&&&&&&&&&&&&& Received Data ${cmd} from sender ${sender()} activeNodes.size= ${activeNodes.size} myState=${myState}")

        //addToLocalLog(cmd)

        val cmdEntry = Command_Message(sender(), cmd)
        pendingCommands += cmdEntry
        sendAliveMessage()
      }
      else {
        print(s" \n ~~~~~~~~~~~~~~ Forwarding the command to leader ${currentLeader}")
        if (currentLeader == null || (currentLeader == self && myState != LEADER)) {
          println("\n The leader is not avaialable. Please try again later! \n")
          sender() ! "TRY AGAIN!"
        }
        else {
          currentLeader.forward(cmd)
        }
      }
    case logInconsistency: APPEND_ENTRIES_LOG_Inconsistency =>

      if (myState == LEADER) {

        //read the committed entries
        val logData = commited_Entries

        val dataSet = ListBuffer[LogEntry]()

        logger(s"\n - Sender ${sender()} logInconsistency ${logInconsistency}", true)
        nextIndex += ((sender.path.address, if (logInconsistency.conflictIndex <= 0) 0 else {
          logInconsistency.conflictIndex + (if (logInconsistency.conflictIndex == logData.size) 0 else 1)
        }))


        var iCount = nextIndex(sender.path.address)

        var previousEntry = if (iCount>0 && logData.size > 1) extractLogEntryInfo(logData(logData.size)) else emptyLogEntry

        println(s"\n iCount=${iCount} previousEntry=${previousEntry}")

        iCount = if(iCount<=0) 1 else iCount
        println(s"\n New iCount=${iCount} logData.size=${logData.size}")

        while (iCount <= logData.size) {
          dataSet += extractLogEntryInfo(logData(iCount))
          iCount += 1
        }



        println(s"\n APPEND_ENTRIES_LOG_Inconsistency => logData : ${logData} ${logInconsistency.conflictIndex}")
        if (logInconsistency.conflictIndex > 0) {
          previousEntry = extractLogEntryInfo(logData(logInconsistency.conflictIndex))
        }
        val msg = APPEND_ENTRIES(currentTerm, previousEntry, dataSet, commitIndex)
        println(s" Data=$dataSet dataSet.size=${dataSet.size} ==> Sending msg=APPEND_ENTRIES in response to APPEND_ENTRIES_LOG_Inconsistency to ${sender}", true)


        sender ! msg
      }

    case "Heartbeat" =>
      self ! SEND_HEARTBEAT

    case TIMER_UP =>
      //println("Resuming Again!!")
      resetTimer()
    // SEND_HEARTBEAT is triggered by RAFT_TimerHandler
    case SEND_HEARTBEAT =>
      logger(" SEND_HEARTBEAT : ${getLogEntryData(print = true)}")

      sendAliveMessage()
    case MemberUp(member) =>
      activeNodes += member.address

      println(s"\n ${member} joined with role ${member.roles}. ${activeNodes.size} is the cluster size")

      if (activeNodes.size > 2 && !IS_TIMER_IN_PROGRESS) {
        println("\n starting the election timer")
        self ! INIT_TIMER
      }

    case MemberRemoved(member, _) =>
      activeNodes -= member.address

      println(s"\n ${member} left with role ${member.getRoles}. ${activeNodes.size} is the cluster size")

    case state: CurrentClusterState =>
      activeNodes = state.members.collect {
        case m if m.status == MemberStatus.Up => m.address
      }

    case Terminated =>
      println(s"\n ${sender()} died. resetting the timer")
      resetTimer()

    case msg: Object => //println(s"Default message $msg")

  }


  def extractLogEntryInfo(entry:Simplified_LogEntry) = {

    LogEntry(entry.term,entry.index,entry.data,null)
  }

  def getSimplifiedLE(entry:LogEntry) = {

    Simplified_LogEntry(entry.term,entry.currentIndex,entry.command)
  }

  def handleAppendEntryRequest(appendEntryFromLeader: APPEND_ENTRIES) = {

    //val buffer =new  StringBuilder()

    var success = false
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


    if (leaderLogEntries.size > 0 || prevLeaderLogEntry_Leader != emptyLogEntry)
      DEBUG = true

    logger(s" Leader Data ==> term ${term_Leader} prevLeaderLogEntry_Leader : ${prevLeaderLogEntry_Leader} leaderCommitIndex ${leaderCommitIndex_Leader} leaderLogEntries.size = ${leaderLogEntries.size} leaderLogEntries = ${leaderLogEntries}")
    logger(s" myLoglength $myLoglength lastCommitedEntry=$lastCommitedEntry mycurrentTerm= ${currentTerm}")

    var conflictIndex = 0
    var conflictTerm = 0

    if (term_Leader >= currentTerm) {

      if (currentLeader != sender()) {
        println(s"\n\n !!!!!!!!!!!!!!!! ${sender.path.name} is the leader for term ${term_Leader} !!!!!!!!!!!!!\n\n")
      }

      currentLeader = sender()
      myState = FOLLOWER
      currentTerm = term_Leader

      //reset the timer
      resetTimer()


      //received heartbeat after consensus on the last command
      if(leaderLogEntries.size == 0 && prevLeaderLogEntry_Leader != emptyLogEntry && !myWall.isEmpty){
        if(prevLeaderLogEntry_Leader==myWall(0)){
              addToCommitEntryQueue(prevLeaderLogEntry_Leader.sender,prevLeaderLogEntry_Leader.command,LeaderLogEntry = prevLeaderLogEntry_Leader,skipCommitIndexIncreament = true)
              myWall.remove(0)
          persistData()
          sender ! RESULT_APPEND_ENTRIES(currentTerm, success)
          alreadyReplied = true
        }
      }

      success = (prevLeaderLogEntry_Leader == emptyLogEntry) || (prevLeaderLogEntry_Leader != emptyLogEntry
        && (prevLogIndex_Leader <= myLoglength && myLastLogTerm == prevLogTerm_Leader))

      logger(s"Dummy success ${success}")
//      if (!success && !alreadyReplied) {
//        conflictIndex = myLoglength
//        logger(s"\n\n Sending response ${APPEND_ENTRIES_LOG_Inconsistency(currentTerm, conflictIndex, 1, false)}")
//        sender ! APPEND_ENTRIES_LOG_Inconsistency(currentTerm, conflictIndex, -1, false)
//      }



      // Return failure if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm

      if (!alreadyReplied && (leaderLogEntries.size == 0 && prevLeaderLogEntry_Leader != emptyLogEntry)
        && (prevLeaderLogEntry_Leader.currentIndex >= myLoglength && prevLeaderLogEntry_Leader != lastCommitedEntry)) {
        success = false

        alreadyReplied = true

        //    I am missing some of the latest log entries. Asking the sender to send the data after myloglength
        conflictIndex = myLoglength

        logger(s"\n\n Sending response ${APPEND_ENTRIES_LOG_Inconsistency(currentTerm, conflictIndex, -1, false)}")
        sender ! APPEND_ENTRIES_LOG_Inconsistency(currentTerm, conflictIndex, -1, false)

      }

      if (!alreadyReplied) {
        logger(s"My Data Size ${myLoglength}")

        logger(s""" prevLogIndex_Leader= $prevLogIndex_Leader,  myloglength= $myLoglength prevLogIndex > loglength? ${prevLogIndex_Leader > myLoglength}""")

        myPrevLogIndex = if (lastCommitedEntry != null) lastCommitedEntry.currentIndex else -1
        myPrevLogTerm = if (lastCommitedEntry != null) lastCommitedEntry.term else -1

        logger(s"myPrevLogIndex = ${myPrevLogIndex}, myPrevLogTerm= ${myPrevLogTerm} prevLogTerm_Leader=${prevLogTerm_Leader}")

        logger(s"my lastCommitedEntry = ${lastCommitedEntry}, Leaders prev entry= ${prevLeaderLogEntry_Leader}")

        var proceedWithCommit = true

        // if myprevLogterm and leader's  previous log term dont match, then travel down my log to get the index and term of the entry for which my previous log term does not match
        //if (myPrevLogIndex > 0 && myPrevLogTerm != prevLogTerm_Leader) {
        if (myPrevLogIndex > 0 && lastCommitedEntry != prevLeaderLogEntry_Leader  && prevLeaderLogEntry_Leader != emptyLogEntry) {
          conflictIndex = prevLogIndex_Leader
          if (myLoglength == 0)
            process = false

          iCount = myLogData.size
          conflictTerm = myPrevLogTerm

          var deleteFlag = false
            //if myprevLogterm and leader's  previous log term dont match,
            //      then travel down my log to get the index and term of the entry for which my previous log term does not match
            if (myPrevLogIndex > 0 && myPrevLogTerm != prevLogTerm_Leader && prevLogTerm_Leader!=emptyLogEntry) {
              process = true

              conflictIndex = prevLogIndex_Leader

              iCount = myLogData.size

              while (process && myLogData.size > 0) {
                //retract until I am exhausted or match found with the leaders previous log entry
                logger(s" ------> iCount=${iCount} - my myLogData(iCount) = ${myLogData(iCount)}, Leaders prev entry= ${prevLeaderLogEntry_Leader} myLogData(iCount) == prevLeaderLogEntry_Leader? ${(myLogData(iCount) == prevLeaderLogEntry_Leader)}")
                if (myLogData(iCount).term != myPrevLogTerm) {
                  process = false
                }
                else {
                  proceedWithCommit = false
                  iCount -= 1

                  if (iCount < 1) //reached the begining of my log
                    process = false
                }
                conflictIndex = iCount
              } //End of while
            }

          logger(s"currentTerm ${currentTerm} conflictIndex=${conflictIndex} myPrevLogTerm=${myPrevLogTerm}")

          //this indicates all my entries are invalid and I must wipe it
            if(myLoglength - conflictIndex>0)
              {
                  deletEntriesIfExists(conflictIndex,true)
              }
          if (!proceedWithCommit) {
            alreadyReplied = true
            logger(s"\n\n Sending response ${APPEND_ENTRIES_LOG_Inconsistency(currentTerm, conflictIndex, myPrevLogTerm, false)}")
            sender ! APPEND_ENTRIES_LOG_Inconsistency(currentTerm, conflictIndex, myPrevLogTerm, false)

            if (deleteFlag) {
              //delete entries afterwards
              deleteEntries(conflictIndex + 1)
            }
          }


        } // End of if (myPrevLogIndex > 0 && myPrevLogTerm != prevLogTerm_Leader)

        logger(" proceedWithCommit ? " + proceedWithCommit)

        if (proceedWithCommit) {

          logger(s"Already Replied?  ${alreadyReplied}")
          logger(s"conflictTerm ${conflictTerm} conflictIndex=${conflictIndex} ")

          logger(s"leaderLogEntries.size ${leaderLogEntries.size}")


          if (leaderLogEntries.size == 0)
            process = false
          else
            process = true


          if (leaderCommitIndex_Leader > commitIndex) {
            //my previous commit index
            commitIndex = if (lastCommitedEntry != null) lastCommitedEntry.currentIndex else -1

            if (commitIndex < 1 || commitIndex > leaderCommitIndex_Leader) {
              commitIndex = leaderCommitIndex_Leader
            }


          }
          //logger(s"2.  leaderCommitIndex ${leaderCommitIndex_Leader} mycommitIndex ${commitIndex}")

          logger(s" commitIndex ${commitIndex} lastApplied ${lastApplied} commitIndex > lastApplied ${commitIndex > lastApplied} leaderLogEntries.size = ${leaderLogEntries.size}")


          //commit pending entries
          if (leaderCommitIndex_Leader > lastApplied && leaderLogEntries.size > 0) {
             process = true

              conflictIndex = -1
              conflictTerm = -1

            var commitChanges = false
            while (process && leaderLogEntries.size > 0) {

              logger(s"1. iCount= ${iCount}  leaderLogEntries(iCount) ${leaderLogEntries(iCount)} ${commited_Entries.size}")

              //addToLocalLog(leaderLogEntries(iCount).command)
              logger(s"2. iCount= ${iCount}  leaderLogEntries(iCount) ${leaderLogEntries(iCount)} ${commited_Entries.size} ${commited_Entries}")
              //write leaders data as it is without increamenting my commit index
              addToCommitEntryQueue(leaderLogEntries(iCount).sender,leaderLogEntries(iCount).command, LeaderLogEntry = leaderLogEntries(iCount), skipCommitIndexIncreament = true)
              commitChanges = true
              logger(s"3. iCount= ${iCount}  leaderLogEntries(iCount) ${leaderLogEntries(iCount)} ${commited_Entries.size}")

              iCount += 1
              if (iCount >= leaderLogEntries.size)
                process = false
            }

            deletEntriesIfExists(deleteAfterIndex = commitIndex + 1 )
            if (commitChanges) {
              persistData()
            }
          }
          success = true
          //alreadyReplied = true
          //sender ! APPEND_ENTRIES_LOG_Inconsistency(currentTerm,-1,-1,true)
        }
        //}
      }
    }
    else {
      success = false
    }


    if (!alreadyReplied) {
      logger(s"\n\n @@@@@@@@@@@@ Sending HEartbeat response as $success - my term ${currentTerm} commited_Entries.size = ${commited_Entries.size}\n\n ")
      if(success && leaderLogEntries.size != 0)
        {
          myWall += Command_Message(leaderLogEntries(0).sender,leaderLogEntries(0).command)
        }

      sender ! RESULT_APPEND_ENTRIES(currentTerm, success)
    }
    DEBUG = false
  }



  def initIndexes() = {

    seenCommands = mutable.Set.empty[Command]

    val data = commited_Entries

    data.foreach(rowData => {
      seenCommands += ((rowData._2.data))
    })
    activeNodes.foreach((add: Address) => {
      nextIndex += ((add, data.size))
      matchIndex += ((add, -1))

    })

    pendingCommands = ListBuffer[Command_Message]()

    //myWall = ListBuffer[Command_Message]()

  }


  /**
   * respond to election request from a candidate
   *
   * @param sender
   * @param candidateId
   * @param candidate_Term
   * @param lastLogIndex
   * @param lastLogTerm
   */
  def decideAndVote(sender: ActorRef, candidateId: String, candidate_Term: Int, lastLogIndex: Int, lastLogTerm: Int) = {
    var myDecision = false

    var continueProcessing = true

    //do not vote/make any changes if already voted for the term
    if (myVoteRegistry.contains(candidate_Term) && sender != self) {

      continueProcessing = false

      var votedCandidate = myVoteRegistry.get(candidate_Term).getOrElse(null)
      println(s" MY ID $candidateID and voted contestants ID = ${if (votedCandidate != null) votedCandidate.candidateID else "-1"}")

      if (votedCandidate != null && votedCandidate.candidateID == candidateID) {
        println(" lol...looks like I am counting my own votes.. I need some sleep")
        continueProcessing = true
        Thread.sleep(20)
      }

    }

    if (continueProcessing) {

      resetTimer()

      if (currentTerm < candidate_Term) {
        myDecision = true

      }
      //val data = getLogEntryData()

      val logLength = commited_Entries.size
      myLastLogIndex = logLength
      myLastLogTerm = -1

      println(s"Voting for election - My logSize is ${logLength}")
      //get my last log term
      if (logLength > 0 && lastCommitedEntry != null) {
        myLastLogTerm = lastCommitedEntry.term //commited_Entries(myLastLogIndex).term
      }


      // reject leaders with old logs
      if (lastLogTerm < myLastLogTerm) {
        myDecision = false
      }
      if (!myDecision)
        logger(s"\n\n lastLogTerm ${lastLogTerm} ${myLastLogTerm}")

      // reject leaders with short logs
      else if (lastLogTerm == myLastLogTerm && lastLogIndex < myLastLogIndex) {
        myDecision = false
      }
      if (!myDecision)
        println(s"\n\n lastLogTerm ${lastLogTerm} ${myLastLogTerm} lastLogIndex=${lastLogIndex} and myLastLogIndex ${myLastLogIndex}")
      myVoteRegistry += ((candidate_Term, Participant_VoteRecord(candidateId, myDecision)))
      //sender ! Voted(myDecision)

      if (myDecision) {
        currentTerm = candidate_Term
        myState = FOLLOWER
      }

      logger(s"\n Received vote request from ${sender} for term $candidate_Term and myDecision ${myDecision}")

      sender ! RESULT_APPEND_ENTRIES(currentTerm, myDecision)
    }

  }


  def sendAliveMessage(postCommitLog: Boolean = false) = {
    if (myState == LEADER) {

      resetTimer()

      votedNodes = Set.empty[ActorRef]

      //val data =    getLogEntryData(print=false)
      val data = commited_Entries
      val logSize = data.size

      val prevLogEntry = if (logSize > 0) extractLogEntryInfo(data(logSize)) else emptyLogEntry

      var currentDataSet = new ListBuffer[LogEntry]()

      var newLogEntry = emptyLogEntry
      //get the  CMD message from prcoessing queue
      var newCmd: Command =  EMPTY_COMMAND

      if (pendingCommands.size > 0 && myWall.size == 0) {

        newCmd = pendingCommands(0).clientCommand
        myWall +=pendingCommands(0)

        pendingCommands.remove(0)

      }

      if(myWall.size>0)
        {
          newLogEntry = LogEntry(currentTerm, logSize + 1, myWall(0).clientCommand,myWall(0).sender)
          currentDataSet += newLogEntry
        }


      //val prevLogEntry = if(logSize >1 ) lastCommitedEntry else emptyLogEntry

      logger(s"sendAliveMessage-- logSize = $logSize prevLogEntry=$prevLogEntry", postCommitLog)

      logger(s"sendAliveMessage-- prevLogEntry = $prevLogEntry commited_Entries=$commited_Entries", postCommitLog)
      //heartbeat message
      var msg = APPEND_ENTRIES(currentTerm, prevLogEntry, currentDataSet, commitIndex)


      if (iRelinquishLeaderCount < 0) {
        //logger("Relinquishing my leadership.. pausing")
        iRelinquishLeaderCount = 10

      }
      else {
        logger(s"\n sending Hearbeat for term $currentTerm", postCommitLog)
        mediator ! Publish(topic, msg)
        iRelinquishLeaderCount -= 1
      }

    }
    else {
      logger(s"\n I am not the leader for term $currentTerm", postCommitLog)

    }


  }

  def resetTimer(): Unit = {

    if (activeNodes.size > 2) {
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

  def createTimerActor(START_SENDING_TIMER: Boolean = true): Unit = {
    IS_TIMER_IN_PROGRESS = false

    timerActor = context.actorOf(Props(classOf[RAFT_TimerHandler], myState), name = "timer" + iTimerCount.incrementAndGet())
    context.watchWith(timerActor, INIT_TIMER)
    Thread.sleep(5)
    //println(s" START_SENDING_TIMER ${START_SENDING_TIMER}")
    if (START_SENDING_TIMER)
      timerActor ! INIT_TIMER
  }

  /**
   * persist the command to the state machine
   *
   * @param command
   * @return
   */
  def addToCommitEntryQueue(sender:ActorRef,command: Command, LeaderLogEntry: LogEntry = emptyLogEntry, skipCommitIndexIncreament: Boolean = false): Unit = {
    var commited = false

    println(s"\n\nBEfore Commit -- commited_Entries.size ${commited_Entries.size} ${commited_Entries}")
    lastApplied = commitIndex
    if (commitIndex < 0)
      commitIndex = 0

    //state being updated by leader for missing entries - so use the commitindex of leader's data
    if (!skipCommitIndexIncreament)
      commitIndex += 1

    var nextCommitIndex = commitIndex


    var finalEntry = Option.empty[LogEntry].orNull
    if (LeaderLogEntry != emptyLogEntry) {
      finalEntry = LeaderLogEntry
      nextCommitIndex = LeaderLogEntry.currentIndex
    }
    else {
      finalEntry = LogEntry(currentTerm, commitIndex, command,sender)

    }

    commited_Entries(nextCommitIndex) = getSimplifiedLE(finalEntry)
    lastCommitedEntry = finalEntry
    commitIndex = nextCommitIndex

    println(s" ************** $command is successfully added to the state machine **************");
    println(s"\n\n After commit, commitIndex=$commitIndex  commited_Entries.size ${commited_Entries.size} ${commited_Entries} ")
  }

  // write the data to file
  def writeOutputToFile(actorName: String, data: String): Unit = {
    var fw: FileWriter = null
    val fileName = "RAFT_Leader_Election.txt"
    try {
      fw = new FileWriter(s"${fileName}", true)
      fw.write(data + "\n")
      logger(s"writing $data in file ")

    }
    catch {
      case e: Exception => {
        print(s"Error creating/writing to the file RAFT_Leader_Election.txt. Error Message = ${e.getMessage}")

      }
    } finally {
      if (fw != null)
        fw.close()
    }

  }


  override val supervisorStrategy = OneForOneStrategy() {
    case _: IllegalArgumentException => SupervisorStrategy.Resume
    case _: ActorInitializationException => SupervisorStrategy.Stop
    case _: DeathPactException => SupervisorStrategy.Stop
    case _: Exception => SupervisorStrategy.Resume
  }

  def logger(data: String, forceDisplay: Boolean = false): Unit =
    if (DEBUG || candidateID.indexOf("25256")> -1 || (myState==LEADER)) {
      println(s"\n ${self.path.name} $myState $data \n")
    }

  /**
   * delete all extra entries from the log
   *
   * @param deleteAfterIndex
   */

  def deletEntriesIfExists(deleteAfterIndex :Int, saveState:Boolean=true )={

    println(s"Trying to delete entries post index= $deleteAfterIndex  while my current commit size is ${commited_Entries.size}")

    println(s"commited_Entries BEFORE delete - ${commited_Entries}")

    //discard entries after deleteAfterIndex
    commited_Entries = commited_Entries.filter( data=>{
      println(s"\n deleteAfterIndex=$deleteAfterIndex - Deleting data $data data._1 <= deleteAfterIndex? ${data._1 <= deleteAfterIndex}")
      data._1 <= deleteAfterIndex
    })

    println(s"commited_Entries post delete - ${commited_Entries}")

    if(saveState)
      persistData()
    else
      readFromStateMachine()

  }

  def persistData():StateMachine_Update_Result = {
    myStateMachineHandler ! PersistState(candidateID, commited_Entries)

    implicit val timeout = Timeout(300 millisecond)
    val future2 = ask(myStateMachineHandler, COMMIT_STATUS).mapTo[StateMachine_Update_Result]
    val commitResult = Await.result(future2, timeout.duration)

    commited_Entries = commitResult.committedEntries

    print(s"\n Updated commited Entries (${commited_Entries.size}) - ${commited_Entries} ")
    refreshEntries

    commitResult
  }

  def readFromStateMachine() = {
    myStateMachineHandler ! LOAD_FROM_FILE(candidateID)
    updateMyStateProperties
  }

  def deleteEntries(purgeIndex: Int) {

    print(s"\n\n ***************88 purgeIndex $purgeIndex commited_Entries.size=${commited_Entries.size}")
    if (purgeIndex < commited_Entries.size) {
      myStateMachineHandler ! PurgeInvalidEntries(candidateID, purgeIndex)

      updateMyStateProperties

    }

  }

  def updateMyStateProperties() = {

    implicit val timeout = Timeout(600 millisecond)
    val future2 = ask(myStateMachineHandler, Get_Entries).mapTo[mutable.HashMap[Int, Simplified_LogEntry]]
    commited_Entries = Await.result(future2, timeout.duration)

    refreshEntries

    println(" Data " + commited_Entries)

  }

  def refreshEntries(): Unit =
  {
    if (commited_Entries.size > 0) {
      var entry: LogEntry = extractLogEntryInfo(commited_Entries(commited_Entries.size))

      lastCommitedEntry = entry
      currentTerm = entry.term
      commitIndex = entry.currentIndex
      lastApplied = commitIndex
      //      if(myState!=LEADER )
      //      myState = FOLLOWER

      println(s"\n\n I ${self.path.name} starting with currentTerm=$currentTerm commitIndex=$commitIndex lastApplied=$lastApplied  lastCommitedEntry = $lastCommitedEntry data Size= ${commited_Entries.size}\n\n")

    }
    else {
      commitIndex = -1
      lastApplied = -1
      lastCommitedEntry = null
      initIndexes()
    }
  }


  var currentTerm: Int = 0
  var currentLeader = Option.empty[ActorRef].orNull

  //index of highest log entry known to be committed (initialized to 0, increases monotonically)
  var commitIndex: Int = -1

  //index of highest log entry applied to state machine (initialized to 0, increases monotonically)
  var lastApplied: Int = -1

  var conflictTerm: Int = 0
  var myLastConflictTerm: Int = 0
  var conflictIndex: Int = 0
  var prevLogIndex: Int = 0

  var myPrevLogIndex: Int = -1
  var myPrevLogTerm: Int = -1

  var nextIndex = mutable.HashMap[Address, Int]()
  var matchIndex = mutable.HashMap[Address, Int]()


  var myLastLogIndex: Int = -1
  var myLastLogTerm: Int = -1
  //join the cluster
  private val cluster: Cluster = Cluster(context.system)

  var timerActor: ActorRef = Option.empty[ActorRef].orNull
  var myState: STATE = FOLLOWER


  var activeNodes: collection.SortedSet[Address] = collection.SortedSet.empty[Address]
  var votedNodes = mutable.Set.empty[ActorRef]
  var respondedNodes = mutable.Set.empty[ActorRef]

  var processedCMD = mutable.HashSet[Command]()



  var IS_TIMER_IN_PROGRESS: Boolean = false
  var myVoteRegistry: mutable.HashMap[Int, Participant_VoteRecord] = mutable.HashMap[Int, Participant_VoteRecord]()

  var iTimerCount: AtomicInteger = new AtomicInteger(0)

  var iRelinquishLeaderCount = 10
  val generator = new scala.util.Random

  val induceSleepValue = ConfigFactory.load("RAFT_CLUSTER").getInt("raft.timer.induceSleep")


  createTimerActor(false)

  var celebrated_leadership = false

  //var myLogger = context.actorOf(Props[LogRepository], name = "myLogger")


  //subscribing to the communication channel
  val mediator = DistributedPubSub(context.system).mediator

  var iReLeaderElectionCount = 0

  val topic = "RAFT"
  var tempTopic = ""
  mediator ! Subscribe(topic, self)

  println(s"\n\n *** ${self.path} joined RAFT algorithm  ***  \n\n")

  val EMPTY_COMMAND = Command("RAFT_EMPTY")
  val emptyLogEntry = LogEntry(currentTerm, -1, EMPTY_COMMAND,null)


  var pendingCommands = ListBuffer[Command_Message]()
  var myWall =new  ListBuffer[Command_Message]()

  var DEBUG = false


  var lastCommitedEntry: LogEntry = Option.empty[LogEntry].orNull

  var myStateMachineHandler = context.actorOf(Props(classOf[StateMachineHandler]), name = "StateMachineHandler")
  var seenCommands = mutable.Set.empty[Command]
  val maxTerm = 100
  val stateMachineName = s"RAFT_${self.path.name}_StateMachine.json"

  var commited_Entries = mutable.HashMap[Int, Simplified_LogEntry]();

  readFromStateMachine()

  myState = FOLLOWER
  self ! INIT_TIMER()
}
