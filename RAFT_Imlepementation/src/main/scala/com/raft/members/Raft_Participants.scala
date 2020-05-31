package com.raft.members

import java.io.FileWriter
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorInitializationException, ActorLogging, ActorRef, Address, DeathPactException, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated, Timers}
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberEvent, MemberRemoved, MemberUp}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe, Unsubscribe}
import akka.cluster.{Cluster, MemberStatus}
import akka.pattern.ask
import akka.util.Timeout
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
    cluster.unsubscribe(self);
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

    case ELECTION_TIMEOUT(timeOut) => {

      celebrated_leadership = false

      println(s"\n The leader : ${currentLeader} is probably down and I reached election timeout ${timeOut} for term $currentTerm. Initiating election")

      votedNodes = Set.empty[ActorRef]
      myState = CANDIDATE //change myself to candidate
      currentTerm +=1 //increase the term#
      votedNodes +=self //add my vote

      myVoteRegistry. +=((currentTerm , Participant_VoteRecord(candidateID,true)))

      mediator ! Publish(topic, RequestVote(candidateID,currentTerm,0,0))

    }

    case RequestVote(candidateId, term, lastLogIndex, lastLogTerm) =>


      if(sender!=self) {
        decideAndVote(sender, candidateId, term, lastLogIndex, lastLogTerm)
      }
    case vote:Voted => //process the received election vote from other participants
      if(vote.decision)
        votedNodes +=sender

      //get quorum of maximum votes
      if(votedNodes.size >= (1 + (activeNodes.size)/2)){

        currentLeader = self
        myState = LEADER

        if(!celebrated_leadership) {


          writeOutputToFile(self.toString(), s"${self.path.name.trim} is the new leader for term=${currentTerm} \t votedNodes ${votedNodes} \t activeNodes ${activeNodes}  \n ")

          println(s"\n\n \t ********** I am the leader  for term ${currentTerm}!!  ********** \n\n")
          logger(s"\n\n votedNodes ${votedNodes} \n activeNodes ${activeNodes} \n\n")

          celebrated_leadership = true

          sendAliveMessage
        }

      }

      //received append Entries from leader
    case leaderEntry : APPEND_ENTRIES =>{

      resetTimer();

      if (sender() != self){
        logger(s" *** ${self} Received append entry from leader ${currentLeader} - Entry => ${leaderEntry}")

        var appendEntryResult = handleAppendEntryRequest(leaderEntry)

        //print(" Ready with the append_entry_result "+appendEntryResult)

        sender ! appendEntryResult
      }
    }


      //received response from the followers
    case result:RESULT_APPEND_ENTRIES => {

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

          var leaderElectionSet = HashSet[String]()
          if (leaderElectionSet.add(s"${self.path.name} is the new leader for term=${currentTerm} \t votedNodes ${votedNodes} \t activeNodes ${activeNodes}  \n ")) {
            writeOutputToFile(self.toString(), s"${self.path.name} is the new leader for term=${currentTerm} \t votedNodes ${votedNodes} \t activeNodes ${activeNodes}  \n ")

            println(s"\n\n \t ********** I am the leader  for term ${currentTerm}!!  ********** \n\n")
            logger(s"\n\n votedNodes ${votedNodes} \n activeNodes ${activeNodes} \n\n")
          }
        }


        //if received the consensus and if there is item in the processingQueue then add it to the state machine
        if(processingCommand.size>0){

          var data:Command_Message = processingCommand(0)

          //add the command to the state machine and respond to the client/sender
          if(statMachine(data.clientCommand))
            {

              var persistEntry = LogEntry(currentTerm,-1,data.clientCommand)
              var msg = CommitEntry(persistEntry,commitIndex)

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
        }
    }

    case commitEntry:CommitEntry =>
      {
        println(s" Received the commitEntry ${commitEntry}")

        //mediator ! Unsubscribe(commitEntry.logEntry.data.toString,self)

        if(sender() != self) {
          var entry = commitEntry.logEntry

          //if(currentTerm<entry.term)
          currentTerm = entry.term
          currentLeader = sender()

          resetTimer()
          addToLocalLog(commitEntry.logEntry.data)
          statMachine(commitEntry.logEntry.data)
          sender ! RESULT_APPEND_ENTRIES(currentTerm, true)
        }


      }

    case cmd:Command =>
    {
      if (myState == LEADER)
      {
        println(s" \n\n &&&&&&&&&&&&&&&& Received Data ${cmd} from sender ${sender()} activeNodes.size= ${activeNodes.size} myState=${myState}")

        addToLocalLog(cmd)

        var cmdEntry = Command_Message(sender(),cmd)
        pendingCommands +=((cmdEntry))
        sendAliveMessage()
      }
      else
      {
        print(s" ~~~~~~~~~~~~~~ Forwarding the command to leader ${currentLeader}")
        if(currentLeader==null){
          sender() ! "TRY AGAIN!"
        }
        else {
          currentLeader.forward(cmd)
        }
      }

    }
    case logInconsistency : APPEND_ENTRIES_LOG_Inconsistency =>{

      if(myState == LEADER) {

        logger(s"\n - Sender ${sender()} logInconsistency ${logInconsistency}",true)
        nextIndex += ((sender.path.address, logInconsistency.conflictIndex))

        var logData = getLogEntryData()

        var dataSet = ListBuffer[LogEntry]()

        var iCount = nextIndex(sender.path.address)
        while (iCount > -1 && iCount < logData.size) {
          dataSet += (logData(iCount))
          iCount += 1
        }

        var msg = APPEND_ENTRIES(currentTerm, if (logData.size > 1) logData(logData.size - 2) else emptyLogEntry, dataSet, commitIndex)
        println(s" Data=${dataSet} ==> Sending msg=${msg}  to ${sender}",true)


        sender ! (topic, msg)
      }

    }

    case "Heartbeat" =>
      self ! SEND_HEARTBEAT

    case TIMER_UP =>
      {
        //println("Resuming Again!!")
        resetTimer();
      }
      // SEND_HEARTBEAT is triggered by RAFT_Timer
    case SEND_HEARTBEAT =>
      {
        logger( " SEND_HEARTBEAT : ${getLogEntryData(print = true)}")

        sendAliveMessage;
      }
    case MemberUp(member) =>
      activeNodes +=member.address

      println(s"\n ${member} joined with role ${member.roles}. ${activeNodes.size} is the cluster size")

      if(activeNodes.size>2){
        println("\n starting the election timer")
        self !INIT_TIMER
      }

    case MemberRemoved(member,_) =>
      {
        activeNodes -= member.address

        println(s"\n ${member} left with role ${member.getRoles}. ${activeNodes.size} is the cluster size")
      }

    case state: CurrentClusterState =>
      activeNodes = state.members.collect {
        case m if m.status == MemberStatus.Up => m.address
      }

    case Terminated =>{
      println(s"\n ${sender() } died. resetting the timer")
      resetTimer()
    }

    case msg:Object => //println(s"Default message $msg")

  }

  def handleAppendEntryRequest(appendEntryFromLeader: APPEND_ENTRIES) =
  {

    //var buffer =new  StringBuilder()

    var success=false

    var alreadyReplied = false

    //print(s" appendEntryFromLeader ${appendEntryFromLeader} currentTerm ${currentTerm}")

    // The follower will reject the request if the leaders term is lower than the follower’s current term — there is a newer leader!

    var term = appendEntryFromLeader.term
    var prevLogIndex = appendEntryFromLeader.prevLogEntry.currentIndex
    var prevLogTerm = appendEntryFromLeader.prevLogEntry.term
    var leaderCommitIndex = appendEntryFromLeader.leaderCommitIndex
    var leaderLogEntries = appendEntryFromLeader.data

    if(leaderLogEntries.size > 0)
      //DEBUG = true

    logger(s" Leader Data ==> term ${term} prevLogIndex : ${prevLogIndex} prevLogTerm : ${prevLogTerm} leaderCommitIndex ${leaderCommitIndex} leaderLogEntries.size = ${leaderLogEntries.size} leaderLogEntries = ${leaderLogEntries}")

    var conflictIndex = -1
    var conflictTerm = -1

    if(term >= currentTerm)
      {

        currentLeader = sender()
        myState = FOLLOWER
        currentTerm = term

        // leaderLogEntries size = 0 indicates heartbeat
        if( leaderLogEntries.size == 0)
          {
            success = true
          }
        else {
          var data = getLogEntryData(print = false)
          var loglength = data.size - 1

          logger(s"My Data Size ${loglength}")

          logger(s" prevLogIndex= ${prevLogIndex},  loglength= ${loglength} prevLogIndex > loglength? ${prevLogIndex > loglength}")

          // let the leader know of my missing entries by sending my datalog length
          if (prevLogIndex > loglength) {
            conflictIndex = loglength
            alreadyReplied = true
            sender ! APPEND_ENTRIES_LOG_Inconsistency(currentTerm, conflictIndex, -1, false)
          }
          else {

            myPrevLogIndex = loglength
            myPrevLogTerm = if (loglength >= 0) data(loglength).term else -1

            logger(s"myPrevLogIndex = ${myPrevLogIndex}, myPrevLogTerm= ${myPrevLogTerm} prevLogTerm=${prevLogTerm}")
            var process = true

            // if myprevLogterm and leader's  previous log term, then travel down my log to get the index and term of the entry for which my previous log term does not match
            if (myPrevLogIndex > 0 && myPrevLogTerm != prevLogTerm) {
              conflictIndex = prevLogIndex

              var iCount = prevLogIndex
              while (process && iCount >= 0) {

                iCount -= 1

                if (data(iCount).term != myPrevLogTerm) {
                  process = false
                }
                if (process) {
                  conflictIndex = iCount
                }

                if (iCount < 0)
                  process = false

              }
              conflictTerm = myPrevLogTerm

              logger(s"currentTerm ${currentTerm} conflictIndex=${conflictIndex} myPrevLogTerm=${myPrevLogTerm}")

              alreadyReplied = true
              sender ! APPEND_ENTRIES_LOG_Inconsistency(currentTerm, conflictIndex, myPrevLogTerm, false)

            }
            else {

              logger(s"Already Replied?  ${alreadyReplied}")
              logger(s"conflictTerm ${conflictTerm} conflictIndex=${conflictIndex} ")

              logger(s"leaderLogEntries.size ${leaderLogEntries.size}")

              var index = -1
              var count = 0

              if (leaderLogEntries.size == 0)
                process = false
              else
                process = true

              //add the additional entry from leader's log
              while (process) {

                var logEntry = leaderLogEntries(count)

                index = prevLogIndex + count + 1
                count += 1
                if (((index >= loglength) || logEntry.term != data(index).term)) {
                  myLogger ! ADD_Entries(logEntry)
                  process = false
                }
                if (count >= leaderLogEntries.size)
                  process = false

              }

              logger(s"1.  leaderCommitIndex ${leaderCommitIndex} mycommitIndex ${commitIndex}")

              if (leaderCommitIndex > commitIndex) {
                data = getLogEntryData()
                loglength = data.size - 1

                //my previous commit index
                commitIndex = if (loglength > 0) loglength - 1 else -1

                if (commitIndex < 0 || commitIndex > leaderCommitIndex) {
                  commitIndex = leaderCommitIndex
                }


              }
              logger(s"2.  leaderCommitIndex ${leaderCommitIndex} mycommitIndex ${commitIndex}")

              logger(s" commitIndex ${commitIndex} lastApplied ${lastApplied} commitIndex > lastApplied ${commitIndex > lastApplied}")

              var iCount = 0
              //commit pending entries
              if (commitIndex > lastApplied && leaderLogEntries.size > 0) {
                var i = lastApplied + 1
                process = true
                while (process && leaderLogEntries.size > 1) {
                  lastApplied = i

                  logger(s" iCount= ${iCount}  ")
                  logger(s" leaderLogEntries ${leaderLogEntries}")

                  logger(s"leaderLogEntries(iCount) ${leaderLogEntries(iCount)}")

                  addToLocalLog(leaderLogEntries(iCount).data)
                  statMachine(leaderLogEntries(iCount).data, skipCommitIndexIncreament = true)
                  iCount += 1
                  if (i > commitIndex || iCount >= leaderLogEntries.size - 1)
                    process = false
                }

              }

              if (leaderLogEntries.size > 0) {
                  tempTopic = leaderLogEntries(leaderLogEntries.size - 1).toString
//                  /mediator ! Subscribe(tempTopic,self)
            }
              success = true
              //alreadyReplied = true
              //sender ! APPEND_ENTRIES_LOG_Inconsistency(currentTerm,-1,-1,true)
            }
          }
        }
      }
    else
      {
        success = false
      }

    DEBUG=false

  if(!alreadyReplied)
     sender ! RESULT_APPEND_ENTRIES(currentTerm,success)


  }



  def initIndexes() = {

    seenCommands = mutable.Set.empty[Command]

    var data = getLogEntryData()

    data.foreach((logEntry:LogEntry) =>
    {
      seenCommands +=((logEntry.data))
    })
    activeNodes.foreach((add:Address) =>
    {
      nextIndex += ((add,data.length))
      matchIndex += ((add,-1))

    })

    pendingCommands = ListBuffer[Command_Message]()
    processingCommand = ListBuffer[Command_Message]()

  }



  def decideAndVote(sender:ActorRef, candidateId:String,candidate_Term:Int,lastLogIndex:Int,lastLogTerm:Int) = {
    var myDecision = false

    if(myVoteRegistry.contains(candidate_Term))
    {

      println(s"I already voted to ${myVoteRegistry.get(candidate_Term)} for the term=${candidate_Term}")
      if(iReLeaderElectionCount>5)
        {
          println("Sleeping briefly")
          //Thread.sleep(generator.nextInt(10))
        }
      myDecision = false
    }
    else
    {

      if(currentTerm<candidate_Term)
      {
        myDecision = true
        currentTerm = candidate_Term
        myState = FOLLOWER
      }
      var data = getLogEntryData()
      var logLength  = data.size - 1
      myLastLogIndex = logLength
      myLastLogTerm = -1

      //get my last log term
      if(logLength>0){
        myLastLogTerm = data(myLastLogIndex).term
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

    }
    logger(s"\n Received vote request from ${sender} for term $candidate_Term and myDecision ${myDecision}")
    resetTimer()
    sender !  RESULT_APPEND_ENTRIES(currentTerm,myDecision)
  }

  def addToLocalLog(cmd:Command): Unit ={
    var data =    getLogEntryData(print=true)
    if(!seenCommands.contains(cmd))
      myLogger ! ADD_Entries(LogEntry(term = currentTerm,data=cmd))
  }
  def getLogEntryData(print:Boolean = false) : ListBuffer[LogEntry] = {

    implicit val timeout = Timeout(5 seconds)
    val future2 = ask(myLogger, Get_Entries).mapTo[ListBuffer[LogEntry]]
    val data = Await.result(future2, timeout.duration)

    if(print)
      logger(s"\n\n My Log entries ${data} \n\n")

    data
  }

  var iPrintCounter=0

  def sendAliveMessage() = {
    if(myState == LEADER) {

      resetTimer()

      votedNodes = Set.empty[ActorRef]

      if(pendingCommands.size > 0 && processingCommand.size ==0) {
        processingCommand += (pendingCommands(0))
        pendingCommands.remove(0)

      }
      //get the  CMD message from prcoessing queue
      var newCmd:Command = if(processingCommand.size > 0 )processingCommand(0).clientCommand else EMPTY_COMMAND

        //heartbeat message
      var msg = APPEND_ENTRIES(currentTerm,emptyLogEntry,new ListBuffer[LogEntry],commitIndex)

      //check if the message is not the empty message (indicates heartbeat) and add the client command to the appendEntry msg
      if(newCmd!=EMPTY_COMMAND)
        {
          println(s" Sender ${sender} ~~~ Self ${self} ==> Try to persist the data in log file ")

          println(" Data persisted in the log ")

          logger(s"processingCommand ${processingCommand}")

          var data =    getLogEntryData(print=false)
          var logSize = data.size

          var prevLogEntry = if(logSize >1 ) data(logSize - 2) else emptyLogEntry
          var currentDataSet = ListBuffer(data(logSize -1))

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
        logger("Relinquishing my leadership.. pausing")
        iRelinquishLeaderCount = 10

        //timers.startSingleTimer("Relinquish",TIMER_UP,Duration(induceSleepValue,TimeUnit.MILLISECONDS))
        resetTimer()
      }
      else {
        logger(s"\n sending Hearbeat for term $currentTerm")
        mediator ! Publish(topic,msg)
        iRelinquishLeaderCount -= 1
      }

    }
    else {
      logger(s"\n I am not the leader for term $currentTerm");

    }




  }

  def resetTimer() = {

    if(activeNodes.size>3) {
      if (timerActor != null && !timerActor.path.name.contains("dead")) {
        //println(s" timer actor name ${timerActor.path.name}")
        context.unwatch(timerActor)
        context.stop(timerActor)
        timerActor = null;
      }

      createTimerActor()

      if (currentTerm >= maxTerm)
        mediator ! Publish(topic, PoisonPill)
    }
    else {
      IS_TIMER_IN_PROGRESS = false
      print("\n\n Cluster has only 3 nodes.. need more than 3 nodes for RAFT to work correctly..")
    }
  }

  def createTimerActor(START_SENDING_TIMER:Boolean =true): Unit =
  {
    IS_TIMER_IN_PROGRESS = false

    timerActor = context.actorOf(Props(classOf[RAFT_Timer],myState), name="timer"+iTimerCount.incrementAndGet())
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
  def statMachine(command: Command, skipCommitIndexIncreament:Boolean=false):Boolean = {
    var commited = false

    lastApplied = commitIndex

    if(!skipCommitIndexIncreament)
     commitIndex +=1

    commited = writeOutputToFile(self.path.name,LogEntry(currentTerm,commitIndex,command).toString,true)


    if(!commited)
      {
        //error saving data to the state machine so revert back the commit index

        commitIndex -=1
        lastApplied = lastApplied - (if(commitIndex == 0) 0 else 1)
        println(s" ??????????? ${command} could not be saved to the state machine ????????????????")
      }else{
      println(s" ************** ${command} is successfully added to the state machine **************  ")
    }
    commited
  }
  // write the data to file
  def writeOutputToFile(actorName:String, data:String,stateChange:Boolean = false): Boolean =
  {
    var fw:FileWriter = null;
    var success = false;
    var fileName = if(stateChange) s"RAFT_${actorName}_StateMachine.txt" else "RAFT_Leader_Election.txt"
    try {
      fw = new FileWriter(s"${fileName}", true);
      fw.write(data+"\n");
      logger(s"writing $data in file ")

      success=true
    }
    catch
      {
        case e:Exception => {
          print(s"Error creating/writing to the file RAFT_Leader_Election.txt. Error Message = ${e.getMessage}")
          success = false;
        }
      }finally {
      if(fw!=null)
        fw.close();
    }
    success
  }


  override val supervisorStrategy = OneForOneStrategy() {
    case _: IllegalArgumentException     => SupervisorStrategy.Resume
    case _: ActorInitializationException => SupervisorStrategy.Stop
    case _: DeathPactException           => SupervisorStrategy.Stop
    case _: Exception                    => SupervisorStrategy.Resume
  }

  var currentTerm:Int=0
  var currentLeader = Option.empty[ActorRef].orNull

  //index of highest log entry known to be committed (initialized to 0, increases monotonically)
  var commitIndex = -1

  //index of highest log entry applied to state machine (initialized to 0, increases monotonically)
  var lastApplied = -1

  var conflictTerm = -1
  var myLastConflictTerm = -1
  var conflictIndex = -1
  var prevLogIndex = -1

  var myPrevLogIndex = -1
  var myPrevLogTerm = -1

  var nextIndex = HashMap[Address,Int]()
  var matchIndex = HashMap[Address,Int]()


  var myLastLogIndex:Int = -1
  var myLastLogTerm:Int = -1
  //join the cluster
  val cluster = Cluster(context.system)

  var timerActor:ActorRef = Option.empty[ActorRef].orNull
  var myState : STATE = FOLLOWER;


  var activeNodes = collection.SortedSet.empty[Address]
  var votedNodes = Set.empty[ActorRef]

  var processedCMD = HashSet[Command]()

  var IS_TIMER_IN_PROGRESS = false;
  var myVoteRegistry = HashMap[Int,Participant_VoteRecord]();

  var iTimerCount:AtomicInteger = new AtomicInteger(0)

  var iRelinquishLeaderCount = 10;
  val generator = new scala.util.Random

  val induceSleepValue = ConfigFactory.load("RAFT_CLUSTER").getInt("raft.timer.induceSleep")

  val maxTerm = 100

  createTimerActor(false);

  var celebrated_leadership = false

  var myLogger = context.actorOf(Props[LogRepository], name = "myLogger")
  //subscribing to the communication channel
  val mediator = DistributedPubSub(context.system).mediator

  var iReLeaderElectionCount=0

  val topic = "RAFT"
  var tempTopic=""
  mediator ! Subscribe(topic, self)
  //mediator ! Unsubscribe(topic,self)
  println(s"\n\n *** ${self.path} joined RAFT algorithm  ***  \n\n")

  val EMPTY_COMMAND = Command("RAFT_EMPTY")
  val emptyLogEntry = LogEntry(currentTerm,-1,EMPTY_COMMAND )



  var pendingCommands = ListBuffer[Command_Message]()
  var processingCommand = ListBuffer[Command_Message]()

  self ! INIT_TIMER();

  var DEBUG=false

  def logger(data:String,forceDisplay:Boolean=false) =
  {
    if(DEBUG || forceDisplay)
        println(s"\n ${self} ${myState} ${data} \n")
  }

  var seenCommands = mutable.Set.empty[Command]

}
