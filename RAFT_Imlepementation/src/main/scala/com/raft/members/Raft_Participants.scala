package com.raft.members

import java.io.FileWriter
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.language.postfixOps
import akka.actor.{Actor, ActorInitializationException, ActorLogging, ActorRef, Address, DeathPactException, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated, Timers}
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.pattern.ask
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberEvent, MemberRemoved, MemberUp}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe}
import akka.util.Timeout
import com.raft.util.{ADD_Entries, APPEND_ENTRIES, CANDIDATE, Command, ELECTION_TIMEOUT, FOLLOWER, Get_Entries, INIT_TIMER, LEADER, LogEntry, LogRepository, Participant_VoteRecord, RECEIVED_HEARTBEAT, RESULT_APPEND_ENTRIES, RemoveEntry, RequestVote, SEND_HEARTBEAT, STATE, TIMER_UP, Voted}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

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
          println("\n election timeout is already in progress")
        }

    case ELECTION_TIMEOUT(timeOut) => {

      celebrated_leadership = false

      println(s"\n The leader : ${currentLeader} is probably down and I reached election timeout ${timeOut} for term $currentTerm. Initiating election")

      votedNodes = Set.empty[Address]
      myState = CANDIDATE //change myself to candidate
      currentTerm +=1 //increase the term#
      votedNodes +=self.path.address //add my vote

      myVoteRegistry.addOne(currentTerm , Participant_VoteRecord(candidateID,true))

      mediator ! Publish(topic, RequestVote(candidateID,currentTerm,0,0))

    }

    case RequestVote(candidateId, term, lastLogIndex, lastLogTerm) =>


      if(sender!=self) {
        decideAndVote(sender, candidateId, term, lastLogIndex, lastLogTerm)
      }
    case vote:Voted => //process the received election vote from other participants
      if(vote.decision)
        votedNodes +=sender.path.address

      //get quorum of maximum votes
      if(votedNodes.size >= (1 + (activeNodes.size)/2)){

        currentLeader = self
        myState = LEADER

        if(!celebrated_leadership) {


          writeOutputToFile(self.toString(), s"${self.path.name.trim} is the new leader for term=${currentTerm} \t votedNodes ${votedNodes} \t activeNodes ${activeNodes}  \n ")

          println(s"\n\n \t ********** I am the leader  for term ${currentTerm}!!  ********** \n\n")
          println(s"\n\n votedNodes ${votedNodes} \n activeNodes ${activeNodes} \n\n")
          celebrated_leadership = true

          sendAliveMessage
        }


      }

      //received append Entries from leader
    case leaderEntry : APPEND_ENTRIES =>{

      resetTimer();

      if (sender() != self){
        println(s" *** ${self} Received append entry from leader ${currentLeader} - Entry => ${leaderEntry}")

        var appendEntryResult = handleAppendEntryRequest(leaderEntry)

        //print(" Ready with the append_entry_result "+appendEntryResult)

        sender ! appendEntryResult
      }
    }


      //received response from the followers
    case result:RESULT_APPEND_ENTRIES => {

      if (result.decision && myState!=FOLLOWER)
        {
        votedNodes += sender.path.address

      //get quorum of maximum votes
      if (votedNodes.size >= (1 + (activeNodes.size) / 2)) {

        currentLeader = self
        myState = LEADER



        if (!celebrated_leadership) {
          var leaderElectionSet = mutable.HashSet[String]()
          if(leaderElectionSet.add(s"${self.path.name} is the new leader for term=${currentTerm} \t votedNodes ${votedNodes} \t activeNodes ${activeNodes}  \n ")) {
            writeOutputToFile(self.toString(), s"${self.path.name} is the new leader for term=${currentTerm} \t votedNodes ${votedNodes} \t activeNodes ${activeNodes}  \n ")

            println(s"\n\n \t ********** I am the leader  for term ${currentTerm}!!  ********** \n\n")
            println(s"\n\n votedNodes ${votedNodes} \n activeNodes ${activeNodes} \n\n")
          }
          celebrated_leadership = true
        }

        timers.startSingleTimer("Heartbeat",TIMER_UP,Duration(induceSleepValue,TimeUnit.MILLISECONDS))
        //sendAliveMessage
      }
      }else
        {
          if(result.term>currentTerm){
            myState = FOLLOWER
            currentTerm = result.term
          }
        }
    }

    case cmd:Command =>
    {
      println(s" \n\n &&&&&&&&&&&&&&&& Received Data ${cmd} from sender ${sender()} activeNodes.size= ${activeNodes.size} myState=${myState}")
      if (myState == LEADER && activeNodes.size>3)
      {
        processedCMD.clear()
        println(" Try to persist the data in log file ")
        persistData(cmd)

        println(" Data persisted in the log ")

        var data =    getLogEntryData(print=false)

        var msg = APPEND_ENTRIES(currentTerm,data(if(data.size >1 ) data.size - 1 else 0 ),data,commitIndex)
        println(s" Data=${data} \n Sending msg=${} to all the nodes...")

       mediator !Publish(topic,msg )

        sender() ! "OK"
      }
      else
      {
        print(s"Forwarding the command to leader ${currentLeader}")
        if(processedCMD.add(cmd)){
          sender() ! "TRY AGAIN!"
        }
        else {
          currentLeader.forward(cmd)
        }
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
        println( " SEND_HEARTBEAT : ${getLogEntryData(print = true)}")
        sendAliveMessage;
      }
//    case RECEIVED_HEARTBEAT(term:Int) =>
//      {
//        if(term<currentTerm) {
//          print(s"\n\n =========>> The leader ${sender} that send heartbeat does not have latest term <<========= \n\n ")
//        } else {
//          currentLeader = sender()
//          currentTerm = term
//
//          if (sender() != self)
//            print(s"\n leader is alive ${sender} for the term $term")
//
//          resetTimer();
//        }
//      }
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


    case "Hello" =>
      print(s"Received Hello from ${sender}")

    case msg:Object => //println(s"Default message $msg")

  }

  def handleAppendEntryRequest(appendEntryFromLeader: APPEND_ENTRIES) =
  {

    var success=false

    //print(s" appendEntryFromLeader ${appendEntryFromLeader} currentTerm ${currentTerm}")

    // The follower will reject the request if the leaders term is lower than the follower’s current term — there is a newer leader!
    if(appendEntryFromLeader.term >= currentTerm)
      {
        //println(s" appendEntryFromLeader.prevLogEntry ${appendEntryFromLeader.prevLogEntry} appendEntryFromLeader.data.size ${appendEntryFromLeader.data.size}")
        if(appendEntryFromLeader.prevLogEntry==null || appendEntryFromLeader.data.size == 0)
          {
            success = true
            currentLeader = sender()
          }
        else
          {
            var data = getLogEntryData(print=false)
            /**
             *
             * The request will also be rejected if the previous entry’s term doesn’t match with the term stored in the follower’s log
             */
            if(appendEntryFromLeader.prevLogEntry.termId < currentTerm)
            {
              println(" !!!!!!!!!!!! You are not the leader.. rejecting your log entry request")
              success = false
            }
            else if(data.size > appendEntryFromLeader.prevLogEntry.currentIndex)
            {
              println(" !!!!!!!!!!! Your entries dont match")
              success = false
            }
            else if(data.size>0 && data(data.size -1 )==appendEntryFromLeader.prevLogEntry){
              println(" !!!!!!!!!!!!!!! Leader's previous log entry does not match with my latest entry.. Rejecting it");
              success = false
            }
            else {
              println(s" !!!!!!!!!!!!!! 1. currentTerm ${currentTerm} appendEntryFromLeader.term ${appendEntryFromLeader.term}")
              if (currentTerm < appendEntryFromLeader.term) {
                currentTerm = appendEntryFromLeader.term
                myState = FOLLOWER
                currentLeader = sender()

                if (data.size > appendEntryFromLeader.prevLogEntry.currentIndex + 1) {
                  if (data(appendEntryFromLeader.prevLogEntry.currentIndex).termId != appendEntryFromLeader.term) {
                    // If existing entry conflicts with new entry
                    // Delete and all that follow it
                    myLogger ! RemoveEntry(appendEntryFromLeader.prevLogEntry.currentIndex)
                    data = getLogEntryData(false)
                  }
                }

                // Set commit index to the min of the leader's commit index and index of last new entry
                if (appendEntryFromLeader.leaderCommitIndex > commitIndex) {
                  lastApplied = commitIndex

                  commitIndex = appendEntryFromLeader.leaderCommitIndex
                }


                //persist the last entry from Leader's log
                persistData(appendEntryFromLeader.data(appendEntryFromLeader.data.size - 1).data)
              }
              println(s" !!!!!!!!!!!!!! 2. currentTerm ${currentTerm} appendEntryFromLeader.term ${appendEntryFromLeader.term}")
              success=true
            }
          }
      }
    else
      {
        success = false
      }


     RESULT_APPEND_ENTRIES(currentTerm,success)


  }



  def decideAndVote(sender:ActorRef, candidateId:String,candidate_Term:Int,lastLogIndex:Int,lastLogTerm:Int) = {
    var myDecision = false

    resetTimer()

    if(myVoteRegistry.contains(candidate_Term))
    {

      println(s"I already voted to ${myVoteRegistry.get(candidate_Term)} for the term=${candidate_Term}")
      if(iReLeaderElectionCount>5)
        {
          println("Sleeping briefly")
          Thread.sleep(generator.nextInt(1000))
        }
      myDecision = false
    }
    else
    {

      if(currentTerm<candidate_Term && lastLogIndex>=commitIndex)
      {
        myDecision = true
        //currentTerm = candidate_Term
        myState = FOLLOWER
      }
      else
      {
        myDecision = false
      }

      myVoteRegistry.addOne(candidate_Term , Participant_VoteRecord(candidateId,myDecision))
      //sender ! Voted(myDecision)

    }
    println(s"\n Received vote request from ${sender} for term $candidate_Term and myDecision ${myDecision}")
    sender !  RESULT_APPEND_ENTRIES(currentTerm,myDecision)
  }

  def persistData(cmd:Command): Unit ={
    var data =    getLogEntryData(print=false)
    lastApplied = data.size

    myLogger ! ADD_Entries(LogEntry(termId = currentTerm,data=cmd))

    commitIndex = lastApplied + 1
  }
  def getLogEntryData(print:Boolean) : ListBuffer[LogEntry] = {

    implicit val timeout = Timeout(5 seconds)
    val future2 = ask(myLogger, Get_Entries).mapTo[ListBuffer[LogEntry]]
    val data = Await.result(future2, timeout.duration)

    if(print)
      println(s"\n\n My Log entries ${data} \n\n")

    data
  }

  var iPrintCounter=0

  def sendAliveMessage() = {
    if(myState == LEADER) {

      resetTimer()

      votedNodes = Set.empty[Address]

      if(iRelinquishLeaderCount < 0)
      {
        println("Relinquishing my leadership.. pausing")
        iRelinquishLeaderCount = 10
        timers.startSingleTimer("Relinquish",TIMER_UP,Duration(induceSleepValue,TimeUnit.MILLISECONDS))
      }
      else {

        println(s"\n sending Hearbeat for term $currentTerm")
        mediator ! Publish(topic,APPEND_ENTRIES(currentTerm,null,new ListBuffer[LogEntry],commitIndex))
        iRelinquishLeaderCount -= 1
      }

    }
    else {
      println(s"\n I am not the leader for term $currentTerm");

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
  // write the data to file
  def writeOutputToFile(actorName:String, data:String): Unit =
  {
    var fw:FileWriter = null;
    try {
      fw = new FileWriter(s"RAFT_Leader_Election.txt", true);
      fw.write(data+"\n");

    }
    catch
      {
        case e:Exception => {
          print(s"Error creating/writing to the file RAFT_Leader_Election.txt. Error Message = ${e.getMessage}")
        }
      }finally {
      if(fw!=null)
        fw.close();
    }

    println(s"writing $data in file ")

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
  var commitIndex = 0

  //index of highest log entry applied to state machine (initialized to 0, increases monotonically)
  var lastApplied = 0


  //join the cluster
  val cluster = Cluster(context.system)

  var timerActor:ActorRef = Option.empty[ActorRef].orNull
  var myState : STATE = FOLLOWER;


  var activeNodes = Set.empty[Address]
  var votedNodes = Set.empty[Address]

  var processedCMD = mutable.HashSet[Command]()

  var IS_TIMER_IN_PROGRESS = false;
  var myVoteRegistry = new mutable.HashMap[Int,Participant_VoteRecord]();

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
  mediator ! Subscribe(topic, self)
  println(s"\n\n *** ${self.path} joined RAFT algorithm  ***  \n\n")

  self ! INIT_TIMER();
}
