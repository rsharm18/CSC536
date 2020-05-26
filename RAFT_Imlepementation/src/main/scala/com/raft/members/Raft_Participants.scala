package com.raft.members

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorLogging, ActorRef, Address, Props, Terminated, Timers}
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberEvent, MemberRemoved, MemberUp}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe}
import com.raft.util.{APPEND_ENTRIES, CANDIDATE, ELECTION_TIMEOUT, FOLLOWER, INIT_TIMER, LEADER, Participant_VoteRecord, RECEIVED_HEARTBEAT, RequestVote, SEND_HEARTBEAT, STATE, TIMER_UP, Voted}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.concurrent.duration.Duration

class Raft_Participants extends Actor with ActorLogging with Timers{



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
      println(s"\n The leader : ${currentLeader} is probably down and I reached election timeout ${timeOut} for term $currentTerm. Initiating election")

      votedNodes = Set.empty[Address]
      myState = CANDIDATE //change myself to candidate
      currentTerm +=1 //increase the term#
      votedNodes +=self.path.address //add my vote

      mediator ! Publish(topic, RequestVote(candidateID,currentTerm,0,0))

    }

    case RequestVote(candidateId, term, lastLogIndex, lastLogTerm) =>
      println(s"\n Received vote request from ${sender} for term $term")

      if(sender!=self) {
        decideAndVote(sender, candidateId, term, lastLogIndex, lastLogTerm)
      }
    case Voted(decision) => //process the received vote from other participants
      if(decision)
        votedNodes +=sender.path.address

      //get quorum of maximum votes
      if(votedNodes.size >= (1 + (activeNodes.size)/2)){

        myState = LEADER
        sendAliveMessage
        println(s"\n\n \t ********** I am the leader  for term ${currentTerm}!!  ********** \n\n")
        println(s"\n\n votedNodes ${votedNodes} \n activeNodes ${activeNodes} \n\n")


      }
case TIMER_UP =>
      {
        println("Resuming Again!!")
        resetTimer();
      }
    case SEND_HEARTBEAT =>
      {
        if(iRelinquishLeaderCount < 0)
          {
            println("Relinquishing my leadership.. pausing")
            iRelinquishLeaderCount = 20
            timers.startSingleTimer("Relinquish",TIMER_UP,Duration(minTimerValue,TimeUnit.MILLISECONDS))
          }
        else {
          sendAliveMessage;
          iRelinquishLeaderCount -= 1
        }
      }
    case RECEIVED_HEARTBEAT(term:Int) =>
      {
        currentLeader = sender()

        if(sender()!=self)
          print(s"\n leader is alive ${sender} for the term $term")

        resetTimer();
      }
    case state: CurrentClusterState =>
      println(s"\n\n ~~~ ${state.members.size} members joined \n\n");

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


  def sendAliveMessage() = {
    if(myState == LEADER) {
      mediator ! Publish(topic, RECEIVED_HEARTBEAT(currentTerm))
      println(s"\n sending Hearbeat for term $currentTerm")
    }
    else {
      println(s"\n I am not the leader for term $currentTerm");

    }

    resetTimer();

  }

  def resetTimer() = {
    if(timerActor!=null  && !timerActor.path.name.contains("dead")){
      //println(s" timer actor name ${timerActor.path.name}")
      context.unwatch(timerActor)
      context.stop(timerActor)
      timerActor = null;
    }

    createTimerActor()

  }

  def createTimerActor(START_SENDING_TIMER:Boolean =true): Unit =
  {
    IS_TIMER_IN_PROGRESS = false

    timerActor = context.actorOf(Props(classOf[RAFT_Timer],myState), name="timer"+iTimerCount.incrementAndGet())
    context.watchWith(timerActor, INIT_TIMER)
    //println(s" START_SENDING_TIMER ${START_SENDING_TIMER}")
   if(START_SENDING_TIMER)
    timerActor ! INIT_TIMER
  }

  def decideAndVote(sender:ActorRef, candidateId:Address,term:Int,lastLogIndex:Int,lastLogTerm:Int) = {
    var myDecision = false

    resetTimer()

    if(myVoteRegistry.contains(term))
    {
      println(s"I already voted to ${myVoteRegistry.get(term)} for the term=${term}")
    }
    else
    {

      if(currentTerm<=term)
      {
        myDecision = true
        currentTerm = term
        myState = FOLLOWER
      }
      else
      {
        myDecision = false
      }

      myVoteRegistry.addOne(term , Participant_VoteRecord(candidateId,myDecision))
      sender ! Voted(myDecision)


    }
  }

  var currentTerm:Int=0
  var currentLeader = Option.empty[ActorRef].orNull

  //join the cluster
  val cluster = Cluster(context.system)

  var timerActor:ActorRef = Option.empty[ActorRef].orNull
  var myState : STATE = FOLLOWER;


  var activeNodes = Set.empty[Address]
  var votedNodes = Set.empty[Address]

  var IS_TIMER_IN_PROGRESS = false;
  var myVoteRegistry = new mutable.HashMap[Int,Participant_VoteRecord]();

  //subscribing to the communication channel
  val mediator = DistributedPubSub(context.system).mediator
  val candidateID = self.path.address;

  val topic = "RAFT"
  mediator ! Subscribe(topic, self)
  println(s"$self joined RAFT algorithm")

  var iTimerCount:AtomicInteger = new AtomicInteger(0)

  var iRelinquishLeaderCount = 20;
  val minTimerValue = ConfigFactory.load().getInt("raft.timer.maxValue")

  createTimerActor(false);
  activeNodes +=self.path.address
  self ! INIT_TIMER();
}
