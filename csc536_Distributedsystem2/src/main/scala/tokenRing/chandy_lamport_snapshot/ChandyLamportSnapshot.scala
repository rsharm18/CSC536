package tokenRing.chandy_lamport_snapshot


import akka.actor.{Actor, ActorRef, ActorSystem, Props}

import scala.collection.mutable

//assigns the neighbors of the current process
case class Start(left: ActorRef, right: ActorRef)

//flows clockwise
case class PING()

//flows anti clockwise
case class PONG()

//indicates the beginning of the token(PING/PONG) transfer
case class INIT_TOKEN_TRANSFER()

//indicates the marker
case class Marker_Message()

//indicates beginning of the snapshot
case class INIT_SNAPSHOT()

//snapshot object to get the consolidated snapshot
object Snapshot_Info_Provider {
  private val snapshotQueue = new mutable.Queue[String]()
  var numberOfProcessPendingSnapshot=3
  def captureSnapshot(msg: String): Unit = {
  Thread.sleep(200)
  snapshotQueue.enqueue("\n"+msg.replaceAllLiterally("\t",""))
  }

  def printSnapshot = {

    while(snapshotQueue.size != numberOfProcessPendingSnapshot)
      {
        println(s" \n\n									Number of processes captured snapshot ${snapshotQueue.size} PENDING = ${numberOfProcessPendingSnapshot - snapshotQueue.size}\n\n ")
        Thread.sleep(300)
      }

        var strBuild = new mutable.StringBuilder();
        strBuild.append("\n\n\n ********************************************** Global SNAPSHOT CAPTURED :: START **********************************************")
        snapshotQueue.foreach((data:String)=>strBuild.append(data))
        strBuild.append("\n\n\n ********************************************** Global SNAPSHOT CAPTURED :: END **********************************************")
        print(s"${strBuild}")
      }
}

class TokenRingChandyLamport extends Actor {

  //message sleep timer
  val PONG_SLEEPTIME=500
  val PING_SLEEPTIME =1000

  var numberOf_PING_MsgsRcvdCounter = 0;
  var numberOf_PONG_MsgsRcvdCounter = 0;

  var myMarkerCapturedState = "";

  var myClockwiseNeighbour: ActorRef = null;
  var myAntiClockwiseNeighbour: ActorRef = null;

  //map used to keep track of the responses received from the all incoming channels
  val neighbor_marker_response_map = new mutable.HashMap[String, Boolean]();

  //queue to keep track of the messages in transit after the marker is received
  var messagesInTransitQueue = new mutable.Queue[String]();

  //Map to keep track of the messages in transit per sender after the marker is received
  val messagesInTransitMap = new mutable.HashMap[ActorRef,mutable.Queue[String]]();


  // boolean flag to keep track if the recording is in progress
  var RECORDING_INBOUND_CHANNELS: Boolean = false

  def receive = {

    case Marker_Message => //marker received

      var strBuilder: mutable.StringBuilder = new mutable.StringBuilder()

      /**
       * received the marker.
       * if not received (RECORDING_INBOUND_CHANNELS = false) already then capture the state and send the marker to all outbounds and close the channel between the sender and the receiver (neighbor_marker_response_map(sender) = true)
       * if the marker is already received
       * then set the ack received for the sender to true and stop listening to that sender for marker
       *
       * if ack received from all the inbound channels (pendingAck = false) then that is the end of the snapshot for that process/node
       *
       */


      if (!RECORDING_INBOUND_CHANNELS) {
        captureSnapshot() //capture the snapshot and start recording
        sendMarkerToAllOutbounds() //send the marker to all outbounds
      }
      else //already recording is in progress and sent the marker out and now recording ack from the neighbors/inbound
      {
        strBuilder.append(s" \n \t\t\t\t\t\t\t\t\t\t\t\t\t\t ${myName} Received marker ACK from ${senderName()}, ")
      }

      //capture the ack from the sender
      neighbor_marker_response_map put(senderName(), true) //close the channel between the sender and the receiver

      var rcvdAllLocks = receivedAllAcks

      //if received ack from the all inbound channels then stop recording
      if (rcvdAllLocks) {
        strBuilder.append(s"\n${addTabs} receivedAllAcks?= ${rcvdAllLocks} \n ")

        //stop recording
        RECORDING_INBOUND_CHANNELS = false

        print(s"${strBuilder}")

        //display the snapshot
        displaySnapshot()
      }
      else { //pending ack for some inbound channels
        strBuilder.append(s" ${addTabs} pending ack from ${neighbor_marker_response_map.filter(x => x._2 == false).keys} \n")
        print(s"${strBuilder}")
      }
    case PING =>

      if (!sender.equals(self)) {
        increasePINGCounter

        if (RECORDING_INBOUND_CHANNELS) {

          //check if we already received the marker recording from sender to me. If yes then it wont be part of the snapshot
          if (!neighbor_marker_response_map.get(senderName()).get) {

            messagesInTransitQueue = messagesInTransitMap.get(sender()) getOrElse( new mutable.Queue[String]() )
            messagesInTransitQueue enqueue (s" PING is in transit from ${senderName()} -> ${self.path.name} and # of Pings at ${self.path.name}  ${numberOf_PING_MsgsRcvdCounter}")

            messagesInTransitMap put(sender,messagesInTransitQueue);
          }
          else {
            print(s"PING NOT Recorded as the recording on the Channel ${senderName()} -> ${myName} is already closed")
          }
        }
        println(s"\nPING ${senderName()} -> ${self.path.name}  # of Pings at ${self.path.name}  ${numberOf_PING_MsgsRcvdCounter}")
        Thread.sleep(PING_SLEEPTIME)

      }
      myClockwiseNeighbour ! PING

    case PONG =>

      if (!sender.equals(self)) {
        increasePONGCounter

        if (RECORDING_INBOUND_CHANNELS) {
          //check if we already received the marker recording from sender to me. If yes then it wont be part of the snapshot
          if (!neighbor_marker_response_map.get(senderName()).get) {

            messagesInTransitQueue = messagesInTransitMap.get(sender()) getOrElse( new mutable.Queue[String]() )
            messagesInTransitQueue enqueue (s" PONG is in transit from ${senderName()} -> ${self.path.name} and # of Pongs at ${self.path.name}  ${numberOf_PONG_MsgsRcvdCounter}")
            messagesInTransitMap put (sender,messagesInTransitQueue);

          }
          else {
            print(s"PONG NOT Recorded as the recording on the Channel ${senderName()} -> ${myName} is already closed")
          }
        }

        println(s"\n                        PONG ${senderName()} -> ${self.path.name}  # of Pongs at ${self.path.name}  ${numberOf_PONG_MsgsRcvdCounter}")

        Thread.sleep(PONG_SLEEPTIME)
      }

      myAntiClockwiseNeighbour ! PONG

    case Start(clockWiseActor, antiClockWiseActor) =>

      myClockwiseNeighbour = clockWiseActor;
      myAntiClockwiseNeighbour = antiClockWiseActor;

      println(s"I am ${self.path.name} and myClockwiseNeighbour ${getNeighborName(myClockwiseNeighbour)} myAntiClockwiseNeighbour : ${getNeighborName(myAntiClockwiseNeighbour)}")

      //initialize the  Neighbor marker Response map and  count of PING & PONG MESSAGES
      init();

    case INIT_TOKEN_TRANSFER =>

      increasePINGCounter
      increasePONGCounter

      println(s"'${self.path.name}' initiates the token with initial # of PINGs=${numberOf_PING_MsgsRcvdCounter} and # of PONGs=${numberOf_PONG_MsgsRcvdCounter}")

      var rightNode = myClockwiseNeighbour
      rightNode ! PING

      var leftNode = myAntiClockwiseNeighbour
      leftNode ! PONG

    case INIT_SNAPSHOT =>
      captureSnapshot()
      sendMarkerToAllOutbounds()
  }

  /**
   * initializes the member variables
   */
  def init(): Unit = {
    // print(s"Adding ${myClockwiseNeighbour} and ${myAntiClockwiseNeighbour} to map ")
    neighbor_marker_response_map put(getNeighborName(myClockwiseNeighbour), false)
    neighbor_marker_response_map put(getNeighborName(myAntiClockwiseNeighbour), false)

    numberOf_PING_MsgsRcvdCounter = 0;
    numberOf_PONG_MsgsRcvdCounter = 0;
  }

  /**
   * sends the marker to all outbound channels
   */
  def sendMarkerToAllOutbounds(): Unit = {
    var strBuilder: mutable.StringBuilder = new mutable.StringBuilder()
    //send the marker to all neighbors  : Start
    var leftNode = myAntiClockwiseNeighbour
    var rightNode = myClockwiseNeighbour

    //strBuilder.append(s"\n${addTabs} ${self.path.name}'s has the marker and its captured state is :: ${myState}")
    strBuilder.append(s"\n${addTabs} ${self.path.name} is sending markers -> ${getNeighborName(myClockwiseNeighbour)} and ${getNeighborName(myAntiClockwiseNeighbour)}")

    println(s"${strBuilder}")

    rightNode ! Marker_Message
    leftNode ! Marker_Message

    //send the marker to all neighbors  : End
  }

  /**
   * increases the ping counter
   */
  def increasePINGCounter = {
    numberOf_PING_MsgsRcvdCounter = numberOf_PING_MsgsRcvdCounter + 1
  }

  /**
   * increases the pong counter
   */
  def increasePONGCounter = {
    numberOf_PONG_MsgsRcvdCounter = numberOf_PONG_MsgsRcvdCounter + 1
  }

  /**
   * returns the sender name
   *
   * @return
   */
  def senderName(): String = {

    var strSender = sender.path.toString.split("/")
    strSender(strSender.length - 1)
  }

  /**
   * captures the current state, sets RECORDING_INBOUND_CHANNELS=true indicating the start of the recording
   */
  def captureSnapshot() = {
    RECORDING_INBOUND_CHANNELS = true
    //capture the state
    myMarkerCapturedState = s" ${self.path.name} :: (PING = " + numberOf_PING_MsgsRcvdCounter + ", PONG = " + numberOf_PONG_MsgsRcvdCounter + ")"
    if (sender != null && !senderName().equals("deadLetters"))
      println(s"\n ${addTabs} ${myName} Received Marker from ${senderName()}- RECORDING_INBOUND_CHANNELS=${RECORDING_INBOUND_CHANNELS}. Capture State = ${myMarkerCapturedState}")
    else {
      println(s"\n ${addTabs} ${myName} has the marker and RECORDING_INBOUND_CHANNELS=${RECORDING_INBOUND_CHANNELS} . Capture State = ${myMarkerCapturedState}")
    }
  }

  /**
   * retuns the name of the current actor
   *
   * @return
   */
  def myName: String = {
    self.path.name
  }

  /**
   * returns the name of a given neighbor path
   *
   * @param neighborPath
   * @return
   */
  def getNeighborName(neighborPath: ActorRef): String = {

    var strSender = neighborPath.path.name.split("/")
    strSender(strSender.length - 1)
  }

  /**
   * retuns if the  current process received marker ack from all of its inbound
   *
   * @return boolean
   */
  def receivedAllAcks: Boolean = {
    neighbor_marker_response_map.filter(x => x._2 == false).isEmpty
  }

  /**
   * displays the snapshot of the curretn process
   */
  def displaySnapshot(): Unit = {

    var strBuilder: mutable.StringBuilder = new mutable.StringBuilder()

    strBuilder
      .append(s"\n ${addTabs}**** Snapshot Message for ${myName} : START ****")
      .append(s"\n${addTabs} Captured STATE for ${myMarkerCapturedState}")
      .append(getMessagesInTransit())
      .append(s"\n${addTabs}**** Snapshot Message for ${myName} : END **** ")

    Snapshot_Info_Provider.captureSnapshot(strBuilder.toString())
    println(s"${strBuilder}")
  }

  /**
   * retruns the list of messages in transit from the queue (messagesInTransitQueue)
   *
   * @return String
   */
  def getMessagesInTransit() = {
    var strBuilder: mutable.StringBuilder = new mutable.StringBuilder()

    strBuilder.append(if (messagesInTransitQueue.size == 0) "\n \t  \t\t\t\t\t\t\t\t\t  \t\t\t\t\t ** NO MESSAGE IS IN TRANSIT ** " else "")

    for ((k:ActorRef,v:mutable.Queue[String]) <- messagesInTransitMap) {
      strBuilder.append(s"\n ${addTabs} TRASIT FROM ${k} \n ${addTabs}\t    Messages :  ${v}\n")
    }
    strBuilder.toString()

  }

  def addTabs:String = {
    "\t\t\t\t\t\t\t\t\t"
  }
}

//ping flows clockwise and pong goes anticlockwise
object ChandyLamportSnapshot  {

  def initChandyLamport() = {

    val system = ActorSystem("TokenRingChandyLamport")

    // create 3 actors
    val actor1 = system.actorOf(Props[TokenRingChandyLamport], name = "P")
    val actor2 = system.actorOf(Props[TokenRingChandyLamport], name = "Q")
    val actor3 = system.actorOf(Props[TokenRingChandyLamport], name = "R")

    println(s"          ${actor1.path.name}")
    println("       /       \\ ")
    println("      /         \\")
    println("     /           \\")
    println("    /             \\")
    println(s"   ${actor3.path.name} - - - - - - - ${actor2.path.name}")

    //assign neigbors of each actor
    actor1 ! Start(actor2, actor3)
    actor2 ! Start(actor3, actor1)
    actor3 ! Start(actor1, actor2)

    // assign the number of actors participating in the snapshot - used for consolidated snapshot
    Snapshot_Info_Provider.numberOfProcessPendingSnapshot = 3

    Thread.sleep(50)

    //initialize token (PING/PONG) transfer
    println("\n Server ready..Calling init Token")
    actor1 ! INIT_TOKEN_TRANSFER

    Thread.sleep(3500)

    //initialize the snapshot algorithm
    println(" 			  									*** R will be initiating the snapshot algorithm ! ")
    actor3 ! INIT_SNAPSHOT

    var break = false

    //print global snapshot
    Snapshot_Info_Provider.printSnapshot
  }
}
