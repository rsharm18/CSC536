import java.io.FileWriter

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging

import scala.collection.mutable.{HashMap, ListBuffer}
import scala.compat.java8.converterImpl.AccumulateAnyArray

sealed trait GroupServiceAPI;
case class GroupMembership(members:ListBuffer[GroupServer])

case class JoinGroup(groupID:BigInt, newMember:GroupServer) extends GroupServiceAPI
case class LeaveGroup(groupID:BigInt) extends GroupServiceAPI
case class MultiCastMessage(groupID:BigInt)  extends GroupServiceAPI

case class ADD_TO_GROUP() extends GroupServiceAPI
case  class REMOVE_FROM_GROUP()  extends GroupServiceAPI
/**
 * GroupService is an example app service for the actor-based KVStore/KVClient.
 * This one stores GroupMembership objects in the KVStore.  Each app server allocates new
 * GroupMembership (ListBuffer[GroupServer]), writes them, and reads them randomly.
 *
 *
 * @param myNodeID sequence number of this actor/server in the app tier
 * @param numNodes total number of servers in the app tier
 * @param storeServers the ActorRefs of the KVStore servers
 * @param burstSize number of commands per burst
 */

class GroupServer (val myNodeID: Int, val numNodes: Int, storeServers: Seq[ActorRef], burstSize: Int) extends Actor {
  val generator = new scala.util.Random
  val cellstore = new KVClient(storeServers)
  val dirtycells = new HashMap[BigInt, Any]
  val localWeight: Int = 70
  val log = Logging(context.system, this)

  val maxWeight = 100

  val joinGroupWeight =  100 * 60/maxWeight //60% probablity of joining a new group
  val leaveGroupWeight =  100 * 15/maxWeight //15% probablity of leaving a group
  val multicastWeight =    100 * 25/maxWeight //25% probablity of multicast a message

  var stats = new Stats
  var allocated: Int = 0
  var endpoints: Option[Seq[ActorRef]] = None

  var append=false;
  var WRITETOFILE=true
  var parentGroupList:ListBuffer[BigInt] = ListBuffer()

   def getActor()= {
    self
  }
  def receive() = {
      case Prime() =>
        printToFile(s" My NodeID is ${myNodeID} \n")
        if(WRITETOFILE==true) {
          println(s"\n [$myNodeID] WRITETOFILE flag is true. This might lead to timeout. Please either set WRITETOFILE=false or adjust opsPerNode/burst size to avoid timeouts  ")
        }
        append=true;
        parentGroupList = ListBuffer()
        //Thread.sleep(50)
      case Command() =>
        statistics(sender)
        command
      case View(e) =>
        endpoints = Some(e)

      case MultiCastMessage(groupID:BigInt) =>
      {
        stats.received_multicasts +=1
        var members = getGroupMembers(groupID)
        printToFile(s" ${myNodeID} received multicast message from [$sender().path.name] group IS [$groupID]  \n")
        if(!members.contains(this))
          {
            printToFile(s" ${myNodeID} not a member of group:[$groupID] yet I received multicast message from ${sender()} \n")
            stats.received_multicast_errors += 1
          }

      }
  }

  private def command() = {
    val sample = generator.nextInt(maxWeight)

    //print(s"\n sample ${sample} leaveGroupWeight ${leaveGroupWeight} multicastWeight ${multicastWeight} joinGroupWeight ${joinGroupWeight} ")
    if(sample < leaveGroupWeight)
      leaveGroup
    else if (sample < multicastWeight)
      multiCastMessage
    else
        joinGroup
  }


  private def multiCastMessage = {

    stats.sent_multicasts += 1

    printToFile(s" ${myNodeID} multicasting to  groups ${parentGroupList} \n")
   // println(s" ${myNodeID} multicasting to  ${parentGroupList.length} groups : Start")
    if(parentGroupList.length>0){
      parentGroupList.map((grpID:BigInt) =>{
      var groupMembers = getGroupMembers(grpID)
        printToFile(s" ${myNodeID} multicasting to  grpID ${grpID}  ${groupMembers.length} members  \n")
        groupMembers.map((member:GroupServer) =>
        {
         //if(member.myNodeID!=this.myNodeID) {
           //println(s"\n member.getActor ${member.getActor}")
           member.getActor ! MultiCastMessage(grpID)
         //}
        })
      })
    }
   // println(s" ${myNodeID} multicasting to  ${parentGroupList.length} groups : End")
  }

//  def receiveMultiCast(multiCastData: String) = {
//    stats.multicastsReceived += 1
//    myPrint(s"received multicast for G$multiCastData")
//
//    // Check group membership for multi-cast just received
//    val groupID = BigInt(multiCastData)
//    val groupMembership0 = directRead(groupID)
//    if(groupMembership0.nonEmpty){
//      val groupMembership = groupMembership0.get
//      if(!groupMembership.membership.contains(this)){
//        // Received a multi-cast from a group I'm not a part of anymore. Do something?
//      }
//    }
//  }


  /**
   * update the stats - message count
   *
   * @param master
   */
  private def statistics(master: ActorRef) = {
    stats.messages += 1

    if (stats.messages >= burstSize) {
      //printToFile(s" myNodeID ${myNodeID} stats.messages ${stats.messages} burstSize ${burstSize}  \n")
      master ! BurstAck(myNodeID, stats)
      stats = new Stats
    }
  }

  /**
   * join a random group
   *
   * @return
   */
  private def joinGroup() = {
    stats.joinedNewGroup += 1

    val groupID = chooseParentGroup

    if(!parentGroupList.contains(groupID)) {
      var members: ListBuffer[GroupServer] = getGroupMembers(groupID)
      updateGroupMembers(groupID, members, this, ADD_TO_GROUP)
      printToFile(s" joined the group ${groupID}  \n")
    }

  }

  private def leaveGroup() = {
    stats.leaveGroup += 1
    if(parentGroupList.length > 0) {

      //get the group ID from the current parent group list
      val groupID = parentGroupList(generator.nextInt(parentGroupList.length))

      var members = getGroupMembers(groupID)
      if(members.contains(this)) {
        updateGroupMembers(groupID, members, this, REMOVE_FROM_GROUP)
        printToFile(s" Leaving the group ${groupID}  \n")
      }else
        {
          printToFile(s" I was told to leave the group ${groupID} but I ma not amember of it  \n")
        }
    }
    else
      {
        printToFile(" received leave group request but I dont have any membership  \n")
      }
  }

  /**
   * choose a new parent group
   * since group is named by an integer, hence using the a random BigInt to get a group
   *
   * @return
   */
  private def chooseParentGroup(): BigInt =
  {
    val joinCreateWeight = BigInt(generator.nextInt(numNodes))
    joinCreateWeight
  }

  /**
   * get the members of any given group
   *
   * @param groupID
   * @return
   */
  private def getGroupMembers(groupID:BigInt):ListBuffer[GroupServer] = {

    var members:ListBuffer[GroupServer] = new ListBuffer[GroupServer];

    var groupMembers= directRead(groupID)
    //println(s"\n groupMembers ${groupMembers} groupMembers != None ${groupMembers != None}")
    if(groupMembers.nonEmpty && groupMembers != None){

      members= groupMembers.get.members

    }
   // printToFile(s" group members for the group ${groupID} are ${members}  \n")
    members
  }

  /**
   * update group members (add/remove)
   *
   * @param groupId
   * @param members
   * @param node
   * @param operationType
   * @return
   */
  private def updateGroupMembers(groupId:BigInt,members:ListBuffer[GroupServer], node:GroupServer,operationType:Any)=
  {

    operationType match {
      case ADD_TO_GROUP =>
              members += node
              parentGroupList +=groupId
              directWrite(groupId,GroupMembership(members))
        printToFile(s" Added [$node.myNodeID] to the group [$groupId]  \n")
      case REMOVE_FROM_GROUP =>
              members -= node
              parentGroupList -=groupId
              directWrite(groupId,GroupMembership(members))
        printToFile(s" Removed [$node.myNodeID] from the group [$groupId]  \n")
    }
  }


  private def chooseActiveParent(): BigInt = {
    var chosenNodeID:Int =
      if (generator.nextInt(100) <= localWeight)
        myNodeID.intValue
      else
        generator.nextInt(numNodes - 1)

    chosenNodeID = chosenNodeID % parentGroupList.length
    val cellSeq = generator.nextInt(allocated)
    cellstore.hashForKey(parentGroupList(chosenNodeID).intValue, cellSeq)
  }

/*  private def rwcheck(key: BigInt, value: GroupCell) = {
    directWrite(key, value)
    val returned = read(key)
    if (returned.isEmpty)
      println("rwcheck failed: empty read")
    else if (returned.get.next != value.next)
      println("rwcheck failed: next match")
    else if (returned.get.prev != value.prev)
      println("rwcheck failed: prev match")
    else
      println("rwcheck succeeded")
  }

  private def read(key: BigInt): Option[GroupCell] = {
    val result = cellstore.read(key)
    if (result.isEmpty) None else
      Some(result.get.asInstanceOf[GroupCell])
  }

  private def write(key: BigInt, value: GroupCell, dirtyset: HashMap[BigInt, Any]): Option[GroupCell] = {
    val coercedMap: HashMap[BigInt, Any] = dirtyset.asInstanceOf[HashMap[BigInt, Any]]
    val result = cellstore.write(key, value, coercedMap)
    if (result.isEmpty) None else
      Some(result.get.asInstanceOf[GroupCell])
  }
 */

  private def directRead(key: BigInt): Option[GroupMembership] = {
    val result = cellstore.directRead(key)
    if (result.isEmpty) None else
      Some(result.get.asInstanceOf[GroupMembership])
  }

  private def directWrite(key: BigInt, value: GroupMembership): Option[GroupMembership] = {
    val result = cellstore.directWrite(key, value)
    if (result.isEmpty) None else
      Some(result.get.asInstanceOf[GroupMembership])
  }


  private def printToFile(message: String) = {
    if(WRITETOFILE){
        var fw:FileWriter = null;
        try {
          fw = new FileWriter(s"[$myNodeID].txt", append);
          fw.write(s"[$myNodeID] ${message} \n")
        }
        catch
          {
            case e:Exception => {
              print(s"Error creating/writing to the file [$myNodeID].txt. Error Message = ")
              e.printStackTrace()
            }
          }
        finally {
          if(fw!=null){
            fw.close()
          }
        }
      }

  }

/*
  private def push(dirtyset: HashMap[BigInt, Any]) = {
    cellstore.push(dirtyset)
  }
 */
}

object GroupServer {
  def props(myNodeID: Int, numNodes: Int, storeServers: Seq[ActorRef], burstSize: Int): Props = {
    Props(classOf[GroupServer], myNodeID, numNodes, storeServers, burstSize)
  }
}
