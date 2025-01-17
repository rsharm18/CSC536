import akka.actor.{Actor, ActorSystem, ActorRef, Props}
import akka.event.Logging

sealed trait LoadMasterAPI
case class Start(maxPerNode: Int) extends LoadMasterAPI
case class BurstAck(senderNodeID: Int, stats: Stats) extends LoadMasterAPI
case class Join() extends LoadMasterAPI

/** LoadMaster is a singleton actor that generates load for the app service tier, accepts acks from
  * the app tier for each command burst, and terminates the experiment when done.  It uses the incoming
  * acks to self-clock the flow of commands, to avoid swamping the mailbox queues in the app tier.
  * It also keeps running totals of various Stats returned by the app servers with each burst ack.
  * A listener may register via Join() to receive a message when the experiment is done.
  *
  * @param numNodes How many actors/servers in the app tier
  * @param servers ActorRefs for the actors/servers in the app tier
  * @param burstSize How many commands per burst
  */

class LoadMaster (val numNodes: Int, val servers: Seq[ActorRef], val burstSize: Int) extends Actor {
  val log = Logging(context.system, this)
  var active: Boolean = true
  var listener: Option[ActorRef] = None
  var nodesActive = numNodes
  var maxPerNode: Int = 0

  val serverStats = for (s <- servers) yield new Stats

  def receive = {
    case Start(totalPerNode) =>
      log.info("Master starting bursts")
      maxPerNode = totalPerNode
      for (s <- servers) {
        s ! Prime()
        burst(s)
      }

    case BurstAck(senderNodeID: Int, stats: Stats) =>
      serverStats(senderNodeID) += stats
      val s = serverStats(senderNodeID)
      if (s.messages == maxPerNode) {
        println(s"node $senderNodeID done, $s")
        nodesActive -= 1
        if (nodesActive == 0)
          deactivate()
      } else {
        if (active)
          burst(servers(senderNodeID))
      }

    case Join() =>
      listener = Some(sender)
  }

  def burst(server: ActorRef): Unit = {
    for (i <- 1 to burstSize)
      server ! Command()
  }

  def deactivate() = {
    active = false
    val total = new Stats
    serverStats.foreach(total += _)
    println(s"$total")
    if (listener.isDefined)
      listener.get ! total
  }
}

object LoadMaster {
   def props(numNodes: Int, servers: Seq[ActorRef], burstSize: Int): Props = {
      Props(classOf[LoadMaster], numNodes, servers, burstSize)
   }
}

