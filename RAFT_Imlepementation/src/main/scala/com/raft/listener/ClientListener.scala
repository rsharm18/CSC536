package com.raft.listener

import akka.actor.{Actor, ActorPath, ActorRef}
import akka.cluster.client.{ContactPointAdded, ContactPointRemoved, ContactPoints, SubscribeContactPoints}

class ClientListener (targetClient: ActorRef) extends Actor {
  override def preStart(): Unit =
    targetClient ! SubscribeContactPoints

  def receive: Receive =
    receiveWithContactPoints(Set.empty)

  def receiveWithContactPoints(contactPoints: Set[ActorPath]): Receive = {
    case ContactPoints(cps) =>
      context.become(receiveWithContactPoints(cps))
    // Now do something with the up-to-date "cps"
    case ContactPointAdded(cp) =>
      context.become(receiveWithContactPoints(contactPoints + cp))
    // Now do something with an up-to-date "contactPoints + cp"
    case ContactPointRemoved(cp) =>
      context.become(receiveWithContactPoints(contactPoints - cp))
    // Now do something with an up-to-date "contactPoints - cp"
  }
}