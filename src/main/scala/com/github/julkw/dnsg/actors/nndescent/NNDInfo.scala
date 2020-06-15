package com.github.julkw.dnsg.actors.nndescent

import scala.collection.mutable

object NNDInfo {

  trait NNDescentEvent
  final case class PotentialNeighbor(g_node: Int, potentialNeighbor: (Int, Double)) extends NNDescentEvent

  final case class JoinNodes(g_nodes: Seq[Int], potentialNeighborIndex: Int) extends NNDescentEvent

  final case class SendLocation(g_node: Int) extends NNDescentEvent

  final case class PotentialNeighborLocation(potentialNeighborIndex: Int, potentialNeighbor: Seq[Float]) extends NNDescentEvent

  final case class RemoveReverseNeighbor(g_nodeIndex: Int, neighborIndex: Int) extends NNDescentEvent

  final case class AddReverseNeighbor(g_nodeIndex: Int, neighborIndex: Int) extends NNDescentEvent
}

case class NNDInfo() {
  import NNDInfo._
  var messagesToSend: mutable.Queue[NNDescentEvent] = mutable.Queue.empty

  def addMessage(message: NNDescentEvent): Unit = {
    messagesToSend += message
  }

  def sendMessage(maxMessageSize: Int): collection.Seq[NNDescentEvent] = {
    var currentMessageSize = 0
    val newMessage = messagesToSend.dequeueWhile {message =>
      val thisMessageSize = message match {
        case PotentialNeighbor(_, _) =>
          3
        case JoinNodes(g_nodes, _) =>
          g_nodes.length + 1
        case RemoveReverseNeighbor(_, _) | AddReverseNeighbor(_, _) =>
          2
        case SendLocation(_) =>
          1
        case PotentialNeighborLocation(_, potentialNeighbor) =>
          potentialNeighbor.length + 1
      }
      if (currentMessageSize + thisMessageSize > maxMessageSize) {
        false
      } else {
        currentMessageSize += thisMessageSize
        true
      }
    }
    newMessage
  }
}