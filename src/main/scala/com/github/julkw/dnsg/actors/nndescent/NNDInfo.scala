package com.github.julkw.dnsg.actors.nndescent

import com.github.julkw.dnsg.actors.nndescent.KnngWorker.{NeighborWithDist, Neighbor}

import scala.collection.mutable

object NNDInfo {

  trait NNDescentEvent
  final case class PotentialNeighbor(g_node: Int, potentialNeighbor: NeighborWithDist) extends NNDescentEvent

  final case class JoinNodes(g_nodes: Seq[Neighbor], potentialNeighbor: Neighbor) extends NNDescentEvent

  final case class SendLocation(g_node: Int) extends NNDescentEvent

  final case class PotentialNeighborLocation(potentialNeighborIndex: Int, potentialNeighbor: Seq[Float]) extends NNDescentEvent

  final case class RemoveReverseNeighbor(g_nodeIndex: Int, neighborIndex: Int) extends NNDescentEvent

  final case class AddReverseNeighbor(g_nodeIndex: Int, newNeighbor: Neighbor) extends NNDescentEvent
}

case class NNDInfo() {
  import NNDInfo._
  val messagesToSend: mutable.Queue[NNDescentEvent] = mutable.Queue.empty
  var sendImmediately: Boolean = false

  def addMessage(message: NNDescentEvent): Unit = {
    messagesToSend += message
  }

  def isEmpty: Boolean = {
    messagesToSend.isEmpty
  }

  def nonEmpty: Boolean = {
    messagesToSend.nonEmpty
  }

  def sendMessage(maxMessageSize: Int): collection.Seq[NNDescentEvent] = {
    var currentMessageSize = 0
    val newMessage = messagesToSend.dequeueWhile {message =>
      val thisMessageSize = message match {
        case PotentialNeighbor(_, _) =>
          4
        case JoinNodes(g_nodes, _) =>
          (g_nodes.length + 1) * 2
        case RemoveReverseNeighbor(_, _) | AddReverseNeighbor(_, _) =>
          3
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