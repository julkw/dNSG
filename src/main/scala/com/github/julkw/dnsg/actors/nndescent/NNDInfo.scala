package com.github.julkw.dnsg.actors.nndescent

import scala.collection.mutable

object NNDInfo {

  trait NNDescentEvent
  final case class PotentialNeighbor(g_node: Int, potentialNeighbor: Int, distance: Double, iteration: Int) extends NNDescentEvent

  final case class JoinNodes(g_node: Int, potentialNeighbor: Int, iteration: Int) extends NNDescentEvent

  final case class SendLocation(g_node: Int) extends NNDescentEvent

  final case class PotentialNeighborLocation(potentialNeighborIndex: Int, potentialNeighbor: Array[Float]) extends NNDescentEvent

  final case class RemoveReverseNeighbor(g_nodeIndex: Int, neighborIndex: Int) extends NNDescentEvent

  final case class AddReverseNeighbor(g_nodeIndex: Int, newNeighbor: Int, iteration: Int) extends NNDescentEvent
}

case class NNDInfo() {
  import NNDInfo._
  // TODO use arrays instead (preallocate memory) to hopefully increase efficiency?
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
        case PotentialNeighbor(_, _, _, _) =>
          4
        case JoinNodes(_, _, _) =>
          3
        case AddReverseNeighbor(_, _, _) =>
          3
        case RemoveReverseNeighbor(_, _) =>
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