package com.github.julkw.dnsg.actors.nndescent

import akka.actor.typed.ActorRef
import com.github.julkw.dnsg.actors.nndescent.KnngWorker.BuildKNNGEvent

import scala.collection.mutable

object NNDescentMessageBuffer {
  trait NNDescentEvent
  final case class PotentialNeighbor(g_node: Int, potentialNeighbor: Int, distance: Double, iteration: Int) extends NNDescentEvent

  final case class JoinNodes(g_node: Int, potentialNeighbor: Int, iteration: Int) extends NNDescentEvent

  final case class SendLocation(g_node: Int) extends NNDescentEvent

  final case class PotentialNeighborLocation(potentialNeighborIndex: Int, potentialNeighbor: Array[Float]) extends NNDescentEvent

  final case class RemoveReverseNeighbor(g_node: Int, neighborIndex: Int) extends NNDescentEvent

  final case class AddReverseNeighbor(g_node: Int, newNeighbor: Int, iteration: Int) extends NNDescentEvent
}


case class NNDescentMessageBuffer(localGraphNodes: Array[Int], workers: Set[ActorRef[BuildKNNGEvent]]) {
  import NNDescentMessageBuffer._

  case class ActorSpecificBuffer(nodeIndependentMessages: mutable.Queue[NNDescentEvent], perNodeMessages: Map[Int, mutable.Queue[NNDescentEvent]], var lastNodeSent: Int, var sendImmediately: Boolean)

  protected var messageBuffers: Map[ActorRef[BuildKNNGEvent], ActorSpecificBuffer] = workers.map { worker =>
    worker -> ActorSpecificBuffer(mutable.Queue.empty, localGraphNodes.map(index => index -> mutable.Queue.empty[NNDescentEvent]).toMap, 0, true)
  }.toMap

  var numMessages: Long = 0

  def addNodeIndependentMessage(message: NNDescentEvent, receiver: ActorRef[BuildKNNGEvent]): Unit = {
    numMessages += 1
    messageBuffers(receiver).nodeIndependentMessages += message
  }

  def addNodeMessage(message: NNDescentEvent, receiver: ActorRef[BuildKNNGEvent], responsibleNode: Int): Unit = {
    numMessages += 1
    messageBuffers(receiver).perNodeMessages(responsibleNode) += message
  }

  def nonEmpty(worker: ActorRef[BuildKNNGEvent]): Boolean = {
    messageBuffers(worker).nodeIndependentMessages.nonEmpty || messageBuffers(worker).perNodeMessages.values.exists(_.nonEmpty)
  }

  def isEmpty(worker: ActorRef[BuildKNNGEvent]): Boolean = {
    ! nonEmpty(worker)
  }

  def nothingToSend: Boolean = {
    numMessages == 0
  }

  def sendImmediately(worker: ActorRef[BuildKNNGEvent]): Boolean = {
    messageBuffers(worker).sendImmediately
  }

  def messageTo(sendTo: ActorRef[BuildKNNGEvent], maxMessageSize: Int): collection.Seq[NNDescentEvent] = {
    val (locationMessages, messageSize) = getMessagesUpTo(maxMessageSize, messageBuffers(sendTo).nodeIndependentMessages)
    val messages = locationMessages ++ nodeMessages(maxMessageSize - messageSize, messageBuffers(sendTo))
    messageBuffers(sendTo).sendImmediately = messages.isEmpty
    messages
  }

  protected def nodeMessages(maxMessageSize: Int, actorBuffer: ActorSpecificBuffer): collection.Seq[NNDescentEvent] = {
    var leftoverMessageSize = maxMessageSize
    val neighborOrder = localGraphNodes.view.slice(actorBuffer.lastNodeSent, localGraphNodes.length) ++ localGraphNodes.view.slice(0, actorBuffer.lastNodeSent)
    neighborOrder.flatMap { index =>
      if (leftoverMessageSize > 0) {
        val (messagesToSend, messageSize) = getMessagesUpTo(leftoverMessageSize, actorBuffer.perNodeMessages(index))
        leftoverMessageSize -= messageSize
        actorBuffer.lastNodeSent = index
        messagesToSend
      } else {
        Seq.empty
      }
    }.toSeq
  }

  protected def getMessagesUpTo(maxMessageSize: Int, messages: mutable.Queue[NNDescentEvent]): (collection.Seq[NNDescentEvent], Int) = {
    var currentMessageSize = 0
    val messagesToSend = messages.dequeueWhile { message =>
      if (currentMessageSize + messageSize(message) > maxMessageSize) {
        false
      } else {
        numMessages -= 1
        currentMessageSize += messageSize(message)
        true
      }
    }
    (messagesToSend, currentMessageSize)
  }

  protected def messageSize(message: NNDescentEvent): Int = {
    message match {
      case PotentialNeighbor(_, _, _, _) =>
        5 // because a double is twice a as big as an int/float
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
  }

  def removeNodeMessages(responsibleNode: Int, removedNeighbor: Int): Unit = {
    messageBuffers.valuesIterator.foreach { actorBuffer =>
      actorBuffer.perNodeMessages(responsibleNode).dequeueAll(message => canBeRemoved(message, removedNeighbor)).length
    }
  }

  protected def canBeRemoved(message: NNDescentEvent, removedNeighbor: Int): Boolean = {
    message match {
      case PotentialNeighbor(g_node, potentialNeighbor, _, _) if (g_node == removedNeighbor || potentialNeighbor == removedNeighbor) =>
        numMessages -= 1
        true
      case JoinNodes(g_node, potentialNeighbor, _) if (g_node == removedNeighbor || potentialNeighbor == removedNeighbor) =>
        numMessages -= 1
        true
      case AddReverseNeighbor(g_node, newNeighbor, _) if (g_node == removedNeighbor || newNeighbor == removedNeighbor) =>
        numMessages -= 1
        true
      case _ =>
        false
    }
  }
}
