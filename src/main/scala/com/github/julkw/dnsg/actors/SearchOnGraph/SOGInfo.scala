package com.github.julkw.dnsg.actors.SearchOnGraph
import scala.collection.mutable

object SOGInfo {

  trait SOGEvent
  final case class GetNeighbors(index: Int) extends SOGEvent

  final case class Neighbors(index: Int, neighbors: Seq[Int]) extends SOGEvent

  final case class GetLocation(index: Int) extends SOGEvent

  final case class Location(index: Int, location: Seq[Float]) extends SOGEvent
}
// TODO add trait to reduce code duplication with NNDInfo
case class SOGInfo() {
  import SOGInfo._
  protected val messagesToSend: mutable.Queue[SOGEvent] = mutable.Queue.empty
  var sendImmediately: Boolean = true

  def addMessage(message: SOGEvent): Unit = {
    messagesToSend += message
  }

  def isEmpty: Boolean = {
    messagesToSend.isEmpty
  }

  def nonEmpty: Boolean = {
    messagesToSend.nonEmpty
  }

  def sendMessage(maxMessageSize: Int): collection.Seq[SOGEvent] = {
    var currentMessageSize = 0
    val newMessage = messagesToSend.dequeueWhile {message =>
      val thisMessageSize = message match {
        case GetNeighbors(_) =>
          1
        case Neighbors(_, neighbors) =>
          neighbors.length + 1
        case GetLocation(_) =>
          1
        case Location(_, location) =>
          location.length + 1
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