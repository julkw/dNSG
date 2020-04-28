package com.github.julkw.dnsg.util

import akka.actor.typed.ActorRef


case class NodeLocatorBuilder[T](dataLength: Int) {
  val nodeMap: Array[Option[ActorRef[T]]] = Array.fill(dataLength){None}

  def addLocations(indices: Seq[Int], location: ActorRef[T]): Option[NodeLocator[T]] = {
    indices.foreach { index =>
      nodeMap(index) = Some(location)
    }
    if (nodeMap.contains(None)) {
      None
    } else {
      Some(NodeLocator(nodeMap.map(_.get)))
    }
  }
}

case class NodeLocator[T](locationData: Array[ActorRef[T]]) {
  def findResponsibleActor (absolutIndex: Int): ActorRef[T] = {
   locationData(absolutIndex)
  }

  def graphSize: Int = {
    locationData.length
  }
}