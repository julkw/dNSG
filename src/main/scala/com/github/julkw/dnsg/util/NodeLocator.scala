package com.github.julkw.dnsg.util

import akka.actor.typed.ActorRef

case class NodeLocatorBuilder[T](dataLength: Int) {
  protected val nodeMap: Array[Option[ActorRef[T]]] = Array.fill(dataLength){None}

  def addLocation(indices: Seq[Int], location: ActorRef[T]): Option[NodeLocator[T]] = {
    indices.foreach { index =>
      nodeMap(index) = Some(location)
    }
    checkIfFull()
  }

  def addFromMap(locations: Map[Int, ActorRef[T]]): Option[NodeLocator[T]] = {
    locations.foreach { case (nodeIndex, location) =>
      nodeMap(nodeIndex) = Some(location)
    }
    checkIfFull()
  }

  protected def checkIfFull(): Option[NodeLocator[T]] = {
    if (nodeMap.contains(None)) {
      None
    } else {
      val locationData = nodeMap.map(_.get)
      Some(NodeLocator(locationData, locationData.toSet))
    }
  }
}

case class NodeLocator[T](locationData: Array[ActorRef[T]], allActors: Set[ActorRef[T]]) {
  val graphSize: Int = locationData.length

  def findResponsibleActor (nodeIndex: Int): ActorRef[T] = {
   locationData(nodeIndex)
  }
}

case class QueryNodeLocator[T](actorMidPoints: Seq[(ActorRef[T], Seq[Float])]) extends Distance {
  def findResponsibleActor(query: Seq[Float]): ActorRef[T] = {
    actorMidPoints.minBy(actorInfo => euclideanDist(actorInfo._2, query))._1
  }
}