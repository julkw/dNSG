package com.github.julkw.dnsg.util

import akka.actor.typed.ActorRef

case class NodeLocatorBuilder[T](dataLength: Int) {
  protected val nodeMap: Array[Int] = Array.fill(dataLength){-1}
  protected var actors: Array[ActorRef[T]] = Array.empty

  def addLocation(indices: Seq[Int], location: ActorRef[T]): Option[NodeLocator[T]] = {
    var actorIndex = actors.indexOf(location)
    if (actorIndex < 0) {
      actorIndex = actors.length
      actors :+= location
    }
    indices.foreach { index =>
      nodeMap(index) = actorIndex
    }
    nodeLocator()
  }

  def nodeLocator(): Option[NodeLocator[T]] = {
    if (nodeMap.contains(-1)) {
      None
    } else {
      Some(NodeLocator(nodeMap, actors))
    }
  }
}

case class NodeLocator[T](locationData: Array[Int], actors: Array[ActorRef[T]]) {
  val graphSize: Int = locationData.length

  def allActors: Set[ActorRef[T]] = {
    actors.toSet
  }

  def actorsResponsibilities(): Map[ActorRef[T], Seq[Int]] = {
    locationData.zipWithIndex.groupBy(_._1).map { case (actorIndex, indices) =>
      actors(actorIndex) -> indices.map(_._2)
    }
  }

  def numberOfNodes(actor: ActorRef[T]): Int = {
    val actorIndex = actors.indexOf(actor)
    locationData.count(_ == actorIndex)
  }

  def nodesOf(actor: ActorRef[T]): Seq[Int] = {
    val actorIndex = actors.indexOf(actor)
    locationData.zipWithIndex.filter(_._1 == actorIndex).map(_._2)
  }

  def findResponsibleActor (nodeIndex: Int): ActorRef[T] = {
   actors(locationData(nodeIndex))
  }
}

case class QueryNodeLocator[T](actorMidPoints: Seq[(ActorRef[T], Array[Float])], graphSize: Int) extends Distance {
  val allActors: Set[ActorRef[T]] = actorMidPoints.map(_._1).toSet

  // TODO replace by comparing angle from navigating node to midpoint?
  def findResponsibleActor(query: Array[Float]): ActorRef[T] = {
    actorMidPoints.minBy(actorInfo => euclideanDist(actorInfo._2, query))._1
  }
}