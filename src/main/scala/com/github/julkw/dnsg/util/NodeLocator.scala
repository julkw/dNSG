package com.github.julkw.dnsg.util

case class NodeLocatorBuilder[T](dataLength: Int) {
  protected val nodeMap: Array[Option[T]] = Array.fill(dataLength){None}

  def addLocation(indices: Seq[Int], location: T): Option[NodeLocator[T]] = {
    indices.foreach { index =>
      nodeMap(index) = Some(location)
    }
    checkIfFull()
  }

  def addFromMap(locations: Map[Int, T]): Option[NodeLocator[T]] = {
    locations.foreach { case (nodeIndex, location) =>
      nodeMap(nodeIndex) = Some(location)
    }
    checkIfFull()
  }

  protected def checkIfFull(): Option[NodeLocator[T]] = {
    if (nodeMap.contains(None)) {
      None
    } else {
      Some(NodeLocator(nodeMap.map(_.get)))
    }
  }
}

case class NodeLocator[T](locationData: Array[T]) {
  val graphSize = locationData.length

  def findResponsibleActor (nodeIndex: Int): T = {
   locationData(nodeIndex)
  }

  def allActors(): Set[T] = {
    locationData.toSet
  }
}