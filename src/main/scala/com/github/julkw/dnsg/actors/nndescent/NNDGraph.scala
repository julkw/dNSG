package com.github.julkw.dnsg.actors.nndescent

import akka.actor.typed.ActorRef
import com.github.julkw.dnsg.actors.nndescent.KnngWorker.BuildKNNGEvent
import com.github.julkw.dnsg.actors.nndescent.NNDInfo.{AddReverseNeighbor, RemoveReverseNeighbor}
import com.github.julkw.dnsg.util.{Distance, NodeLocator}

import scala.collection.{IndexedSeqView, mutable}

case class NNDGraph(k: Int, nodes: Seq[Int], maxReverseNeighbors: Int) extends Distance {
  case class GraphNode(neighbors: Array[Int], var numberOfNeighbors: Int, distances: mutable.PriorityQueue[(Double, Int)], var reverseNeighbors: Set[Int])

  val graph: Map[Int, GraphNode] = nodes.map {index =>
    index -> GraphNode(Array.fill(k){-1}, 0, mutable.PriorityQueue.empty, Set.empty)
  }.toMap

  def insertInitial(graphNode: Int, neighbors: Seq[(Int, Double)]): Unit = {
    val node = graph(graphNode)
    neighbors.zipWithIndex.foreach { case (neighbor, neighborIndex) =>
      node.neighbors(neighborIndex) = neighbor._1
      node.distances.addOne((neighbor._2, neighborIndex))
      node.numberOfNeighbors += 1
    }
  }

  def insert(graphNode: Int,
             neighbor: Int,
             distance: Double,
             iteration: Int,
             toSend: Map[ActorRef[BuildKNNGEvent], NNDInfo],
             nodeLocator: NodeLocator[BuildKNNGEvent]): Boolean = {
    val node = graph(graphNode)
    if (node.distances.head._1 <= distance || node.neighbors.contains(neighbor)) {
      false
    } else {
      if (node.numberOfNeighbors >= k) {
        val neighborIndex = node.distances.dequeue()._2
        val replacedNeighbor = node.neighbors(neighborIndex)
        toSend(nodeLocator.findResponsibleActor(replacedNeighbor)).addMessage(RemoveReverseNeighbor(replacedNeighbor, graphNode))
        node.neighbors(neighborIndex) = neighbor
        node.distances.addOne((distance, neighborIndex))
      } else {
        node.neighbors(node.numberOfNeighbors) = neighbor
        node.distances.addOne((distance, node.numberOfNeighbors))
        node.numberOfNeighbors += 1
      }
      toSend(nodeLocator.findResponsibleActor(neighbor)).addMessage(AddReverseNeighbor(neighbor, graphNode, iteration))
      true
    }
  }

  def addReverseNeighbor(node: Int, neighbor: Int): Unit = {
    val oldRevNeighbors = graph(node).reverseNeighbors
    if (oldRevNeighbors.size >= maxReverseNeighbors) {
      graph(node).reverseNeighbors = oldRevNeighbors - oldRevNeighbors.head + neighbor
    } else {
      graph(node).reverseNeighbors = oldRevNeighbors + neighbor
    }
  }

  def removeReverseNeighbor(node: Int, neighbor: Int): Unit = {
    graph(node).reverseNeighbors -= neighbor
  }

  def getNeighbors(index: Int): IndexedSeqView[Int] = {
    val node = graph(index)
    node.neighbors.view.slice(0, node.numberOfNeighbors)
  }

  def getReversedNeighbors(index: Int): Set[Int] = {
    graph(index).reverseNeighbors
  }

  def cleanedGraph(): Map[Int, Seq[Int]] = {
    graph.transform { (_, node) =>
      node.neighbors.slice(0, node.numberOfNeighbors).toSeq
    }
  }

  def averageGraphDist(): Double = {
    val allLocalDists = graph.toSeq.flatMap { case (_, node) =>
      node.distances.toSeq.map{_._1}
    }
    allLocalDists.sum / allLocalDists.length
  }

}
