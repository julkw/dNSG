package com.github.julkw.dnsg.actors.nndescent

import com.github.julkw.dnsg.actors.nndescent.KnngWorker.{BuildGraphEvent, CalculateDistance, PotentialNeighbor, SendLocation}
import com.github.julkw.dnsg.util.{Distance, LocalData, NodeLocator}


abstract class Joiner(sampleRate: Double, data: LocalData[Float]) extends Distance {

  def joinNeighbors(neighbors: Seq[(Int, Double)], nodeLocator: NodeLocator[BuildGraphEvent], g_nodeIndex: Int): Unit = {
    val sampledNeighbors = neighbors.filter(_ => scala.util.Random.nextFloat() < sampleRate)
    for (n1 <- 0 until sampledNeighbors.length) {
      for (n2 <- n1 + 1 until sampledNeighbors.length) {
        joinPair(n1, n2, nodeLocator, g_nodeIndex)
      }
    }
  }

  def joinPair(neighbor1: Int, neighbor2: Int, nodeLocator: NodeLocator[BuildGraphEvent], g_nodeIndex: Int): Unit = {
    val point1 = data.at(neighbor1)
    val point2 = data.at(neighbor2)
    point1 match {
      case Some(p1) =>
        point2 match {
          case Some(p2) =>
            val dist = euclideanDist(p1, p2)
            nodeLocator.findResponsibleActor(neighbor1) ! PotentialNeighbor(neighbor1, (neighbor2, dist), g_nodeIndex)
            nodeLocator.findResponsibleActor(neighbor2) ! PotentialNeighbor(neighbor2, (neighbor1, dist), g_nodeIndex)
          case None =>
            nodeLocator.findResponsibleActor(neighbor2) ! CalculateDistance(neighbor2, neighbor1, p1, g_nodeIndex)
        }
      case None =>
        point2 match {
          case Some(p2) =>
            nodeLocator.findResponsibleActor(neighbor1) ! CalculateDistance(neighbor1, neighbor2, p2, g_nodeIndex)
          case None =>
            nodeLocator.findResponsibleActor(neighbor1) ! SendLocation(neighbor1, neighbor2, g_nodeIndex)
        }
    }
  }

  def joinNewNeighbor(neighbors: Seq[(Int, Double)], oldReverseNeighbors: Set[Int], g_nodeIndex: Int, newNeighbor: Int, senderIndex: Int, nodeLocator: NodeLocator[BuildGraphEvent]): Unit = {
      // don't send newNeighbor back to the node that introduced us
      // use set to prevent duplication of nodes that are both neighbors and reverse neighbors
      val allNeighbors = neighbors.map(_._1).toSet ++ oldReverseNeighbors - senderIndex
      allNeighbors.foreach { oldNeighbor =>
        if (scala.util.Random.nextFloat() < sampleRate) {
          joinPair(oldNeighbor, newNeighbor, nodeLocator, g_nodeIndex)
        }
      }
  }
}
