package com.github.julkw.dnsg.actors.nndescent

import com.github.julkw.dnsg.actors.nndescent.KnngWorker.{BuildGraphEvent, PotentialNeighbor}
import com.github.julkw.dnsg.util.{Distance, LocalData, NodeLocator}


abstract class Joiner(sampleRate: Double, data: LocalData[Float]) extends Distance {

  def joinNeighbors(neighbors: Seq[(Int, Double)], nodeLocator: NodeLocator[BuildGraphEvent], g_nodeIndex: Int): Unit = {
    val sampledNeighbors = neighbors.filter(_ => scala.util.Random.nextFloat() < sampleRate)
    for (n1 <- 0 until sampledNeighbors.length) {
      for (n2 <- n1 + 1 until sampledNeighbors.length) {
        val neighbor1 = sampledNeighbors(n1)._1
        val neighbor2 = sampledNeighbors(n2)._1
        // TODO this will have to be adjusted to deal with potentially non local neighbors
        val dist = euclideanDist(data.at(neighbor1).get, data.at(neighbor2).get)
        nodeLocator.findResponsibleActor(neighbor1) ! PotentialNeighbor(neighbor1, (neighbor2, dist), g_nodeIndex)
        nodeLocator.findResponsibleActor(neighbor2) ! PotentialNeighbor(neighbor2, (neighbor1, dist), g_nodeIndex)
      }
    }
  }

  def joinNewNeighbor(neighbors: Seq[(Int, Double)], oldReverseNeighbors: Set[Int], g_nodeIndex: Int, newNeighbor: Int, senderIndex: Int, nodeLocator: NodeLocator[BuildGraphEvent]): Unit = {
      // don't send newNeighbor back to the node that introduced us
      // use set to prevent duplication of nodes that are both neighbors and reverse neighbors
      val allNeighbors = neighbors.map(_._1).toSet ++ oldReverseNeighbors - senderIndex
      val newNeighborActor = nodeLocator.findResponsibleActor(newNeighbor)
      allNeighbors.foreach { oldNeighbor =>
        if (scala.util.Random.nextFloat() < sampleRate) {
          // TODO this will have to be adjusted to deal with potentially non local neighbors
          val dist = euclideanDist(data.at(oldNeighbor).get, data.at(newNeighbor).get)
          nodeLocator.findResponsibleActor(oldNeighbor) ! PotentialNeighbor(oldNeighbor, (newNeighbor, dist), g_nodeIndex)
          newNeighborActor ! PotentialNeighbor(newNeighbor, (oldNeighbor, dist), g_nodeIndex)
        }
      }
  }
}
