package com.github.julkw.dnsg.actors.nndescent

import com.github.julkw.dnsg.actors.nndescent.KnngWorker.{BuildGraphEvent, CalculateDistance, JoinNodes, PotentialNeighbor, SendLocation}
import com.github.julkw.dnsg.util.{Distance, LocalData, NodeLocator}


abstract class Joiner(sampleRate: Double, data: LocalData[Float]) extends Distance {

  def join(n1Index: Int, n1Data: Seq[Float], n2Index: Int, n2Data: Seq[Float], nodeLocator: NodeLocator[BuildGraphEvent], senderIndex: Int): Unit = {
    val dist = euclideanDist(n1Data, n2Data)
    nodeLocator.findResponsibleActor(n1Index) ! PotentialNeighbor(n1Index, (n2Index, dist), senderIndex)
  }

  def joinNeighbors(neighbors: Seq[(Int, Double)], nodeLocator: NodeLocator[BuildGraphEvent], g_nodeIndex: Int): Unit = {
    val sampledNeighbors = neighbors.filter(_ => scala.util.Random.nextFloat() < sampleRate).map(_._1)

    for (n1 <- 0 until sampledNeighbors.length) {
      val neighbor1 = sampledNeighbors(n1)
      if(data.isLocal(neighbor1)) {
        for (n2 <- n1 + 1 until sampledNeighbors.length) {
          val neighbor2 = sampledNeighbors(n2)
          if (data.isLocal(neighbor2)) {
            join(neighbor1, data.get(neighbor1), neighbor2, data.get(neighbor2), nodeLocator, g_nodeIndex)
          } else {
            nodeLocator.findResponsibleActor(neighbor2) ! JoinNodes(neighbor2, neighbor1, g_nodeIndex)
          }
        }
      } else {
        for (n2 <- n1 + 1 until sampledNeighbors.length) {
          val neighbor2 = sampledNeighbors(n2)
          nodeLocator.findResponsibleActor(neighbor1) ! JoinNodes(neighbor1, neighbor2, g_nodeIndex)
        }
      }
    }
  }

  def joinNewNeighbor(neighbors: Seq[(Int, Double)], oldReverseNeighbors: Set[Int], g_nodeIndex: Int, newNeighbor: Int, senderIndex: Int, nodeLocator: NodeLocator[BuildGraphEvent]): Unit = {
    // don't send newNeighbor back to the node that introduced us
    // use set to prevent duplication of nodes that are both neighbors and reverse neighbors
    val allNeighbors = (neighbors.map(_._1).toSet ++ oldReverseNeighbors - senderIndex).filter(_ => scala.util.Random.nextFloat() < sampleRate)
    if (data.isLocal(newNeighbor)) {
      // decide between sending location and calculating distance
      val newData = data.get(newNeighbor)
      allNeighbors.foreach { oldNeighbor =>
        if (data.isLocal(oldNeighbor)) {
          join(newNeighbor, newData, oldNeighbor, data.get(oldNeighbor), nodeLocator, senderIndex)
        } else {
          nodeLocator.findResponsibleActor(oldNeighbor) ! JoinNodes(oldNeighbor, newNeighbor, senderIndex)
        }
      }
    }
    else {
      allNeighbors.foreach { oldNeighbor =>
        nodeLocator.findResponsibleActor(oldNeighbor) ! JoinNodes(oldNeighbor, newNeighbor, senderIndex)
      }
    }
  }
}
