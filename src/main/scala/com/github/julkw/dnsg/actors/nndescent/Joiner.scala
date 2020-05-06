package com.github.julkw.dnsg.actors.nndescent

import com.github.julkw.dnsg.actors.nndescent.KnngWorker.{BuildGraphEvent, CalculateDistance, JoinNodes, PotentialNeighbor, SendLocation}
import com.github.julkw.dnsg.util.{Distance, LocalData, NodeLocator}


abstract class Joiner(sampleRate: Double, data: LocalData[Float]) extends Distance {

  def joinLocals(n1Index: Int, n1Data: Seq[Float], n2Index: Int, n2Data: Seq[Float], nodeLocator: NodeLocator[BuildGraphEvent], senderIndex: Int): Unit = {
    val dist = euclideanDist(n1Data, n2Data)
    nodeLocator.findResponsibleActor(n1Index) ! PotentialNeighbor(n1Index, (n2Index, dist), senderIndex)
  }

  def joinNode(node: Int, neighbors: Seq[Int], nodeLocator: NodeLocator[BuildGraphEvent], senderIndex: Int): Unit = {
    if (data.isLocal(node)) {
      val newData = data.get(node)
      neighbors.filter(neighbor => data.isLocal(neighbor)).foreach(neighbor =>
        joinLocals(node, newData, neighbor, data.get(neighbor), nodeLocator, senderIndex)
      )
      neighbors.filter(neighbor => !data.isLocal(neighbor)).groupBy(neighbor => nodeLocator.findResponsibleActor(neighbor)).foreach { case (actor, oldNeighbors) =>
        actor ! JoinNodes(oldNeighbors, node, senderIndex)
      }
    }
    else {
      neighbors.groupBy(neighbor => nodeLocator.findResponsibleActor(neighbor)).foreach { case (actor, neighbors) =>
        actor ! JoinNodes(neighbors, node, senderIndex)
      }
    }
  }

  def joinNeighbors(neighbors: Seq[(Int, Double)], nodeLocator: NodeLocator[BuildGraphEvent], g_nodeIndex: Int): Unit = {
    val sampledNeighbors = neighbors.filter(_ => scala.util.Random.nextFloat() < sampleRate).map(_._1)
    for (n1 <- 0 until sampledNeighbors.length) {
      val neighbor1 = sampledNeighbors(n1)
      joinNode(neighbor1, sampledNeighbors.slice(n1, sampledNeighbors.size), nodeLocator, g_nodeIndex)
    }
  }

  def joinNewNeighbor(neighbors: Seq[(Int, Double)], oldReverseNeighbors: Set[Int], g_nodeIndex: Int, newNeighbor: Int, senderIndex: Int, nodeLocator: NodeLocator[BuildGraphEvent]): Unit = {
    // don't send newNeighbor back to the node that introduced us
    // use set to prevent duplication of nodes that are both neighbors and reverse neighbors
    val allNeighbors = (neighbors.map(_._1).toSet ++ oldReverseNeighbors - senderIndex).filter(_ => scala.util.Random.nextFloat() < sampleRate)
    joinNode(newNeighbor, allNeighbors.toSeq, nodeLocator, senderIndex)
  }
}
