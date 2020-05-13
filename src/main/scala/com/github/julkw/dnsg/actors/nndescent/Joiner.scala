package com.github.julkw.dnsg.actors.nndescent

import com.github.julkw.dnsg.actors.nndescent.KnngWorker.{BuildGraphEvent, JoinNodes, PotentialNeighbor}
import com.github.julkw.dnsg.util.{Distance, LocalData, NodeLocator}


abstract class Joiner(sampleRate: Double, data: LocalData[Float]) extends Distance {

  def joinLocals(n1Index: Int, n1Data: Seq[Float], n2Index: Int, n2Data: Seq[Float], nodeLocator: NodeLocator[BuildGraphEvent]): Unit = {
    val dist = euclideanDist(n1Data, n2Data)
    nodeLocator.findResponsibleActor(n1Index) ! PotentialNeighbor(n1Index, (n2Index, dist))
  }

  def joinNode(node: Int, neighbors: Seq[Int], nodeLocator: NodeLocator[BuildGraphEvent]): Unit = {
    if (data.isLocal(node)) {
      val newData = data.get(node)
      neighbors.filter(neighbor => data.isLocal(neighbor)).foreach(neighbor =>
        joinLocals(node, newData, neighbor, data.get(neighbor), nodeLocator)
      )
      neighbors.filter(neighbor => !data.isLocal(neighbor)).groupBy(neighbor => nodeLocator.findResponsibleActor(neighbor)).foreach { case (actor, oldNeighbors) =>
        actor ! JoinNodes(oldNeighbors, node)
      }
    }
    else {
      neighbors.groupBy(neighbor => nodeLocator.findResponsibleActor(neighbor)).foreach { case (actor, neighbors) =>
        actor ! JoinNodes(neighbors, node)
      }
    }
  }

  def joinNeighbors(neighbors: Seq[(Int, Double)], nodeLocator: NodeLocator[BuildGraphEvent]): Unit = {
    val sampledNeighbors = neighbors.filter(_ => scala.util.Random.nextFloat() < sampleRate).map(_._1)
    for (n1 <- 0 until sampledNeighbors.length) {
      val neighbor1 = sampledNeighbors(n1)
      joinNode(neighbor1, sampledNeighbors.slice(n1, sampledNeighbors.size), nodeLocator)
    }
  }

  def joinNewNeighbor(neighbors: Seq[(Int, Double)], oldReverseNeighbors: Set[Int], newNeighbor: Int, nodeLocator: NodeLocator[BuildGraphEvent]): Unit = {
    // use set to prevent duplication of nodes that are both neighbors and reverse neighbors
    val allNeighbors = (neighbors.map(_._1).toSet ++ oldReverseNeighbors).filter(_ => scala.util.Random.nextFloat() < sampleRate)
    joinNode(newNeighbor, allNeighbors.toSeq, nodeLocator)
  }
}
