package com.github.julkw.dnsg.actors.nndescent

import com.github.julkw.dnsg.actors.nndescent.KnngWorker.{BuildGraphEvent, CalculateDistance, PotentialNeighbor, SendLocation}
import com.github.julkw.dnsg.util.{Distance, LocalData, NodeLocator}


abstract class Joiner(sampleRate: Double, data: LocalData[Float]) extends Distance {

  def joinNeighbors(neighbors: Seq[(Int, Double)], nodeLocator: NodeLocator[BuildGraphEvent], g_nodeIndex: Int): Unit = {
    val sampledNeighbors = neighbors.filter(_ => scala.util.Random.nextFloat() < sampleRate).map(_._1)
    val localNeighbors = sampledNeighbors.filter(neighbor => data.isLocal(neighbor))
    val remoteNeighbors = sampledNeighbors.filter(neighbor => !data.isLocal(neighbor))

    for (n1 <- 0 until localNeighbors.length) {
      val neighbor1 = localNeighbors(n1)
      val data1 = data.at(neighbor1).get
      // both neighbors are local
      for (n2 <- n1 + 1 until localNeighbors.length) {
        val neighbor2 = localNeighbors(n2)
        val dist = euclideanDist(data1, data.at(neighbor2).get)
        nodeLocator.findResponsibleActor(neighbor1) ! PotentialNeighbor(neighbor1, (neighbor2, dist), g_nodeIndex)
        nodeLocator.findResponsibleActor(neighbor2) ! PotentialNeighbor(neighbor2, (neighbor1, dist), g_nodeIndex)
      }
      // one of the neighbors is local
      remoteNeighbors.groupBy(remoteNeighbor => nodeLocator.findResponsibleActor(remoteNeighbor)).foreach {
        case (actor, potentialNeighbors) =>
          actor ! CalculateDistance(potentialNeighbors, neighbor1, data1, g_nodeIndex)
      }
    }
    // neither neighbor is local
    for (n1 <- 0 until remoteNeighbors.length) {
      val neighbor1 = remoteNeighbors(n1)
      nodeLocator.findResponsibleActor(neighbor1) ! SendLocation(neighbor1, remoteNeighbors.slice(n1 + 1, remoteNeighbors.length), g_nodeIndex)
    }
  }

  def joinNewNeighbor(neighbors: Seq[(Int, Double)], oldReverseNeighbors: Set[Int], g_nodeIndex: Int, newNeighbor: Int, senderIndex: Int, nodeLocator: NodeLocator[BuildGraphEvent]): Unit = {
    // don't send newNeighbor back to the node that introduced us
    // use set to prevent duplication of nodes that are both neighbors and reverse neighbors
    val allNeighbors = (neighbors.map(_._1).toSet ++ oldReverseNeighbors - senderIndex).filter(_ => scala.util.Random.nextFloat() < sampleRate)
    val localNeighbors = allNeighbors.filter(neighbor => data.isLocal(neighbor)).toSeq
    val remoteNeighbors = allNeighbors.filter(neighbor => !data.isLocal(neighbor)).toSeq
    if (data.isLocal(newNeighbor)) {
      // decide between sending location and calculating distance
      val newData = data.at(newNeighbor).get
      localNeighbors.foreach { oldNeighbor =>
        val dist = euclideanDist(newData, data.at(oldNeighbor).get)
        nodeLocator.findResponsibleActor(newNeighbor) ! PotentialNeighbor(newNeighbor, (oldNeighbor, dist), g_nodeIndex)
        nodeLocator.findResponsibleActor(oldNeighbor) ! PotentialNeighbor(oldNeighbor, (newNeighbor, dist), g_nodeIndex)
      }
      remoteNeighbors.groupBy(remoteNeighbor => nodeLocator.findResponsibleActor(remoteNeighbor)).foreach {
        case (actor, potentialNeighbors) =>
          actor ! CalculateDistance(potentialNeighbors, newNeighbor, newData, g_nodeIndex)
      }
    }
    else {
      // decide between sending it the other locations and asking for locations
      // TODO this sends a lot of single messages again. Bundle somehow?
      // (I do not want to always send node data in PotentialNeighbor messages, which would help here but explode message sizes overall
      localNeighbors.foreach ( neighbor =>
        nodeLocator.findResponsibleActor(newNeighbor) ! CalculateDistance(Seq(newNeighbor), neighbor, data.at(neighbor).get, g_nodeIndex)
      )
      nodeLocator.findResponsibleActor(newNeighbor) ! SendLocation(newNeighbor, remoteNeighbors, g_nodeIndex)
    }
  }
}
