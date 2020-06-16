package com.github.julkw.dnsg.actors.nndescent

import akka.actor.typed.ActorRef
import com.github.julkw.dnsg.actors.nndescent.KnngWorker.BuildGraphEvent
import com.github.julkw.dnsg.actors.nndescent.NNDInfo.{JoinNodes, PotentialNeighbor}
import com.github.julkw.dnsg.util.Data.CacheData
import com.github.julkw.dnsg.util.{Distance, NodeLocator}


abstract class Joiner(sampleRate: Double, data: CacheData[Float]) extends Distance {

  def joinLocals(n1Index: Int,
                 n1Data: Seq[Float],
                 n2Index: Int,
                 n2Data: Seq[Float],
                 toSend: Map[ActorRef[BuildGraphEvent], NNDInfo],
                 nodeLocator: NodeLocator[BuildGraphEvent]): Unit = {
    val dist = euclideanDist(n1Data, n2Data)
    toSend(nodeLocator.findResponsibleActor(n1Index)).addMessage(PotentialNeighbor(n1Index, (n2Index, dist)))
    toSend(nodeLocator.findResponsibleActor(n2Index)).addMessage(PotentialNeighbor(n2Index, (n1Index, dist)))
  }

  def joinNode(nodeToJoin: Int, neighbors: Seq[Int], toSend: Map[ActorRef[BuildGraphEvent], NNDInfo], nodeLocator: NodeLocator[BuildGraphEvent]): Unit = {
    if (data.isLocal(nodeToJoin)) {
      val newData = data.get(nodeToJoin)
      val (localNeighbors, remoteNeighbors) = neighbors.partition(neighbor => data.isLocal(neighbor))
      localNeighbors.foreach(neighbor =>
        joinLocals(nodeToJoin, newData, neighbor, data.get(neighbor), toSend, nodeLocator)
      )
      remoteNeighbors.groupBy(neighbor => nodeLocator.findResponsibleActor(neighbor)).foreach { case (actor, oldNeighbors) =>
        toSend(actor).addMessage(JoinNodes(oldNeighbors, nodeToJoin))
      }
    }
    else {
      neighbors.groupBy(neighbor => nodeLocator.findResponsibleActor(neighbor)).foreach { case (actor, groupedNeighbors) =>
        toSend(actor).addMessage(JoinNodes(groupedNeighbors, nodeToJoin))
      }
    }
  }

  def joinNeighbors(neighbors: Seq[(Int, Double)], toSend: Map[ActorRef[BuildGraphEvent], NNDInfo], nodeLocator: NodeLocator[BuildGraphEvent]): Unit = {
    val sampledNeighbors = neighbors.filter(_ => scala.util.Random.nextFloat() < sampleRate).map(_._1)
    for (n1 <- sampledNeighbors.indices) {
      val neighbor1 = sampledNeighbors(n1)
      joinNode(neighbor1, sampledNeighbors.slice(n1, sampledNeighbors.size), toSend, nodeLocator)
    }
  }

  def joinNewNeighbor(neighbors: Seq[(Int, Double)],
                      oldReverseNeighbors: Set[Int],
                      newNeighbor: Int,
                      toSend: Map[ActorRef[BuildGraphEvent], NNDInfo],
                      nodeLocator: NodeLocator[BuildGraphEvent]): Unit = {
    // use set to prevent duplication of nodes that are both neighbors and reverse neighbors
    val allNeighbors = (neighbors.map(_._1).toSet ++ oldReverseNeighbors).filter(_ => scala.util.Random.nextFloat() < sampleRate)
    joinNode(newNeighbor, allNeighbors.toSeq, toSend, nodeLocator)
  }
}
