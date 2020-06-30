package com.github.julkw.dnsg.actors.nndescent

import akka.actor.typed.ActorRef
import com.github.julkw.dnsg.actors.nndescent.KnngWorker.{BuildKNNGEvent, Neighbor}
import com.github.julkw.dnsg.actors.nndescent.NNDInfo.{JoinNodes, PotentialNeighbor}
import com.github.julkw.dnsg.util.Data.CacheData
import com.github.julkw.dnsg.util.{Distance, NodeLocator}


abstract class Joiner(sampleRate: Double, data: CacheData[Float]) extends Distance {

  def joinLocals(n1Index: Int,
                 n1Data: Seq[Float],
                 n2Index: Int,
                 n2Data: Seq[Float],
                 iteration: Int,
                 toSend: Map[ActorRef[BuildKNNGEvent], NNDInfo],
                 nodeLocator: NodeLocator[BuildKNNGEvent]): Unit = {
    val dist = euclideanDist(n1Data, n2Data)
    toSend(nodeLocator.findResponsibleActor(n1Index)).addMessage(PotentialNeighbor(n1Index, Neighbor(n2Index, dist, iteration)))
    toSend(nodeLocator.findResponsibleActor(n2Index)).addMessage(PotentialNeighbor(n2Index, Neighbor(n1Index, dist, iteration)))
  }

  def joinNode(nodeToJoin: Neighbor, neighbors: Seq[Neighbor], toSend: Map[ActorRef[BuildKNNGEvent], NNDInfo], nodeLocator: NodeLocator[BuildKNNGEvent]): Unit = {
    if (data.isLocal(nodeToJoin.index)) {
      val newData = data.get(nodeToJoin.index)
      val (localNeighbors, remoteNeighbors) = neighbors.partition(neighbor => data.isLocal(neighbor.index))
      localNeighbors.foreach { neighbor =>
        val iteration = math.max(nodeToJoin.iteration, neighbor.iteration) + 1
        joinLocals(nodeToJoin.index, newData, neighbor.index, data.get(neighbor.index), iteration, toSend, nodeLocator)
      }
      remoteNeighbors.groupBy(neighbor => nodeLocator.findResponsibleActor(neighbor.index)).foreach { case (actor, oldNeighbors) =>
        val oldNs = oldNeighbors.map(neighbor => (neighbor.index, neighbor.iteration))
        toSend(actor).addMessage(JoinNodes(oldNs, (nodeToJoin.index, nodeToJoin.iteration)))
      }
    }
    else {
      neighbors.groupBy(neighbor => nodeLocator.findResponsibleActor(neighbor.index)).foreach { case (actor, groupedNeighbors) =>
        val groupedNs = groupedNeighbors.map(neighbor => (neighbor.index, neighbor.iteration))

        toSend(actor).addMessage(JoinNodes(groupedNs, (nodeToJoin.index, nodeToJoin.iteration)))
      }
    }
  }

  def joinNeighbors(neighbors: Seq[Neighbor], toSend: Map[ActorRef[BuildKNNGEvent], NNDInfo], nodeLocator: NodeLocator[BuildKNNGEvent]): Unit = {
    val sampledNeighbors = neighbors.filter(_ => scala.util.Random.nextFloat() < sampleRate)
    for (n1 <- sampledNeighbors.indices) {
      val neighbor1 = sampledNeighbors(n1)
      joinNode(neighbor1, sampledNeighbors.slice(n1, sampledNeighbors.size), toSend, nodeLocator)
    }
  }

  def joinNewNeighbor(neighbors: Seq[Neighbor],
                      oldReverseNeighbors: Set[Neighbor],
                      newNeighbor: Neighbor,
                      toSend: Map[ActorRef[BuildKNNGEvent], NNDInfo],
                      nodeLocator: NodeLocator[BuildKNNGEvent]): Unit = {
    // use set to prevent duplication of nodes that are both neighbors and reverse neighbors
    val allNeighbors = (neighbors.toSet ++ oldReverseNeighbors).filter(_ => scala.util.Random.nextFloat() < sampleRate)
    joinNode(newNeighbor, allNeighbors.toSeq, toSend, nodeLocator)
  }
}
