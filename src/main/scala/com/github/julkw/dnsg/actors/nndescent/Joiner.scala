package com.github.julkw.dnsg.actors.nndescent

import akka.actor.typed.ActorRef
import com.github.julkw.dnsg.actors.nndescent.KnngWorker.BuildKNNGEvent
import com.github.julkw.dnsg.actors.nndescent.NNDInfo.{JoinNodes, PotentialNeighbor}
import com.github.julkw.dnsg.util.Data.CacheData
import com.github.julkw.dnsg.util.{Distance, NodeLocator}

import scala.collection.IndexedSeqView


abstract class Joiner(maxIterations: Int, data: CacheData[Float]) extends Distance {

  def joinNodePair(n1Index: Int,
                   n2Index: Int,
                   iteration: Int,
                   toSend: Map[ActorRef[BuildKNNGEvent], NNDInfo],
                   nodeLocator: NodeLocator[BuildKNNGEvent]): Unit = {
    if (data.isLocal(n1Index) && data.isLocal(n2Index)) {
      joinLocals(n1Index, data.get(n1Index), n2Index, data.get(n2Index), iteration, toSend, nodeLocator)
    } else {
      toSend(nodeLocator.findResponsibleActor(n1Index)).addMessage(JoinNodes(n1Index, n2Index, iteration))
    }
  }

  def joinLocals(n1Index: Int,
                 n1Location: Seq[Float],
                 n2Index: Int,
                 n2Location: Seq[Float],
                 iteration: Int,
                 toSend: Map[ActorRef[BuildKNNGEvent], NNDInfo],
                 nodeLocator: NodeLocator[BuildKNNGEvent]): Unit = {
    val dist = euclideanDist(n1Location, n2Location)
    toSend(nodeLocator.findResponsibleActor(n1Index)).addMessage(PotentialNeighbor(n1Index, n2Index, dist, iteration))
    toSend(nodeLocator.findResponsibleActor(n2Index)).addMessage(PotentialNeighbor(n2Index, n1Index, dist, iteration))
  }

  def joinNode(node: Int, neighbors: IndexedSeqView[Int], reverseNeighbors: Set[Int], lastIteration: Int, toSend: Map[ActorRef[BuildKNNGEvent], NNDInfo], nodeLocator: NodeLocator[BuildKNNGEvent]): Unit = {
    if (lastIteration < maxIterations) {
      neighbors.foreach { neighbor =>
        if (neighbor != node && !reverseNeighbors.contains(neighbor)) {
          joinNodePair(neighbor, node, lastIteration + 1, toSend, nodeLocator)
        }
      }
      reverseNeighbors.foreach { neighbor =>
        if (neighbor != node) {
          joinNodePair(neighbor, node, lastIteration + 1, toSend, nodeLocator)
        }
      }
    }
  }

  def joinNeighbors(neighbors: IndexedSeqView[Int], iteration: Int, toSend: Map[ActorRef[BuildKNNGEvent], NNDInfo], nodeLocator: NodeLocator[BuildKNNGEvent]): Unit = {
    val numNeighbors = neighbors.length
    neighbors.indices.foreach { neighborIndex =>
      joinNode(neighbors(neighborIndex), neighbors.slice(neighborIndex, numNeighbors), Set.empty, iteration, toSend, nodeLocator)
    }
  }

}
