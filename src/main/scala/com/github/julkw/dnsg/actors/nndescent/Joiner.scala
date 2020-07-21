package com.github.julkw.dnsg.actors.nndescent

import com.github.julkw.dnsg.actors.nndescent.KnngWorker.BuildKNNGEvent
import com.github.julkw.dnsg.actors.nndescent.NNDescentMessageBuffer.{JoinNodes, PotentialNeighbor}
import com.github.julkw.dnsg.util.Data.LocalData
import com.github.julkw.dnsg.util.{Distance, NodeLocator}

import scala.collection.IndexedSeqView

abstract class Joiner(maxIterations: Int, data: LocalData[Float]) extends Distance {

  def joinNodePair(n1Index: Int,
                   n2Index: Int,
                   iteration: Int,
                   joiningNode: Int,
                   toSend: NNDescentMessageBuffer,
                   nodeLocator: NodeLocator[BuildKNNGEvent]): Unit = {
    if (data.isLocal(n1Index) && data.isLocal(n2Index)) {
      joinLocals(n1Index, data.get(n1Index), n2Index, data.get(n2Index), iteration, joiningNode, toSend, nodeLocator)
    } else {
      toSend.addNodeMessage(JoinNodes(n1Index, n2Index, iteration), nodeLocator.findResponsibleActor(n1Index), joiningNode)
    }
  }

  def joinLocals(n1Index: Int,
                 n1Location: Array[Float],
                 n2Index: Int,
                 n2Location: Array[Float],
                 iteration: Int,
                 joiningNode: Int,
                 toSend: NNDescentMessageBuffer,
                 nodeLocator: NodeLocator[BuildKNNGEvent]): Unit = {
    val dist = euclideanDist(n1Location, n2Location)
    if (joiningNode >= 0) {
      toSend.addNodeMessage(PotentialNeighbor(n1Index, n2Index, dist, iteration), nodeLocator.findResponsibleActor(n1Index), joiningNode)
      toSend.addNodeMessage(PotentialNeighbor(n2Index, n1Index, dist, iteration), nodeLocator.findResponsibleActor(n2Index), joiningNode)
    } else {
      toSend.addNodeIndependentMessage(PotentialNeighbor(n1Index, n2Index, dist, iteration), nodeLocator.findResponsibleActor(n1Index))
      toSend.addNodeIndependentMessage(PotentialNeighbor(n2Index, n1Index, dist, iteration), nodeLocator.findResponsibleActor(n2Index))
    }

  }

  def joinNode(node: Int, neighbors: IndexedSeqView[Int],
               reverseNeighbors: Set[Int],
               lastIteration: Int,
               joiningNode: Int,
               toSend: NNDescentMessageBuffer,
               nodeLocator: NodeLocator[BuildKNNGEvent]): Unit = {
    if (lastIteration < maxIterations) {
      neighbors.foreach { neighbor =>
        if (neighbor != node && !reverseNeighbors.contains(neighbor)) {
          joinNodePair(neighbor, node, lastIteration + 1, joiningNode, toSend, nodeLocator)
        }
      }
      reverseNeighbors.foreach { neighbor =>
        if (neighbor != node) {
          joinNodePair(neighbor, node, lastIteration + 1, joiningNode, toSend, nodeLocator)
        }
      }
    }
  }

  def joinNeighbors(neighbors: IndexedSeqView[Int],
                    iteration: Int,
                    joiningNode: Int,
                    toSend: NNDescentMessageBuffer,
                    nodeLocator: NodeLocator[BuildKNNGEvent]): Unit = {
    val numNeighbors = neighbors.length
    neighbors.indices.foreach { neighborIndex =>
      joinNode(neighbors(neighborIndex), neighbors.slice(neighborIndex, numNeighbors), Set.empty, iteration, joiningNode, toSend, nodeLocator)
    }
  }

}
