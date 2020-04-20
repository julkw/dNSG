package com.github.julkw.dnsg.actors.nndescent

import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.TimerScheduler
import com.github.julkw.dnsg.actors.nndescent.KnngWorker.{AddReverseNeighbor, BuildGraphEvent, CorrectFinishedNNDescent, NNDescentTimeout, NNDescentTimerKey, PotentialNeighbors, RemoveReverseNeighbor, timeoutAfter}
import com.github.julkw.dnsg.util.{Distance, NodeLocator}


abstract class Joiner(k: Int, sampleRate: Double, data: Seq[Seq[Float]], supervisor: ActorRef[BuildGraphEvent], timers: TimerScheduler[KnngWorker.BuildGraphEvent]) extends Distance {

  def joinNeighbors(neighbors: Seq[(Int, Double)], nodeLocator: NodeLocator[BuildGraphEvent], g_nodeIndex: Int): Unit = {
    val sampledNeighbors = neighbors.filter(_ => scala.util.Random.nextFloat() < sampleRate)
    val neighborDistances = Array.fill(sampledNeighbors.length){Array.fill(sampledNeighbors.length - 1){(0, 0d)}}
    for (n1 <- 0 until sampledNeighbors.length) {
      for (n2 <- n1 + 1 until sampledNeighbors.length) {
        val neighbor1 = sampledNeighbors(n1)._1
        val neighbor2 = sampledNeighbors(n2)._1
        val dist = euclideanDist(data(neighbor1), data(neighbor2))
        neighborDistances(n1)(n2 - 1) = (neighbor2, dist)
        neighborDistances(n2)(n1) = (neighbor1, dist)
      }
    }
    for (neighborIndex <- 0 until sampledNeighbors.length) {
      val neighbor = sampledNeighbors(neighborIndex)._1
      nodeLocator.findResponsibleActor(data(neighbor)) ! PotentialNeighbors(neighbor, neighborDistances(neighborIndex), g_nodeIndex)
    }
  }


  def joinReverseNeighbor(neighbors: Seq[(Int, Double)], oldReverseNeighbors: Set[Int], g_nodeIndex: Int, neighborIndex: Int, nodeLocator: NodeLocator[BuildGraphEvent]): Unit = {
    // all neighbors without duplicates and without the new neighbor being introduced
    val allNeighbors = neighbors.map(_._1).toSet ++ oldReverseNeighbors - neighborIndex
    // calculate distances
    val potentialNeighbors = allNeighbors.filter(_ => scala.util.Random.nextFloat() < sampleRate).map(index =>
      (index, euclideanDist(data(index), data(neighborIndex)))).toSeq
    // introduce the new neighbor to all other neighbors
    nodeLocator.findResponsibleActor(data(neighborIndex)) ! PotentialNeighbors(neighborIndex, potentialNeighbors, g_nodeIndex)
    // introduce all other neighbors to the new neighbor
    potentialNeighbors.foreach { case(index, distance) =>
      nodeLocator.findResponsibleActor(data(index)) ! PotentialNeighbors(index, Seq((neighborIndex, distance)), g_nodeIndex)
    }
  }

  def updateNeighbors(potentialNeighbors: Seq[(Int, Double)],
                      currentNeighbors: Seq[(Int, Double)],
                      reverseNeighbors: Set[Int],
                      g_node: Int,
                      senderIndex: Int,
                      nodeLocator: NodeLocator[BuildGraphEvent]): Seq[(Int, Double)] = {
    val currentMaxDist: Double = currentNeighbors(currentNeighbors.length - 1)._2
    val probableNeighbors: Set[(Int, Double)] = potentialNeighbors.filter(_._2 < currentMaxDist).toSet -- currentNeighbors - ((g_node, 0.0))
    if (probableNeighbors.isEmpty) {
      // nothing changes
      Seq.empty
    } else {
      if (timers.isTimerActive(NNDescentTimerKey)) {
        timers.cancel(NNDescentTimerKey)
      } else {
        // if the timer is inactive, it has already run out and the actor has mistakenly told its supervisor that it is done
        supervisor ! CorrectFinishedNNDescent
      }
      // update neighbors
      val mergedNeighbors = (currentNeighbors ++: probableNeighbors.toSeq).sortBy(_._2)
      val updatedNeighbors = mergedNeighbors.slice(0, k)
      // update the reverse neighbors of changed neighbors
      val newNeighbors: Set[Int] = probableNeighbors.intersect(updatedNeighbors.toSet).map(_._1)
      newNeighbors.foreach {index =>
        nodeLocator.findResponsibleActor(data(index)) ! AddReverseNeighbor(index, g_node)
      }
      val removedNeighbors = mergedNeighbors.slice(k, mergedNeighbors.length).intersect(currentNeighbors).map(_._1)
      removedNeighbors.foreach {index =>
        nodeLocator.findResponsibleActor(data(index)) ! RemoveReverseNeighbor(index, g_node)
      }

      // send out new potential neighbors (join the remaining old neighbors with the new ones)
      val keptNeighbors = (updatedNeighbors.intersect(currentNeighbors.diff(Seq(senderIndex))).map(_._1) ++: reverseNeighbors).filter(_ => scala.util.Random.nextFloat() < sampleRate)
      // those who need to be joined with the kept neighbors
      val newNeighborsToPair = newNeighbors -- reverseNeighbors.filter(_ => scala.util.Random.nextFloat() < sampleRate)
      // first calculate distances to prevent double calculation
      val potentialPairs = for {
        n1 <- newNeighborsToPair
        n2 <- keptNeighbors
        dist = euclideanDist(data(n1), data(n2))
      } yield(n1, n2, dist)
      // introduce old neighbors to new neighbors
      // (the new neighbors do not have to be joined, as that will already have happened at the g_node the message originated from)
      newNeighborsToPair.foreach { index =>
        val thisNodesPotentialNeighbors = potentialPairs.collect{case (n1, n2, dist) if n1== index => (n2, dist)}.toSeq
        nodeLocator.findResponsibleActor(data(index)) ! PotentialNeighbors(index, thisNodesPotentialNeighbors, g_node)
      }
      // introduce old neighbors to new neighbors
      keptNeighbors.foreach {index =>
        val thisNodesPotentialNeighbors = potentialPairs.collect{case (n1, n2, dist) if n2 == index => (n1, dist)}.toSeq
        nodeLocator.findResponsibleActor(data(index)) ! PotentialNeighbors(index, thisNodesPotentialNeighbors, g_node)
      }
      // something changed so reset the NNDescent Timer
      timers.startSingleTimer(NNDescentTimerKey, NNDescentTimeout, timeoutAfter)
      updatedNeighbors
    }
  }
}
