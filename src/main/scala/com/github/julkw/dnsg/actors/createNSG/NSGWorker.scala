package com.github.julkw.dnsg.actors.createNSG

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.julkw.dnsg.actors.ClusterCoordinator.CoordinationEvent
import com.github.julkw.dnsg.actors.SearchOnGraphActor.{CheckedNodesOnSearch, SearchOnGraphEvent}
import com.github.julkw.dnsg.actors.createNSG.NSGMerger.{MergeNSGEvent, ReverseNeighbors}
import com.github.julkw.dnsg.util.{Distance, LocalData, NodeLocator, dNSGSerializable}

import scala.language.postfixOps


object NSGWorker {

  sealed trait BuildNSGEvent extends dNSGSerializable
  // setup
  final case class Responsibility(responsibility: Seq[Int]) extends BuildNSGEvent
  // build NSG
  final case class StartEdgeFindingProcessFor(responsibilityIndex: Int) extends BuildNSGEvent

  final case class SortedCheckedNodes(queryIndex: Int, checkedNodes: Seq[(Int, Seq[Float])]) extends BuildNSGEvent

  def apply(supervisor: ActorRef[CoordinationEvent],
            data: LocalData[Float],
            navigatingNode: Int,
            k: Int,
            maxReverseNeighbors: Int,
            nodeLocator: NodeLocator[SearchOnGraphEvent],
            nsgMerger: ActorRef[MergeNSGEvent]): Behavior[BuildNSGEvent] = Behaviors.setup { ctx =>
    //ctx.log.info("Started NSGWorker")
    Behaviors.setup(ctx =>
      new NSGWorker(supervisor, data, navigatingNode, k, maxReverseNeighbors, nodeLocator, nsgMerger, ctx).setup())
  }
}

class NSGWorker(supervisor: ActorRef[CoordinationEvent],
                data: LocalData[Float],
                navigatingNode: Int,
                k: Int,
                maxReverseNeighbors: Int,
                nodeLocator: NodeLocator[SearchOnGraphEvent],
                nsgMerger: ActorRef[MergeNSGEvent],
                ctx: ActorContext[NSGWorker.BuildNSGEvent]) extends Distance {

  import NSGWorker._

  def setup(): Behavior[BuildNSGEvent] =
    Behaviors.receiveMessagePartial {
      case Responsibility(responsibility) =>
        //ctx.log.info("Received responsibilities")
        ctx.self ! StartEdgeFindingProcessFor(0)
        buildNSG(responsibility)
    }

  def buildNSG(responsibility: Seq[Int]): Behavior[BuildNSGEvent] =
    Behaviors.receiveMessagePartial {
      case StartEdgeFindingProcessFor(responsibilityIndex) =>
        if (responsibilityIndex < responsibility.length - 1) {
          ctx.self ! StartEdgeFindingProcessFor(responsibilityIndex + 1)
        }
        val nodeToProcess = responsibility(responsibilityIndex)
        nodeLocator.findResponsibleActor(nodeToProcess) !
          CheckedNodesOnSearch(nodeToProcess, navigatingNode, k, ctx.self)
        buildNSG(responsibility)

      case SortedCheckedNodes(queryIndex, checkedNodes) =>
        // check neighbor candidates for conflicts
        // TODO In cluster, I do not get this for all nodes. WHY? :(
        // Maybe there are messages lost of something in the SearchOnGraph Actor?
        //ctx.log.info("Got sorted checked nodes for {}", queryIndex)
        var neighborIndices: Seq[Int] = Seq.empty
        var neighborLocations: Seq[Seq[Float]] = Seq.empty
        val query = data.get(queryIndex)
        // choose up to maxReverseNeighbors neighbors from checked nodes by checking for conflicts
        var nodeIndex = 0
        // don't make a node its own neighbor
        if (checkedNodes.head._1 == queryIndex) nodeIndex = 1
        while (neighborIndices.length < maxReverseNeighbors && nodeIndex < checkedNodes.length) {
          val node = checkedNodes(nodeIndex)._2
          if (!conflictFound(query, node, neighborLocations)) {
            neighborIndices = neighborIndices :+ checkedNodes(nodeIndex)._1
            neighborLocations = neighborLocations :+ checkedNodes(nodeIndex)._2
          }
          nodeIndex += 1
        }
        nsgMerger ! ReverseNeighbors(queryIndex, neighborIndices)
        buildNSG(responsibility)
  }

  def conflictFound(query: Seq[Float], nodeToTest: Seq[Float], neighborsSoFar: Seq[Seq[Float]]): Boolean = {
    var conflictFound = false
    val potentialEdgeDist = euclideanDist(query, nodeToTest)
    // check for conflicts (conflict exists if potential Edge is the longest edge in triangle of query, node and neighbor)
    var neighborIndex = 0
    while (!conflictFound && neighborIndex < neighborsSoFar.length) {
      val setNeighbor = neighborsSoFar(neighborIndex)
      conflictFound = (potentialEdgeDist >= euclideanDist(query, setNeighbor)) &&
        (potentialEdgeDist >= euclideanDist(nodeToTest, setNeighbor))
      neighborIndex += 1
    }
    conflictFound
  }
}



