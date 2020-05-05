package com.github.julkw.dnsg.actors.createNSG

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.julkw.dnsg.actors.ClusterCoordinator.CoordinationEvent
import com.github.julkw.dnsg.actors.SearchOnGraph
import com.github.julkw.dnsg.actors.SearchOnGraph.{CheckedNodesOnSearch, SearchOnGraphEvent, SortedCheckedNodes}
import com.github.julkw.dnsg.actors.createNSG.NSGMerger.{MergeNSGEvent, ReverseNeighbors}
import com.github.julkw.dnsg.util.{Distance, LocalData, NodeLocator, dNSGSerializable}

import scala.language.postfixOps


object NSGWorker {

  sealed trait BuildNSGEvent extends dNSGSerializable
  // setup
  final case class Responsibility(responsibility: Seq[Int]) extends BuildNSGEvent
  // build NSG
  final case class StartEdgeFindingProcessFor(responsibilityIndex: Int) extends BuildNSGEvent

  final case class WrappedSearchOnGraphEvent(event: SearchOnGraph.SearchOnGraphEvent) extends BuildNSGEvent

  def apply(supervisor: ActorRef[CoordinationEvent],
            data: LocalData[Float],
            navigatingNode: Int,
            maxReverseNeighbors: Int,
            nodeLocator: NodeLocator[SearchOnGraphEvent],
            nsgMerger: ActorRef[MergeNSGEvent]): Behavior[BuildNSGEvent] = Behaviors.setup { ctx =>
    //ctx.log.info("Started NSGWorker")
    Behaviors.setup(ctx =>
      new NSGWorker(supervisor, data, navigatingNode, maxReverseNeighbors, nodeLocator, nsgMerger, ctx).setup())
  }
}

class NSGWorker(supervisor: ActorRef[CoordinationEvent],
                data: LocalData[Float],
                navigatingNode: Int,
                maxReverseNeighbors: Int,
                nodeLocator: NodeLocator[SearchOnGraphEvent],
                nsgMerger: ActorRef[MergeNSGEvent],
                ctx: ActorContext[NSGWorker.BuildNSGEvent]) extends Distance {
  import NSGWorker._

  def setup(): Behavior[BuildNSGEvent] =
    Behaviors.receiveMessagePartial{
      case Responsibility(responsibility) =>
        //ctx.log.info("Received responsibilities")
        val searchOnGraphEventAdapter: ActorRef[SearchOnGraphEvent] =
          ctx.messageAdapter { event => WrappedSearchOnGraphEvent(event)}
        ctx.self ! StartEdgeFindingProcessFor(0)
        buildNSG(responsibility, searchOnGraphEventAdapter)
    }

  def buildNSG(responsibility: Seq[Int],
               searchOnGraphEventAdapter: ActorRef[SearchOnGraphEvent]): Behavior[BuildNSGEvent] =
    Behaviors.receiveMessagePartial{
      case StartEdgeFindingProcessFor(responsibilityIndex) =>
        if (responsibilityIndex < responsibility.length - 1) {
          ctx.self ! StartEdgeFindingProcessFor (responsibilityIndex + 1)
        }
        nodeLocator.findResponsibleActor(responsibility(responsibilityIndex)) !
          CheckedNodesOnSearch(responsibility(responsibilityIndex), navigatingNode, searchOnGraphEventAdapter)
        buildNSG(responsibility, searchOnGraphEventAdapter)

      case WrappedSearchOnGraphEvent(event) =>
        event match {
          case SortedCheckedNodes(queryIndex, checkedNodes) =>
            // check neighbor candidates for conflicts
            var neighbors: Seq[Int] = Seq.empty
            val query = data.get(queryIndex)
            // choose up to maxReverseNeighbors neighbors from checked nodes by checking for conflicts
            var nodeIndex = 0
            // don't make a node its own neighbor
            if (checkedNodes.head == queryIndex) nodeIndex = 1
            while (neighbors.length < maxReverseNeighbors && nodeIndex < checkedNodes.length) {
              val node = data.get(checkedNodes(nodeIndex))
              if (!conflictFound(query, node, neighbors)) {
                neighbors = neighbors :+ checkedNodes(nodeIndex)
              }
              nodeIndex += 1
            }
            nsgMerger ! ReverseNeighbors(queryIndex, neighbors)
        }
        buildNSG(responsibility, searchOnGraphEventAdapter)
    }

  def conflictFound(query: Seq[Float], nodeToTest: Seq[Float], neighborsSoFar: Seq[Int]): Boolean = {
    var conflictFound = false
    val potentialEdgeDist = euclideanDist(query, nodeToTest)
    // check for conflicts (conflict exists if potential Edge is the longest edge in triangle of query, node and neighbor)
    var neighborIndex = 0
    while (!conflictFound && neighborIndex < neighborsSoFar.length) {
      val setNeighbor = data.get(neighborsSoFar(neighborIndex))
      conflictFound = (potentialEdgeDist >= euclideanDist(query, setNeighbor)) &&
        (potentialEdgeDist >= euclideanDist(nodeToTest, setNeighbor))
      neighborIndex += 1
    }
    conflictFound
  }
}



