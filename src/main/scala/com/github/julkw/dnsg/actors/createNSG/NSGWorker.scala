package com.github.julkw.dnsg.actors.createNSG

import math._
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.julkw.dnsg.actors.Coordinator.CoordinationEvent
import com.github.julkw.dnsg.actors.SearchOnGraph
import com.github.julkw.dnsg.actors.SearchOnGraph.{CheckedNodesOnSearch, SearchOnGraphEvent, SortedCheckedNodes}
import com.github.julkw.dnsg.util.NodeLocator

import scala.language.postfixOps


object NSGWorker {

  sealed trait BuildNSGEvent
  // setup
  final case class Responsibility(responsibility: Seq[Int]) extends BuildNSGEvent
  // build NSG
  final case class StartEdgeFindingProcessFor(responsibilityIndex: Int) extends BuildNSGEvent

  final case class WrappedSearchOnGraphEvent(event: SearchOnGraph.SearchOnGraphEvent) extends BuildNSGEvent

  def apply(supervisor: ActorRef[CoordinationEvent],
            data: Seq[Seq[Float]],
            navigatingNode: Int,
            nodeLocator: NodeLocator[SearchOnGraphEvent]): Behavior[BuildNSGEvent] = Behaviors.setup { ctx =>
    ctx.log.info("Started NSGWorker")
    Behaviors.setup(ctx => new NSGWorker(supervisor, data, navigatingNode, nodeLocator, ctx).setup())
  }
}

class NSGWorker(supervisor: ActorRef[CoordinationEvent],
                data: Seq[Seq[Float]],
                navigatingNode: Int,
                nodeLocator: NodeLocator[SearchOnGraphEvent],
                ctx: ActorContext[NSGWorker.BuildNSGEvent]) {
  import NSGWorker._

  def setup(): Behavior[BuildNSGEvent] =
    Behaviors.receiveMessagePartial{
      case Responsibility(responsibility) =>
        ctx.log.info("Received responsibilities")
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
        nodeLocator.findResponsibleActor(data(responsibility(responsibilityIndex))) !
          CheckedNodesOnSearch(responsibility(responsibilityIndex), navigatingNode, searchOnGraphEventAdapter)
        buildNSG(responsibility, searchOnGraphEventAdapter)

      case WrappedSearchOnGraphEvent(event) =>
        event match {
          case SortedCheckedNodes(queryIndex, checkedNodes) =>
            ctx.log.info("Received candidates for NSG")
            // check neighbor candidates for conflicts
            var neighbors: Seq[Int] = Seq.empty
            val query = data(queryIndex)
            checkedNodes.foreach { nodeIndex =>
              // decide whether to add the edge (node->query) to the NSG
              var conflictFound = false
              var neighborIndex = 0
              val potentialEdgeDist = euclideanDist(query, data(nodeIndex))
              // check for conflicts (conflict exists if potential Edge is the longest edge in triangle of query, node and neighbor)
              while (!conflictFound && neighborIndex < neighbors.length) {
                conflictFound = (potentialEdgeDist >= euclideanDist(query, data(neighborIndex))) &&
                  (potentialEdgeDist >= euclideanDist(data(nodeIndex), data(neighborIndex)))
                neighborIndex += 1
              }
              if (!conflictFound) {
                neighbors = neighbors :+ nodeIndex
              }
            }
            // TODO update graph with neighbors (but reverse, every neighbor gets queryIndex added as neighbor)
            // TODO this should be done in mergeNSG actor, because this actor does not hold all the nodes that will get a new edge
            ctx.log.info("Found {} neighbors for node {}", neighbors.length, queryIndex)
        }
        buildNSG(responsibility, searchOnGraphEventAdapter)
    }

  // TODO move to util
  def euclideanDist(pointX: Seq[Float], pointY: Seq[Float]): Double = {
    sqrt((pointX zip pointY).map { case (x,y) => pow(y - x, 2) }.sum)
  }
}



