package com.github.julkw.dnsg.actors

import math._
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.julkw.dnsg.actors.Coordinator.CoordinationEvent
import com.github.julkw.dnsg.actors.createNSG.NSGWorker.{BuildNSGEvent, Responsibility}
import com.github.julkw.dnsg.util.NodeLocator

import scala.language.postfixOps


object SearchOnGraph {

  sealed trait SearchOnGraphEvent

  // setup
  final case class Graph(graph: Map[Int, Seq[Int]], sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class GraphReceived(graphHolder: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class GraphDistribution(nodeLocator: NodeLocator[SearchOnGraphEvent]) extends SearchOnGraphEvent

  // answering queries
  final case class FindNearestNeighbors(query: Seq[Float], asker: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class FindNearestNeighborsStartingFrom(query: Seq[Float], startingPoint: Int, asker: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class CheckedNodesOnSearch(endPoint: Int, startingPoint: Int, asker: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class SortedCheckedNodes(queryIndex: Int, checkedNodes: Seq[Int]) extends SearchOnGraphEvent

  final case class GetNeighbors(index: Int, query: Query, sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class Neighbors(query: Query, index: Int, neighbors: Seq[Int]) extends SearchOnGraphEvent

  final case class KNearestNeighbors(query: Seq[Float], neighbors: Seq[Int]) extends SearchOnGraphEvent

  // send responsiblities to NSG workers
  final case class SendResponsibleIndicesTo(nsgWorker: ActorRef[BuildNSGEvent]) extends SearchOnGraphEvent

  // safe knng to file
  final case class GetGraph(sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  // data type for more readable code
  protected case class Query(point: Seq[Float], asker: ActorRef[SearchOnGraphEvent])

  protected case class QueryCandidate(index: Int, distance: Double, var processed: Boolean)

  protected case class PathQueryInfo(queryIndex: Int, var checkedCandidates: Seq[(Int, Double)])

  def apply(supervisor: ActorRef[CoordinationEvent],
            data: Seq[Seq[Float]],
            k: Int): Behavior[SearchOnGraphEvent] = Behaviors.setup { ctx =>
    ctx.log.info("Started SearchOnGraph")
    Behaviors.setup(ctx => new SearchOnGraph(supervisor, data, k, ctx).waitForLocalGraph())
  }
}

class SearchOnGraph(supervisor: ActorRef[CoordinationEvent],
                    data: Seq[Seq[Float]],
                    k: Int,
                    ctx: ActorContext[SearchOnGraph.SearchOnGraphEvent]) {
  import SearchOnGraph._

  def waitForLocalGraph(): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial{
      case Graph(graph, sender) =>
        ctx.log.info("Received graph")
        sender ! GraphReceived(ctx.self)
        waitForDistributionInfo(graph)
    }

  def waitForDistributionInfo(graph: Map[Int, Seq[Int]]): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial{
      case GraphDistribution(nodeLocator) =>
        ctx.log.info("Received distribution info, ready for queries")
        searchOnGraph(graph, nodeLocator, Map.empty, Map.empty)
    }

  def searchOnGraph(graph: Map[Int, Seq[Int]],
                    nodeLocator: NodeLocator[SearchOnGraphEvent],
                    neighborQueries: Map[Query, Seq[QueryCandidate]],
                    pathQueries: Map[Query, PathQueryInfo]): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessage {
      case FindNearestNeighbors(query, asker) =>
        ctx.log.info("Received a query")
        // choose node to start search from local nodes
        val startingNodeIndex: Int = graph.keys.head
        val candidateList = Seq(QueryCandidate(startingNodeIndex, euclideanDist(data(startingNodeIndex), query), processed=false))
        // since this node is located locally, just ask self
        ctx.self ! GetNeighbors(startingNodeIndex, Query(query, asker), ctx.self)
        searchOnGraph(graph, nodeLocator, neighborQueries + (Query(query, asker) -> candidateList), pathQueries)

      case FindNearestNeighborsStartingFrom(query, startingPoint, asker) =>
        ctx.log.info("Received a query with starting point")
        val candidateList = Seq(QueryCandidate(startingPoint, euclideanDist(data(startingPoint), query), processed=false))
        nodeLocator.findResponsibleActor(data(startingPoint)) ! GetNeighbors(startingPoint, Query(query, asker), ctx.self)
        searchOnGraph(graph, nodeLocator, neighborQueries + (Query(query, asker) -> candidateList), pathQueries)

      case CheckedNodesOnSearch(endPoint, startingPoint, asker) =>
        ctx.log.info("Asked for the checked nodes to use as candidates in building the NSG")
        val query = data(endPoint)
        val candidateList = Seq(QueryCandidate(startingPoint, euclideanDist(data(startingPoint), query), processed=false))
        nodeLocator.findResponsibleActor(data(startingPoint)) ! GetNeighbors(startingPoint, Query(query, asker), ctx.self)
        searchOnGraph(graph, nodeLocator,
          neighborQueries + (Query(query, asker) -> candidateList),
          pathQueries + (Query(query, asker) -> PathQueryInfo(endPoint, Seq.empty)))

      case GetNeighbors(index, query, sender) =>
        sender ! Neighbors(query, index, graph(index))
        searchOnGraph(graph, nodeLocator, neighborQueries, pathQueries)

      case Neighbors(query, processedIndex, neighbors) =>
        // update candidates
        val currentCandidates: Seq[QueryCandidate] = neighborQueries(query)
        val currentCandidateIndices = currentCandidates.map(_.index)
        // only add candidates that are not already in the candidateList
        val newCandidates = neighbors.diff(currentCandidateIndices).map(
          candidateIndex => QueryCandidate(candidateIndex, euclideanDist(query.point, data(candidateIndex)), processed=false))
        val mergedCandidates = (currentCandidates ++: newCandidates).sortBy(_.distance)
        // set flag for the now processed index to true
        val processedCandidate = mergedCandidates.find(query => query.index == processedIndex)
        processedCandidate match {
          case Some(candidate) =>
            candidate.processed = true
            if (pathQueries.contains(query)) {
              // TODO check this works as expected
              pathQueries(query).checkedCandidates = pathQueries(query).checkedCandidates :+ (candidate.index, candidate.distance)
            }
          case None =>
            // this should not happen
            ctx.log.info("Somehow got neighbors for a removed candidate.")
        }

        val updatedCandidates = mergedCandidates.slice(0, k)
        // check if all candidates have been processed
        val nextCandidateToProcess = updatedCandidates.find(query => !query.processed)
        nextCandidateToProcess match {
          case Some(nextCandidate) =>
            // find the neighbors of the next candidate to be processed and update queries
            nodeLocator.findResponsibleActor(data(nextCandidate.index)) ! GetNeighbors(nextCandidate.index, query, ctx.self)
            val updatedQueries = neighborQueries + (query -> updatedCandidates)
            searchOnGraph(graph, nodeLocator, updatedQueries, pathQueries)
          case None =>
            // all candidates have been processed
            if (pathQueries.contains(query)) {
              // send back the checked nodes instead of the result
              val result = pathQueries(query)
              val sortedCandidates = result.checkedCandidates.sortBy(_._2).map(_._1)
              query.asker ! SortedCheckedNodes(result.queryIndex, sortedCandidates)
            } else {
              val nearestNeighbors: Seq[Int] = updatedCandidates.map(_.index)
              query.asker ! KNearestNeighbors(query.point, nearestNeighbors)
            }
            searchOnGraph(graph, nodeLocator, neighborQueries - query, pathQueries - query)
        }

      case GetGraph(sender) =>
        ctx.log.info("Asked for graph info")
        sender ! Graph(graph, ctx.self)
        searchOnGraph(graph, nodeLocator, neighborQueries, pathQueries)

      case SendResponsibleIndicesTo(nsgWorker) =>
        nsgWorker ! Responsibility(graph.keys.toSeq)
        searchOnGraph(graph, nodeLocator, neighborQueries, pathQueries)
    }

  def euclideanDist(pointX: Seq[Float], pointY: Seq[Float]): Double = {
    sqrt((pointX zip pointY).map { case (x,y) => pow(y - x, 2) }.sum)
  }
}




