package com.github.julkw.dnsg.actors

import math._
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.julkw.dnsg.actors.Coordinator.CoordinationEvent
import com.github.julkw.dnsg.util.NodeLocator

import scala.language.postfixOps


object SearchOnGraph {

  sealed trait SearchOnGraphEvent

  // setup
  final case class Graph(graph: Map[Int, Seq[Int]], sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class GraphReceived(graphHolder: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class GraphDistribution(nodeLocator: NodeLocator[SearchOnGraphEvent]) extends SearchOnGraphEvent

  // answering queries
  final case class FindNearestNeighbors(query: Seq[Float], k: Int, asker: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class GetNeighbors(index: Int, query: Query, sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class Neighbors(query: Query, index: Int, neighbors: Seq[Int]) extends SearchOnGraphEvent

  final case class KNearestNeighbors(query: Seq[Float], neighbors: Seq[Int]) extends SearchOnGraphEvent

  // safe knng to file
  final case class GetGraph(sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  // data type for more readable code
  protected case class Query(point: Seq[Float], k: Int, asker: ActorRef[SearchOnGraphEvent])

  protected case class QueryCandidate(index: Int, distance: Double, var processed: Boolean)


  def apply(supervisor: ActorRef[CoordinationEvent],
            data: Seq[Seq[Float]]): Behavior[SearchOnGraphEvent] = Behaviors.setup { ctx =>
    ctx.log.info("Started SearchOnGraph")
    Behaviors.setup(ctx => new SearchOnGraph(supervisor, data, ctx).waitForLocalGraph())
  }
}

class SearchOnGraph(supervisor: ActorRef[CoordinationEvent],
                    data: Seq[Seq[Float]],
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
        searchOnGraph(graph, nodeLocator, Map.empty)
    }

  def searchOnGraph(graph: Map[Int, Seq[Int]],
                    nodeLocator: NodeLocator[SearchOnGraphEvent],
                    queries: Map[Query, Seq[QueryCandidate]]): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessage {
      case FindNearestNeighbors(query, k, asker) =>
        ctx.log.info("Received a query")
        // choose node to start search from local nodes
        val startingNodeIndex: Int = graph.keys.head
        val candidateList = Seq(QueryCandidate(startingNodeIndex, euclideanDist(data(startingNodeIndex), query), processed=false))
        // since this node is located locally, just ask self
        ctx.self ! GetNeighbors(startingNodeIndex, Query(query, k, asker), ctx.self)
        val updatedQueries = queries + (Query(query, k, asker) -> candidateList)
        searchOnGraph(graph, nodeLocator, updatedQueries)

      case GetNeighbors(index, query, sender) =>
        sender ! Neighbors(query, index, graph(index))
        searchOnGraph(graph, nodeLocator, queries)

      case Neighbors(query, processedIndex, neighbors) =>
        // update candidates
        val currentCandidates: Seq[QueryCandidate] = queries(query)
        val currentCandidateIndices = currentCandidates.map(_.index)
        // only add candidates that are not already in the candidateList
        val newCandidates = neighbors.diff(currentCandidateIndices).map(
          candidateIndex => QueryCandidate(candidateIndex, euclideanDist(query.point, data(candidateIndex)), processed=false))
        val mergedCandidates = (currentCandidates ++: newCandidates).sortBy(_.distance)
        // set flag for the now processed index to true
        // TODO add check if candidate is found? (Should always be the case)
        mergedCandidates.find(query => query.index == processedIndex).get.processed = true
        val updatedCandidates = mergedCandidates.slice(0, query.k)
        // check if all candidates have been processed
        val nextCandidateToProcess = updatedCandidates.find(query => !query.processed)
        nextCandidateToProcess match {
          case Some(nextCandidate) =>
            // find the neighbors of the next candidate to be processed and update queries
            nodeLocator.findResponsibleActor(data(nextCandidate.index)) ! GetNeighbors(nextCandidate.index, query, ctx.self)
            val updatedQueries = queries + (query -> updatedCandidates)
            searchOnGraph(graph, nodeLocator, updatedQueries)
          case None =>
            // all candidates have been processed
            val nearestNeighbors: Seq[Int] = updatedCandidates.map(_.index)
            query.asker ! KNearestNeighbors(query.point, nearestNeighbors)
            val updatedQueries = queries - query
            searchOnGraph(graph, nodeLocator, updatedQueries)
        }

      case GetGraph(sender) =>
        // send graph to dataHolder
        sender ! Graph(graph, ctx.self)
        searchOnGraph(graph, nodeLocator, queries)
    }

  def euclideanDist(pointX: Seq[Float], pointY: Seq[Float]): Double = {
    sqrt((pointX zip pointY).map { case (x,y) => pow(y - x, 2) }.sum)
  }
}




