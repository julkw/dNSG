package com.github.julkw.dnsg.actors.SearchOnGraph

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.{CoordinationEvent, DoneWithRedistribution, KNearestNeighbors, NSGonSOG, SearchOnGraphDistributionInfo}
import com.github.julkw.dnsg.actors.Coordinators.GraphConnectorCoordinator.{ConnectionCoordinationEvent, ReceivedNewEdge}
import com.github.julkw.dnsg.actors.{GraphConnector, GraphRedistributer}
import com.github.julkw.dnsg.actors.createNSG.NSGMerger.{GetPartialGraph, MergeNSGEvent}
import com.github.julkw.dnsg.actors.createNSG.NSGWorker.{BuildNSGEvent, Responsibility}
import com.github.julkw.dnsg.util.Data.{CacheData, LocalData}
import com.github.julkw.dnsg.util._

import scala.language.postfixOps

object SearchOnGraphActor {

  sealed trait SearchOnGraphEvent extends dNSGSerializable

  // setup
  final case class Graph(graph: Map[Int, Seq[Int]], sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class GraphAndData(graph: Map[Int, Seq[Int]], cacheData: CacheData[Float], sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class GraphReceived(graphHolder: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class GraphDistribution(nodeLocator: NodeLocator[ActorRef[SearchOnGraphEvent]]) extends SearchOnGraphEvent

  // TODO call and use these three
  final case object StartGraphRedistribution extends SearchOnGraphEvent

  final case class RedistributeGraph(nodeAssignments: NodeLocator[Set[ActorRef[SearchOnGraphEvent]]]) extends SearchOnGraphEvent

  final case class PartialGraph(partialGraph: Map[Int, Seq[Int]]) extends SearchOnGraphEvent

  final case class UpdatedLocalData(data: LocalData[Float]) extends  SearchOnGraphEvent

  // queries
  final case class FindNearestNeighbors(query: Seq[Float], k: Int, asker: ActorRef[CoordinationEvent]) extends SearchOnGraphEvent

  final case class FindNearestNeighborsStartingFrom(query: Seq[Float], startingPoint: Int, k: Int, asker: ActorRef[CoordinationEvent]) extends SearchOnGraphEvent

  final case class CheckedNodesOnSearch(endPoint: Int, startingPoint: Int, neighborsWanted: Int, asker: ActorRef[BuildNSGEvent]) extends SearchOnGraphEvent

  // search
  final case class GetNeighbors(index: Int, queryId: Int, sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class Neighbors(queryId: Int, index: Int, neighbors: Seq[Int]) extends SearchOnGraphEvent

  final case class GetLocation(index: Int, sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class Location(index: Int, location: Seq[Float]) extends SearchOnGraphEvent

  final case class ReaskForLocation(index: Int) extends SearchOnGraphEvent

  // connectivity

  final case class ConnectGraph(graphConnectorSupervisor: ActorRef[ConnectionCoordinationEvent]) extends SearchOnGraphEvent

  final case class AddToGraph(startNode: Int, endNode: Int, sender: ActorRef[ConnectionCoordinationEvent]) extends SearchOnGraphEvent

  // send responsiblities to NSG workers
  final case class SendResponsibleIndicesTo(nsgWorker: ActorRef[BuildNSGEvent]) extends SearchOnGraphEvent
  // get NSG from NSGMerger
  final case class GetNSGFrom(nsgMerger: ActorRef[MergeNSGEvent]) extends SearchOnGraphEvent

  // safe knng to file
  final case class GetGraph(sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent


  def apply(clusterCoordinator: ActorRef[CoordinationEvent]): Behavior[SearchOnGraphEvent] = Behaviors.setup { ctx =>
    Behaviors.withTimers(timers =>
      new SearchOnGraphActor(clusterCoordinator, new WaitingOnLocation, timers, ctx).waitForLocalGraph()
    )
  }
}

class SearchOnGraphActor(clusterCoordinator: ActorRef[CoordinationEvent],
                         waitingOnLocation: WaitingOnLocation,
                         timers: TimerScheduler[SearchOnGraphActor.SearchOnGraphEvent],
                         ctx: ActorContext[SearchOnGraphActor.SearchOnGraphEvent]) extends SearchOnGraph(clusterCoordinator, waitingOnLocation, timers, ctx) {
  import SearchOnGraphActor._

  def waitForLocalGraph(): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial{
      case GraphAndData(graph, localData, sender) =>
        //sender ! GraphReceived(ctx.self)
        clusterCoordinator ! SearchOnGraphDistributionInfo(graph.keys.toSeq, ctx.self)
        waitForDistributionInfo(graph, localData)
    }

  def waitForDistributionInfo(graph: Map[Int, Seq[Int]], data: CacheData[Float]): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial{
      case GraphDistribution(nodeLocator) =>
        searchOnGraph(graph, data, nodeLocator, Map.empty, Map.empty, -1)
    }

  def searchOnGraph(graph: Map[Int, Seq[Int]],
                    data: CacheData[Float],
                    nodeLocator: NodeLocator[ActorRef[SearchOnGraphEvent]],
                    neighborQueries: Map[Int, QueryInfo],
                    respondTo: Map[Int, ActorRef[CoordinationEvent]],
                    lastIdUsed: Int): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial {
      case FindNearestNeighbors(query, k, asker) =>
        ctx.log.info("Asked to find navigating node")
        // choose node to start search from local nodes
        val startingNodeIndex: Int = graph.keys.head
        val queryInfo = QueryInfo(query, k, Seq(QueryCandidate(startingNodeIndex, euclideanDist(data.get(startingNodeIndex), query), processed = false)), 0)
        val queryId = lastIdUsed + 1
        // since this node is located locally, just ask self
        ctx.self ! GetNeighbors(startingNodeIndex, queryId, ctx.self)
        searchOnGraph(graph, data, nodeLocator, neighborQueries + (queryId -> queryInfo), respondTo + (queryId -> asker), queryId)

      case FindNearestNeighborsStartingFrom(query, startingPoint, k, asker) =>
        val queryId = lastIdUsed + 1
        val queryInfo = if (data.isLocal(startingPoint)) {
          val location = data.get(startingPoint)
          nodeLocator.findResponsibleActor(startingPoint) ! GetNeighbors(startingPoint, queryId, ctx.self)
          QueryInfo(query, k, Seq(QueryCandidate(startingPoint, euclideanDist(location, query), processed = false)), 0)
        } else {
          val qi = QueryInfo(query, k, Seq.empty, 0)
          askForLocation(startingPoint, queryId, qi, nodeLocator)
          qi
        }
        nodeLocator.findResponsibleActor(startingPoint) ! GetNeighbors(startingPoint, queryId, ctx.self)
        searchOnGraph(graph, data, nodeLocator, neighborQueries + (queryId -> queryInfo), respondTo + (queryId -> asker), queryId)

      case GetNeighbors(index, queryId, sender) =>
        sender ! Neighbors(queryId, index, graph(index))
        searchOnGraph(graph, data, nodeLocator, neighborQueries, respondTo, lastIdUsed)

      case Neighbors(queryId, processedIndex, neighbors) =>
        if (neighborQueries.contains(queryId)) {
          val queryInfo = neighborQueries(queryId)
          updateCandidates(queryInfo, queryId, processedIndex, neighbors.diff(queryInfo.candidates), nodeLocator, data)
          // check if all candidates have been processed
          val nextCandidateToProcess = queryInfo.candidates.find(query => !query.processed)
          nextCandidateToProcess match {
            case Some(nextCandidate) =>
              // find the neighbors of the next candidate to be processed and update queries
              nodeLocator.findResponsibleActor(nextCandidate.index) ! GetNeighbors(nextCandidate.index, queryId, ctx.self)
              searchOnGraph(graph, data, nodeLocator, neighborQueries, respondTo, lastIdUsed)
            case None =>
              if (neighborQueries(queryId).waitingOn > 0) {
                // do nothing for now
                searchOnGraph(graph, data, nodeLocator, neighborQueries, respondTo, lastIdUsed)
              } else {
                val finalNeighbors = queryInfo.candidates.map(_.index)
                respondTo(queryId) ! KNearestNeighbors(queryInfo.query, finalNeighbors)
                searchOnGraph(graph, data, nodeLocator, neighborQueries - queryId, respondTo - queryId, lastIdUsed)
              }
          }
        } else {
          // already done with this query, can ignore this message
          searchOnGraph(graph, data, nodeLocator, neighborQueries, respondTo, lastIdUsed)
        }

      case GetLocation(index, sender) =>
        sender ! Location(index, data.get(index))
        searchOnGraph(graph, data, nodeLocator, neighborQueries, respondTo, lastIdUsed)

      case ReaskForLocation(index) =>
        ctx.log.info("Still haven't received the location of {}. Sending another request.", index)
        nodeLocator.findResponsibleActor(index) ! GetLocation(index, ctx.self)
        searchOnGraph(graph, data, nodeLocator, neighborQueries, respondTo, lastIdUsed)

      case Location(index, location) =>
        timers.cancel(LocationTimerKey(index))
        var removedQueries: Set[Int] = Set.empty
        data.add(index, location)
        waitingOnLocation.received(index).foreach {queryId =>
          if (neighborQueries.contains(queryId)) {
            val queryInfo = neighborQueries(queryId)
            queryInfo.waitingOn -= 1
            val queryFinished = addCandidate(queryInfo, queryId, index, location, nodeLocator)
            if (queryFinished) {
              val finalNeighbors = queryInfo.candidates.map(_.index)
              respondTo(queryId) ! KNearestNeighbors(queryInfo.query, finalNeighbors)
              removedQueries = removedQueries + queryId
            }
          }
        }
        searchOnGraph(graph, data, nodeLocator, neighborQueries -- removedQueries, respondTo -- removedQueries, lastIdUsed)

      case AddToGraph(startNode, endNode, sender) =>
        ctx.log.info("Add edge to graph to ensure connectivity")
        sender ! ReceivedNewEdge
        val newNeighbors = graph(startNode) :+ endNode
        searchOnGraph(graph + (startNode -> newNeighbors), data, nodeLocator, neighborQueries, respondTo, lastIdUsed)

      case GetGraph(sender) =>
        //ctx.log.info("Asked for graph info")
        sender ! Graph(graph, ctx.self)
        searchOnGraph(graph, data, nodeLocator, neighborQueries, respondTo, lastIdUsed)

      case SendResponsibleIndicesTo(nsgWorker) =>
        nsgWorker ! Responsibility(graph.keys.toSeq)
        searchOnGraphForNSG(graph, data, nodeLocator, Map.empty, Map.empty, QueryResponseLocations(data))

      case StartGraphRedistribution =>
        ctx.spawn(GraphRedistributer(graph, clusterCoordinator), name="GraphRedistributer")
        waitForRedistributionResults(graph, data)

      case ConnectGraph(graphConnectorSupervisor) =>
        ctx.log.info("Told to connect the graph")
        ctx.spawn(GraphConnector(data.data, graph, graphConnectorSupervisor), name="graphConnector")
        searchOnGraph(graph, data, nodeLocator, neighborQueries, respondTo, lastIdUsed)
    }

  def waitForRedistributionResults(oldGraph: Map[Int, Seq[Int]], oldData: CacheData[Float]): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial {
      case RedistributeGraph(nodeAssignments) =>
        val nodesExpected = nodeAssignments.locationData.count(assignees => assignees.contains(ctx.self))
        // TODO send out partialGraphs in more steps?
        oldGraph.keys.groupBy(index => nodeAssignments.findResponsibleActor(index)).foreach { case (assignees, nodes) =>
          val partialGraph = nodes.map(node => node -> oldGraph(node)).toMap
          assignees.foreach(searchOnGraphActor => searchOnGraphActor ! PartialGraph(partialGraph))
        }
        // TODO use something other(more random) than head / if self in set use self?
        val nodeLocator = NodeLocator(nodeAssignments.locationData.map(_.head))
        redistributeGraph(oldGraph, nodeLocator, Map.empty, nodesExpected, oldData, false)
    }

  def redistributeGraph(oldGraph: Map[Int, Seq[Int]],
                        nodeLocator: NodeLocator[ActorRef[SearchOnGraphEvent]],
                        newGraph: Map[Int, Seq[Int]],
                        nodesExpected: Int,
                        data: CacheData[Float],
                        dataUpdated: Boolean): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial {
      case UpdatedLocalData(newData) =>
        // TODO Cache size from settings?
      if (newGraph.size == nodesExpected) {
          clusterCoordinator ! DoneWithRedistribution
          searchOnGraph(newGraph, CacheData(0, newData), nodeLocator, Map.empty, Map.empty, -1)
        } else {
        redistributeGraph(oldGraph, nodeLocator, newGraph, nodesExpected, CacheData(0, newData), true)
      }

      case PartialGraph(partialGraph) =>
        val updatedGraph = newGraph ++ partialGraph
        if (updatedGraph.size == nodesExpected && dataUpdated) {
          clusterCoordinator ! DoneWithRedistribution
          searchOnGraph(updatedGraph, data, nodeLocator, Map.empty, Map.empty, -1)
        }
        redistributeGraph(oldGraph, nodeLocator, updatedGraph, nodesExpected, data, dataUpdated)
  }

  def searchOnGraphForNSG(graph: Map[Int, Seq[Int]],
                          data: CacheData[Float],
                          nodeLocator: NodeLocator[ActorRef[SearchOnGraphEvent]],
                          pathQueries: Map[Int, QueryInfo],
                          respondTo: Map[Int, ActorRef[BuildNSGEvent]],
                          responseLocations: QueryResponseLocations[Float]): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial {
      case CheckedNodesOnSearch(endPoint, startingPoint, neighborsWanted, asker) =>
        // the end point should always be local, because that is how the SoG Actor is chosen
        val query = data.get(endPoint)
        val queryId = endPoint
        // starting point is navigating node, so as of yet not always local
        val pathQueryInfo = if (responseLocations.hasLocation(startingPoint)) {
          val location = responseLocations.location(startingPoint)
          responseLocations.addedToCandidateList(startingPoint, location)
          nodeLocator.findResponsibleActor(startingPoint) ! GetNeighbors(startingPoint, queryId, ctx.self)
          QueryInfo(query, neighborsWanted, Seq(QueryCandidate(startingPoint, euclideanDist(location, query), processed = false)), 0)
        } else {
          val queryInfo = QueryInfo(query, neighborsWanted, Seq.empty, 0)
          askForLocation(startingPoint, queryId, queryInfo, nodeLocator)
          queryInfo
        }
        searchOnGraphForNSG(graph, data, nodeLocator,
          pathQueries + (queryId -> pathQueryInfo),
          respondTo  + (queryId -> asker),
          responseLocations)

      case GetNeighbors(index, query, sender) =>
        sender ! Neighbors(query, index, graph(index))
        searchOnGraphForNSG(graph, data, nodeLocator, pathQueries, respondTo, responseLocations)

      case Neighbors(queryId, processedIndex, neighbors) =>
        val queryInfo = pathQueries(queryId)
        updatePathCandidates(queryInfo,  queryId, processedIndex, neighbors, responseLocations, nodeLocator)
        // check if all candidates have been processed
        val nextCandidateToProcess = queryInfo.candidates.find(query => !query.processed)
        nextCandidateToProcess match {
          case Some(nextCandidate) =>
            // find the neighbors of the next candidate to be processed and update queries
            nodeLocator.findResponsibleActor(nextCandidate.index) ! GetNeighbors(nextCandidate.index, queryId, ctx.self)
            searchOnGraphForNSG(graph, data, nodeLocator, pathQueries, respondTo, responseLocations)
          case None =>
            if (pathQueries(queryId).waitingOn > 0) {
              // do nothing for now
              searchOnGraphForNSG(graph, data, nodeLocator, pathQueries, respondTo, responseLocations)
            } else {
              sendResults(queryId, queryInfo, responseLocations, respondTo(queryId))
              searchOnGraphForNSG(graph, data, nodeLocator, pathQueries - queryId, respondTo - queryId, responseLocations)
            }
        }

      case GetLocation(index, sender) =>
        sender ! Location(index, data.get(index))
        searchOnGraphForNSG(graph, data, nodeLocator, pathQueries, respondTo, responseLocations)

      case ReaskForLocation(index) =>
        ctx.log.info("Still haven't received the location of {}. Sending another request.", index)
        nodeLocator.findResponsibleActor(index) ! GetLocation(index, ctx.self)
        searchOnGraphForNSG(graph, data, nodeLocator, pathQueries, respondTo, responseLocations)

      case Location(index, location) =>
        timers.cancel(LocationTimerKey(index))
        var removedQueries: Set[Int] = Set.empty
        waitingOnLocation.received(index).foreach {queryId =>
          if (pathQueries.contains(queryId)){
            val queryInfo = pathQueries(queryId)
            queryInfo.waitingOn -= 1
            val oldNumberOfCandidates = queryInfo.candidates.length
            val queryFinished = addCandidate(queryInfo, queryId, index, location, nodeLocator)
            if (queryInfo.candidates.length > oldNumberOfCandidates) { // the candidate has been added
              responseLocations.addedToCandidateList(index, location)
            }
            if (queryFinished) {
              removedQueries = removedQueries + queryId
              sendResults(queryId, queryInfo, responseLocations, respondTo(queryId))
            }
          }
        }
        searchOnGraphForNSG(graph, data, nodeLocator, pathQueries -- removedQueries, respondTo -- removedQueries, responseLocations)

      case GetNSGFrom(nsgMerger) =>
        //ctx.log.info("Asking NSG Merger for my part of the NSG")
        nsgMerger ! GetPartialGraph(graph.keys.toSet, ctx.self)
        waitForNSG(nodeLocator, data)
    }

  def waitForNSG(nodeLocator: NodeLocator[ActorRef[SearchOnGraphEvent]], data: CacheData[Float]): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial{
      case PartialGraph(graph) =>
        ctx.log.info("Received nsg, ready for queries")
        clusterCoordinator ! NSGonSOG
        searchOnGraph(graph, data, nodeLocator, Map.empty, Map.empty, -1)
    }

}




