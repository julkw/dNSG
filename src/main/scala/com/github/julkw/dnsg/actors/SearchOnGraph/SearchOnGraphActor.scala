package com.github.julkw.dnsg.actors.SearchOnGraph

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.{CoordinationEvent, KNearestNeighbors, NSGonSOG, SearchOnGraphDistributionInfo}
import com.github.julkw.dnsg.actors.Coordinators.GraphConnectorCoordinator.{ConnectionCoordinationEvent, ReceivedNewEdge}
import com.github.julkw.dnsg.actors.Coordinators.GraphRedistributionCoordinator.{DoneWithRedistribution, RedistributionCoordinationEvent}
import com.github.julkw.dnsg.actors.{GraphConnector, GraphRedistributer}
import com.github.julkw.dnsg.actors.createNSG.NSGMerger.{GetPartialNSG, MergeNSGEvent}
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

  final case class GraphDistribution(nodeLocator: NodeLocator[SearchOnGraphEvent]) extends SearchOnGraphEvent

  // redistribution
  final case class RedistributeGraph(primaryAssignments: NodeLocator[SearchOnGraphEvent], secondaryAssignments: Map[Int, Set[ActorRef[SearchOnGraphEvent]]], redistributionCoordinator: ActorRef[RedistributionCoordinationEvent]) extends SearchOnGraphEvent

  final case class SendPartialGraph(size: Int, sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class PartialGraph(partialGraph: Seq[(Int, Seq[Int])], sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

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

  // get NSG from NSGMerger
  final case class GetNSGFrom(nsgMerger: ActorRef[MergeNSGEvent]) extends SearchOnGraphEvent

  final case class PartialNSG(partialGraph: Map[Int, Seq[Int]]) extends SearchOnGraphEvent

  // safe knng to file
  final case class GetGraph(sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  def apply(clusterCoordinator: ActorRef[CoordinationEvent]): Behavior[SearchOnGraphEvent] = Behaviors.setup { ctx =>
    Behaviors.withTimers { timers =>
      val settings = Settings(ctx.system.settings.config)
      new SearchOnGraphActor(clusterCoordinator, new WaitingOnLocation, settings, timers, ctx).waitForLocalGraph()
    }
  }
}

class SearchOnGraphActor(clusterCoordinator: ActorRef[CoordinationEvent],
                         waitingOnLocation: WaitingOnLocation,
                         settings: Settings,
                         timers: TimerScheduler[SearchOnGraphActor.SearchOnGraphEvent],
                         ctx: ActorContext[SearchOnGraphActor.SearchOnGraphEvent]) extends SearchOnGraph(clusterCoordinator, waitingOnLocation, timers, ctx) {
  import SearchOnGraphActor._

  def waitForLocalGraph(): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial{
      case GraphAndData(graph, localData, sender) =>
        // The data is send over with cache because there is a 1:1 mapping between knngWorkers and searchOnGraphActors and the first stops using the data after it has send it to the latter
        //sender ! GraphReceived(ctx.self)
        clusterCoordinator ! SearchOnGraphDistributionInfo(graph.keys.toSeq, ctx.self)
        waitForDistributionInfo(graph, localData)
    }

  def waitForDistributionInfo(graph: Map[Int, Seq[Int]], data: CacheData[Float]): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial{
      case GraphDistribution(nodeLocator) =>
        searchOnGraph(graph, data, nodeLocator, Map.empty, Map.empty, lastIdUsed = -1)

      case FindNearestNeighbors(value, k, asker) =>
        ctx.self ! FindNearestNeighbors(value, k, asker)
        waitForDistributionInfo(graph, data)
    }

  def searchOnGraph(graph: Map[Int, Seq[Int]],
                    data: CacheData[Float],
                    nodeLocator: NodeLocator[SearchOnGraphEvent],
                    neighborQueries: Map[Int, QueryInfo],
                    respondTo: Map[Int, ActorRef[CoordinationEvent]],
                    lastIdUsed: Int): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial {
      case FindNearestNeighbors(query, k, asker) =>
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
          getNeighbors(startingPoint, queryId, graph, nodeLocator)
          nodeLocator.findResponsibleActor(startingPoint) ! GetNeighbors(startingPoint, queryId, ctx.self)
          QueryInfo(query, k, Seq(QueryCandidate(startingPoint, euclideanDist(location, query), processed = false)), 0)
        } else {
          val qi = QueryInfo(query, k, Seq.empty, 0)
          askForLocation(startingPoint, queryId, qi, nodeLocator)
          qi
        }
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
              getNeighbors(nextCandidate.index, queryId, graph, nodeLocator)
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
            val queryFinished = addCandidate(queryInfo, queryId, index, location, graph, nodeLocator)
            if (queryFinished) {
              val finalNeighbors = queryInfo.candidates.map(_.index)
              respondTo(queryId) ! KNearestNeighbors(queryInfo.query, finalNeighbors)
              removedQueries = removedQueries + queryId
            }
          }
        }
        searchOnGraph(graph, data, nodeLocator, neighborQueries -- removedQueries, respondTo -- removedQueries, lastIdUsed)

      case AddToGraph(startNode, endNode, sender) =>
        sender ! ReceivedNewEdge
        val newNeighbors = graph(startNode) :+ endNode
        searchOnGraph(graph + (startNode -> newNeighbors), data, nodeLocator, neighborQueries, respondTo, lastIdUsed)

      case GetGraph(sender) =>
        sender ! Graph(graph, ctx.self)
        searchOnGraph(graph, data, nodeLocator, neighborQueries, respondTo, lastIdUsed)

      case CheckedNodesOnSearch(endPoint, startingPoint, neighborsWanted, asker) =>
        ctx.self ! CheckedNodesOnSearch(endPoint, startingPoint, neighborsWanted, asker)
        searchOnGraphForNSG(graph, data, nodeLocator, Map.empty, Map.empty, QueryResponseLocations(data))

      case ConnectGraph(graphConnectorSupervisor) =>
        val responsibility = graph.keys.filter(node => nodeLocator.findResponsibleActor(node) == ctx.self).toSeq
        ctx.spawn(GraphConnector(data.data, graph, responsibility, graphConnectorSupervisor), name="graphConnector")
        searchOnGraph(graph, data, nodeLocator, neighborQueries, respondTo, lastIdUsed)

      case RedistributeGraph(primaryAssignments, secondaryAssignments, redistributionCoordinator) =>
        // if a worker is the primary assignee for a graph_node it should not appear with the secondary assignees
        val nodesExpected = primaryAssignments.locationData.count(assignee => assignee == ctx.self) +
          secondaryAssignments.valuesIterator.count(assignees => assignees.contains(ctx.self))
        val toSend = graph.keys.groupBy(index => primaryAssignments.findResponsibleActor(index)).transform { (worker, nodes) =>
          val alsoSend = secondaryAssignments.keys.filter(node => graph.contains(node) && secondaryAssignments(node).contains(worker))
          (nodes ++ alsoSend).toSeq
        }
        val graphMessageSize = settings.maxMessageSize / (settings.k + 1)
        toSend.keys.foreach(graphHolder => graphHolder ! SendPartialGraph(graphMessageSize, ctx.self))
        redistributeGraph(toSend, graph, primaryAssignments, Map.empty, nodesExpected, graphMessageSize, data, false, redistributionCoordinator)

      case SendPartialGraph(size, sender) =>
        ctx.self ! SendPartialGraph(size, sender)
        searchOnGraph(graph, data, nodeLocator, neighborQueries, respondTo, lastIdUsed)
    }

  def redistributeGraph(toSend: Map[ActorRef[SearchOnGraphEvent], Seq[Int]],
                        oldGraph: Map[Int, Seq[Int]],
                        nodeLocator: NodeLocator[SearchOnGraphEvent],
                        newGraph: Map[Int, Seq[Int]],
                        nodesExpected: Int,
                        graphMessageSize: Int,
                        data: CacheData[Float],
                        dataUpdated: Boolean,
                        redistributionCoordinator: ActorRef[RedistributionCoordinationEvent]): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial {
      case UpdatedLocalData(newData) =>
        val updatedData = CacheData(settings.cacheSize, newData)
        checkIfRedistributionDone(toSend, oldGraph, nodeLocator, newGraph, nodesExpected, graphMessageSize, updatedData, dataUpdated = true, redistributionCoordinator)

      case PartialGraph(partialGraph, sender) =>
        val updatedGraph = newGraph ++ partialGraph
        if(partialGraph.size == graphMessageSize) {
          // else this was the last piece of graph from this actor
          sender ! SendPartialGraph(graphMessageSize, ctx.self)
        }
        checkIfRedistributionDone(toSend, oldGraph, nodeLocator, updatedGraph, nodesExpected, graphMessageSize, data, dataUpdated, redistributionCoordinator)

      case SendPartialGraph(size, sender) =>
        if (toSend.contains(sender)) {
          val stillToSend = toSend(sender)
          val partialGraph = stillToSend.slice(0, size).map(node => (node, oldGraph(node)))
          sender ! PartialGraph(partialGraph, ctx.self)
          val updatedToSend = toSend + (sender -> stillToSend.slice(size, stillToSend.size))
          checkIfRedistributionDone(updatedToSend, oldGraph, nodeLocator, newGraph, nodesExpected, graphMessageSize, data, dataUpdated, redistributionCoordinator)
        } else {
          // none of my nodes have been assigned to this specific actor
          sender ! PartialGraph(Seq.empty, ctx.self)
          checkIfRedistributionDone(toSend, oldGraph, nodeLocator, newGraph, nodesExpected, graphMessageSize, data, dataUpdated, redistributionCoordinator)
        }
  }

  def checkIfRedistributionDone(toSend: Map[ActorRef[SearchOnGraphEvent], Seq[Int]],
                                oldGraph: Map[Int, Seq[Int]],
                                nodeLocator: NodeLocator[SearchOnGraphEvent],
                                newGraph: Map[Int, Seq[Int]],
                                nodesExpected: Int,
                                graphMessageSize: Int,
                                data: CacheData[Float],
                                dataUpdated: Boolean,
                                redistributionCoordinator: ActorRef[RedistributionCoordinationEvent]): Behavior[SearchOnGraphEvent] = {
    val everythingSent = !toSend.valuesIterator.exists(_.nonEmpty)
    if (newGraph.size == nodesExpected && dataUpdated && everythingSent) {
      redistributionCoordinator ! DoneWithRedistribution
      searchOnGraph(newGraph, data, nodeLocator, Map.empty, Map.empty, -1)
    } else {
      redistributeGraph(toSend, oldGraph, nodeLocator, newGraph, nodesExpected, graphMessageSize, data, dataUpdated, redistributionCoordinator)
    }
  }

  def searchOnGraphForNSG(graph: Map[Int, Seq[Int]],
                          data: CacheData[Float],
                          nodeLocator: NodeLocator[SearchOnGraphEvent],
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
          getNeighbors(startingPoint, queryId, graph, nodeLocator)
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
            getNeighbors(nextCandidate.index, queryId, graph, nodeLocator)
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
            val queryFinished = addCandidate(queryInfo, queryId, index, location, graph, nodeLocator)
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
        nsgMerger ! GetPartialNSG(graph.keys.toSet, ctx.self)
        waitForNSG(nodeLocator, data)
    }

  def waitForNSG(nodeLocator: NodeLocator[SearchOnGraphEvent], data: CacheData[Float]): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial{
      case PartialNSG(graph) =>
        val myResponsibility = nodeLocator.locationData.zipWithIndex.filter(locData => locData._1 == ctx.self).map(_._2)
        val responsibilityMidPoint = (0 until data.data.dimension).map(dim => myResponsibility.map(index => data.get(index)).map(_(dim)).sum / myResponsibility.length)
        clusterCoordinator ! NSGonSOG(responsibilityMidPoint, ctx.self)
        searchOnGraph(graph, data, nodeLocator, Map.empty, Map.empty, lastIdUsed = -1)
    }
}




