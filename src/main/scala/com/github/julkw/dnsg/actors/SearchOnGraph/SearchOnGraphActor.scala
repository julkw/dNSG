package com.github.julkw.dnsg.actors.SearchOnGraph

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.ClusterCoordinator._
import com.github.julkw.dnsg.actors.SearchOnGraph
import com.github.julkw.dnsg.actors.createNSG.NSGMerger.{GetPartialGraph, MergeNSGEvent}
import com.github.julkw.dnsg.actors.createNSG.NSGWorker.{BuildNSGEvent, Responsibility}
import com.github.julkw.dnsg.util._

import scala.collection.mutable
import scala.language.postfixOps


object SearchOnGraphActor {

  sealed trait SearchOnGraphEvent extends dNSGSerializable

  // setup
  final case class Graph(graph: Map[Int, Seq[Int]], sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class GraphReceived(graphHolder: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class GraphDistribution(nodeLocator: NodeLocator[SearchOnGraphEvent]) extends SearchOnGraphEvent

  // queries
  final case class FindNearestNeighbors(query: Seq[Float], k: Int, asker: ActorRef[CoordinationEvent]) extends SearchOnGraphEvent

  final case class FindNearestNeighborsStartingFrom(query: Seq[Float], startingPoint: Int, k: Int, asker: ActorRef[CoordinationEvent]) extends SearchOnGraphEvent

  final case class CheckedNodesOnSearch(endPoint: Int, startingPoint: Int, neighborsWanted: Int, asker: ActorRef[BuildNSGEvent]) extends SearchOnGraphEvent

  // search
  final case class GetNeighbors(index: Int, queryId: Int, sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class Neighbors(queryId: Int, index: Int, neighbors: Seq[Int]) extends SearchOnGraphEvent

  final case class GetLocation(index: Int, sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class Location(index: Int, location: Seq[Float]) extends SearchOnGraphEvent

  // send responsiblities to NSG workers
  final case class SendResponsibleIndicesTo(nsgWorker: ActorRef[BuildNSGEvent]) extends SearchOnGraphEvent
  // get NSG from NSGMerger
  final case class GetNSGFrom(nsgMerger: ActorRef[MergeNSGEvent]) extends SearchOnGraphEvent

  final case class PartialNSG(graph: Map[Int, Seq[Int]]) extends SearchOnGraphEvent

  // TODO move to other actor?
  // check for Connectivity
  final case class UpdateConnectivity(root: Int) extends SearchOnGraphEvent

  final case class IsConnected(connectedNode: Int, parent: Int) extends SearchOnGraphEvent

  final case class DoneConnectingChildren(nodeAwaitingAnswer: Int) extends SearchOnGraphEvent

  final case class FindUnconnectedNode(sendTo: ActorRef[CoordinationEvent], notAskedYet: Set[ActorRef[SearchOnGraphEvent]]) extends SearchOnGraphEvent

  final case class AddToGraph(startNode: Int, endNode: Int) extends SearchOnGraphEvent

  final case class GraphConnected(sender: ActorRef[CoordinationEvent]) extends SearchOnGraphEvent

  // safe knng to file
  final case class GetGraph(sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent


  def apply(supervisor: ActorRef[CoordinationEvent],
            data: LocalData[Float]): Behavior[SearchOnGraphEvent] = Behaviors.setup { ctx =>
    //ctx.log.info("Started SearchOnGraph")
     new SearchOnGraphActor(supervisor, data, ctx).waitForLocalGraph()
  }
}

class SearchOnGraphActor(supervisor: ActorRef[CoordinationEvent],
                         data: LocalData[Float],
                         ctx: ActorContext[SearchOnGraphActor.SearchOnGraphEvent]) extends SearchOnGraph(supervisor, ctx) {
  import SearchOnGraphActor._

  def waitForLocalGraph(): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial{
      case Graph(graph, sender) =>
        //sender ! GraphReceived(ctx.self)
        supervisor ! SearchOnGraphDistributionInfo(graph.keys.toSeq, ctx.self)
        waitForDistributionInfo(graph)
    }

  def waitForDistributionInfo(graph: Map[Int, Seq[Int]]): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial{
      case GraphDistribution(nodeLocator) =>
        searchOnGraph(graph, nodeLocator, Map.empty, Map.empty, -1, new WaitingOnLocation)
    }

  def searchOnGraph(graph: Map[Int, Seq[Int]],
                    nodeLocator: NodeLocator[SearchOnGraphEvent],
                    neighborQueries: Map[Int, QueryInfo],
                    respondTo: Map[Int, ActorRef[CoordinationEvent]],
                    lastIdUsed: Int,
                    waitingOnLocation: WaitingOnLocation): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial {
      case FindNearestNeighbors(query, k, asker) =>
        ctx.log.info("Asked to find navigating node")
        // choose node to start search from local nodes
        val startingNodeIndex: Int = graph.keys.head
        val queryInfo = QueryInfo(query, k, Seq(QueryCandidate(startingNodeIndex, euclideanDist(data.get(startingNodeIndex), query), processed = false)), 0)
        val queryId = lastIdUsed + 1
        // since this node is located locally, just ask self
        ctx.self ! GetNeighbors(startingNodeIndex, queryId, ctx.self)
        searchOnGraph(graph, nodeLocator, neighborQueries + (queryId -> queryInfo), respondTo + (queryId -> asker), queryId, waitingOnLocation)

      case FindNearestNeighborsStartingFrom(query, startingPoint, k, asker) =>
        val queryId = lastIdUsed + 1
        val queryInfo = if (data.isLocal(startingPoint)) {
          val location = data.get(startingPoint)
          nodeLocator.findResponsibleActor(startingPoint) ! GetNeighbors(startingPoint, queryId, ctx.self)
          QueryInfo(query, k, Seq(QueryCandidate(startingPoint, euclideanDist(location, query), processed = false)), 0)
        } else {
          val qi = QueryInfo(query, k, Seq.empty, 0)
          askForLocation(startingPoint, queryId, qi, nodeLocator, waitingOnLocation)
          qi
        }
        nodeLocator.findResponsibleActor(startingPoint) ! GetNeighbors(startingPoint, queryId, ctx.self)
        searchOnGraph(graph, nodeLocator, neighborQueries + (queryId -> queryInfo), respondTo + (queryId -> asker), queryId, waitingOnLocation)

      case GetNeighbors(index, queryId, sender) =>
        sender ! Neighbors(queryId, index, graph(index))
        searchOnGraph(graph, nodeLocator, neighborQueries, respondTo, lastIdUsed, waitingOnLocation)

      case Neighbors(queryId, processedIndex, neighbors) =>
        if (neighborQueries.contains(queryId)) {
          val queryInfo = neighborQueries(queryId)
          updateCandidates(queryInfo, queryId, processedIndex, neighbors.diff(queryInfo.candidates), nodeLocator, waitingOnLocation, data)
          // check if all candidates have been processed
          val nextCandidateToProcess = queryInfo.candidates.find(query => !query.processed)
          nextCandidateToProcess match {
            case Some(nextCandidate) =>
              // find the neighbors of the next candidate to be processed and update queries
              nodeLocator.findResponsibleActor(nextCandidate.index) ! GetNeighbors(nextCandidate.index, queryId, ctx.self)
              searchOnGraph(graph, nodeLocator, neighborQueries, respondTo, lastIdUsed, waitingOnLocation)
            case None =>
              if (neighborQueries(queryId).waitingOn > 0) {
                // do nothing for now
                searchOnGraph(graph, nodeLocator, neighborQueries, respondTo, lastIdUsed, waitingOnLocation)
              } else {
                val finalNeighbors = queryInfo.candidates.map(_.index)
                respondTo(queryId) ! KNearestNeighbors(queryInfo.query, finalNeighbors)
                searchOnGraph(graph, nodeLocator, neighborQueries - queryId, respondTo - queryId, lastIdUsed, waitingOnLocation)
              }
          }
        } else {
          // already done with this query, can ignore this message
          searchOnGraph(graph, nodeLocator, neighborQueries, respondTo, lastIdUsed, waitingOnLocation)
        }

      case GetLocation(index, sender) =>
        sender ! Location(index, data.get(index))
        searchOnGraph(graph, nodeLocator, neighborQueries, respondTo, lastIdUsed, waitingOnLocation)

      case Location(index, location) =>
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
        searchOnGraph(graph, nodeLocator, neighborQueries -- removedQueries, respondTo -- removedQueries, lastIdUsed, waitingOnLocation)

      case GetGraph(sender) =>
        //ctx.log.info("Asked for graph info")
        sender ! Graph(graph, ctx.self)
        searchOnGraph(graph, nodeLocator, neighborQueries, respondTo, lastIdUsed, waitingOnLocation)

      case SendResponsibleIndicesTo(nsgWorker) =>
        nsgWorker ! Responsibility(graph.keys.toSeq)
        searchOnGraphForNSG(graph, nodeLocator, Map.empty, Map.empty, SearchOnGraph.QueryResponseLocations(data), new WaitingOnLocation)
    }

  def searchOnGraphForNSG(graph: Map[Int, Seq[Int]],
                          nodeLocator: NodeLocator[SearchOnGraphEvent],
                          pathQueries: Map[Int, QueryInfo],
                          respondTo: Map[Int, ActorRef[BuildNSGEvent]],
                          responseLocations: QueryResponseLocations[Float],
                          waitingOnLocation: WaitingOnLocation): Behavior[SearchOnGraphEvent] =
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
          askForLocation(startingPoint, queryId, queryInfo, nodeLocator, waitingOnLocation)
          queryInfo
        }
        searchOnGraphForNSG(graph, nodeLocator,
          pathQueries + (queryId -> pathQueryInfo),
          respondTo  + (queryId -> asker),
          responseLocations,
          waitingOnLocation)

      case GetNeighbors(index, query, sender) =>
        sender ! Neighbors(query, index, graph(index))
        searchOnGraphForNSG(graph, nodeLocator, pathQueries, respondTo, responseLocations, waitingOnLocation)

      case Neighbors(queryId, processedIndex, neighbors) =>
        val queryInfo = pathQueries(queryId)
        updatePathCandidates(queryInfo,  queryId, processedIndex, neighbors, responseLocations, nodeLocator, waitingOnLocation)
        // check if all candidates have been processed
        val nextCandidateToProcess = queryInfo.candidates.find(query => !query.processed)
        nextCandidateToProcess match {
          case Some(nextCandidate) =>
            // find the neighbors of the next candidate to be processed and update queries
            nodeLocator.findResponsibleActor(nextCandidate.index) ! GetNeighbors(nextCandidate.index, queryId, ctx.self)
            searchOnGraphForNSG(graph, nodeLocator, pathQueries, respondTo, responseLocations, waitingOnLocation)
          case None =>
            if (pathQueries(queryId).waitingOn > 0) {
              // do nothing for now
              searchOnGraphForNSG(graph, nodeLocator, pathQueries, respondTo, responseLocations, waitingOnLocation)
            } else {
              sendResults(queryId, queryInfo, responseLocations, respondTo(queryId))
              searchOnGraphForNSG(graph, nodeLocator, pathQueries - queryId, respondTo - queryId, responseLocations, waitingOnLocation)
            }
        }

      case GetLocation(index, sender) =>
        sender ! Location(index, data.get(index))
        searchOnGraphForNSG(graph, nodeLocator, pathQueries, respondTo, responseLocations, waitingOnLocation)

      case Location(index, location) =>
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
        searchOnGraphForNSG(graph, nodeLocator, pathQueries -- removedQueries, respondTo -- removedQueries, responseLocations, waitingOnLocation)

      case GetNSGFrom(nsgMerger) =>
        //ctx.log.info("Asking NSG Merger for my part of the NSG")
        nsgMerger ! GetPartialGraph(graph.keys.toSet, ctx.self)
        waitForNSG(nodeLocator)
    }

  def waitForNSG(nodeLocator: NodeLocator[SearchOnGraphEvent]): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial{
      case PartialNSG(graph) =>
        ctx.log.info("Received nsg, ready for queries/establishing connectivity")
        supervisor ! UpdatedToNSG
        establishConnectivity(graph, nodeLocator, ConnectivityInfo(mutable.Set.empty, mutable.Map.empty))
    }

  def establishConnectivity(graph: Map[Int, Seq[Int]],
                            nodeLocator: NodeLocator[SearchOnGraphEvent],
                            connectivityInfo: ConnectivityInfo): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial {
      case UpdateConnectivity(root) =>
        ctx.log.info("Updating connectivity")
        connectivityInfo.connectedNodes.add(root)
        updateNeighborConnectedness(root, root, connectivityInfo, graph, nodeLocator)
        establishConnectivity(graph, nodeLocator, connectivityInfo)

      case IsConnected(connectedNode, parent) =>
        // in case the parent is placed on another node this might not be known here
        connectivityInfo.connectedNodes.add(parent)
        if (connectivityInfo.connectedNodes.contains(connectedNode)) {
          nodeLocator.findResponsibleActor(parent) ! DoneConnectingChildren(parent)
        } else {
          connectivityInfo.connectedNodes.add(connectedNode)
          updateNeighborConnectedness(connectedNode, parent, connectivityInfo, graph, nodeLocator)
        }
        establishConnectivity(graph, nodeLocator, connectivityInfo)

      case DoneConnectingChildren(nodeAwaitingAnswer) =>
        val messageCounter = connectivityInfo.messageTracker(nodeAwaitingAnswer)
        messageCounter.waitingForMessages -= 1
        if (messageCounter.waitingForMessages == 0) {
          if (messageCounter.parentNode == nodeAwaitingAnswer) {
            supervisor ! FinishedUpdatingConnectivity
            ctx.log.info("Done with updating connectivity")
          } else {
            nodeLocator.findResponsibleActor(messageCounter.parentNode) !
              DoneConnectingChildren(messageCounter.parentNode)
            connectivityInfo.messageTracker -= nodeAwaitingAnswer
          }
        }
        establishConnectivity(graph, nodeLocator, connectivityInfo)

      case FindUnconnectedNode(sendTo, notAskedYet) =>
        val unconnectedNodes = graph.keys.toSet -- connectivityInfo.connectedNodes
        if (unconnectedNodes.isEmpty) {
          // no unconnected nodes in this actor, ask others
          if (notAskedYet.nonEmpty) {
            notAskedYet.head ! FindUnconnectedNode(sendTo, notAskedYet - ctx.self)
          } else {
            // there are no unconnected nodes
            supervisor ! AllConnected
          }
        } else {
          // send one of the unconnected nodes
          sendTo ! UnconnectedNode(unconnectedNodes.head, data.get(unconnectedNodes.head))
        }
        establishConnectivity(graph, nodeLocator, connectivityInfo)

      case AddToGraph(startNode, endNode) =>
        ctx.log.info("Add edge to graph to ensure connectivity")
        val updatedNeighbors = graph(startNode) :+ endNode
        establishConnectivity(graph + (startNode -> updatedNeighbors), nodeLocator, connectivityInfo)

      case GraphConnected(sender) =>
        sender ! BackToSearch
        searchOnGraph(graph, nodeLocator, Map.empty, Map.empty, -1, new WaitingOnLocation)
    }
}




