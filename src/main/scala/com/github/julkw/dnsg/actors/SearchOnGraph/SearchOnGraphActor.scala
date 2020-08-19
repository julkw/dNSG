package com.github.julkw.dnsg.actors.SearchOnGraph

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.{CoordinationEvent, GetMoreQueries, NSGonSOG, UpdatedGraph}
import com.github.julkw.dnsg.actors.Coordinators.GraphConnectorCoordinator.{ConnectionCoordinationEvent, ReceivedNewEdge}
import com.github.julkw.dnsg.actors.Coordinators.GraphRedistributionCoordinator.{DoneWithRedistribution, RedistributionCoordinationEvent}
import com.github.julkw.dnsg.actors.DataHolder.{GraphForFile, LoadDataEvent}
import com.github.julkw.dnsg.actors.GraphConnector
import com.github.julkw.dnsg.actors.NodeLocatorHolder.{LocalSOGDistributionInfo, NodeLocationEvent}
import com.github.julkw.dnsg.actors.SearchOnGraph.SOGInfo.{GetLocation, GetNeighbors, Location, Neighbors, SOGEvent}
import com.github.julkw.dnsg.actors.createNSG.NSGMerger.{GetPartialNSG, MergeNSGEvent}
import com.github.julkw.dnsg.actors.createNSG.NSGWorker.{BuildNSGEvent, GetMorePathQueries}
import com.github.julkw.dnsg.actors.nndescent.KnngWorker
import com.github.julkw.dnsg.actors.nndescent.KnngWorker.BuildKNNGEvent
import com.github.julkw.dnsg.util.Data.LocalData
import com.github.julkw.dnsg.util._

import scala.language.postfixOps

object SearchOnGraphActor {

  sealed trait SearchOnGraphEvent extends dNSGSerializable

  // setup
  final case class InitializeGraph(responsibility: Seq[Int], graphSize: Int, localData: LocalData[Float]) extends SearchOnGraphEvent

  final case class GraphAndData(graph: Map[Int, Seq[Int]], cacheData: LocalData[Float], sender: ActorRef[BuildKNNGEvent]) extends SearchOnGraphEvent

  final case class GraphReceived(graphHolder: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class GraphDistribution(nodeLocator: NodeLocator[SearchOnGraphEvent]) extends SearchOnGraphEvent

  // redistribution
  final case class RedistributeGraph(primaryAssignments: NodeLocator[SearchOnGraphEvent], secondaryAssignments: Map[Int, Set[ActorRef[SearchOnGraphEvent]]], redistributionCoordinator: ActorRef[RedistributionCoordinationEvent]) extends SearchOnGraphEvent

  final case class SendPartialGraph(sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class PartialGraph(partialGraph: Seq[(Int, Seq[Int])], sender: ActorRef[SearchOnGraphEvent], moreToSend: Boolean) extends SearchOnGraphEvent

  final case class UpdatedLocalData(data: LocalData[Float]) extends  SearchOnGraphEvent

  // queries
  final case class FindNearestNeighbors(queries: Seq[(Array[Float], Int)], k: Int, asker: ActorRef[CoordinationEvent], sendWithDist: Boolean, moreQueries: Boolean) extends SearchOnGraphEvent

  final case class FindNearestNeighborsStartingFrom(queries: Seq[(Array[Float], Int)], startingPoint: Int,  k: Int, asker: ActorRef[CoordinationEvent], moreQueries: Boolean) extends SearchOnGraphEvent

  final case class CheckedNodesOnSearch(queries: Seq[Int], startingPoint: Int, k: Int, asker: ActorRef[BuildNSGEvent], moreQueries: Boolean) extends SearchOnGraphEvent

  // search
  final case class ProcessNextCandidate(queryId: Int) extends SearchOnGraphEvent

  final case class GetSearchOnGraphInfo(sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class SearchOnGraphInfo(info: collection.Seq[SOGEvent], sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  // connectivity
  final case class ConnectGraph(graphConnectorSupervisor: ActorRef[ConnectionCoordinationEvent]) extends SearchOnGraphEvent

  final case class AddToGraph(startNode: Int, endNode: Int, sender: ActorRef[ConnectionCoordinationEvent]) extends SearchOnGraphEvent

  // get NSG from NSGMerger
  final case class GetNSGFrom(nsgMerger: ActorRef[MergeNSGEvent]) extends SearchOnGraphEvent

  final case class PartialNSG(partialGraph: Map[Int, Seq[Int]]) extends SearchOnGraphEvent

  // safe knng to file
  final case class SendGraphForFile(sender: ActorRef[LoadDataEvent]) extends SearchOnGraphEvent

  def apply(clusterCoordinator: ActorRef[CoordinationEvent],
            nodeLocatorHolder: ActorRef[NodeLocationEvent]): Behavior[SearchOnGraphEvent] = Behaviors.setup { ctx =>
    val settings = Settings(ctx.system.settings.config)
    new SearchOnGraphActor(clusterCoordinator, nodeLocatorHolder, new WaitingOnLocation, new WaitingOnLocation, settings, ctx).setup()
  }
}

class SearchOnGraphActor(clusterCoordinator: ActorRef[CoordinationEvent],
                         nodeLocatorHolder: ActorRef[NodeLocationEvent],
                         waitingOnLocation: WaitingOnLocation[Int],
                         waitingOnNeighbors: WaitingOnLocation[Int],
                         settings: Settings,
                         ctx: ActorContext[SearchOnGraphActor.SearchOnGraphEvent])
  extends SearchOnGraph(waitingOnLocation, waitingOnNeighbors, settings.maxMessageSize, settings.maxNeighborCandidates, ctx) {
  import SearchOnGraphActor._

  def setup(): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial{
      case InitializeGraph(responsibility, graphSize, localData) =>
        val graph = responsibility.map { index =>
          index -> randomNodes(settings.preNNDescentK, graphSize).toSeq
        }.toMap
        nodeLocatorHolder ! LocalSOGDistributionInfo(responsibility, ctx.self)
        waitForDistributionInfo(graph, localData)
    }

  def waitForDistributionInfo(graph: Map[Int, Seq[Int]], data: LocalData[Float]): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial{
      case GraphDistribution(nodeLocator) =>
        ctx.spawn(KnngWorker(data, nodeLocator, ctx.self, clusterCoordinator, nodeLocatorHolder), name = "KnngWorker")
        val toSend = nodeLocator.allActors.map(worker => worker -> new SOGInfo).toMap
        searchOnGraph(graph, data, nodeLocator, Map.empty, Map.empty, lastIdUsed = -1, toSend)

      case FindNearestNeighbors(queries, k, asker, sendWithDist, moreQueries) =>
        ctx.self ! FindNearestNeighbors(queries, k, asker, sendWithDist, moreQueries)
        waitForDistributionInfo(graph, data)

      case SearchOnGraphInfo(info, sender) =>
        ctx.self ! SearchOnGraphInfo(info, sender)
        waitForDistributionInfo(graph, data)

      case SendGraphForFile(sender) =>
        ctx.self ! SendGraphForFile(sender)
        waitForDistributionInfo(graph, data)
    }

  def searchOnGraph(graph: Map[Int, Seq[Int]],
                    data: LocalData[Float],
                    nodeLocator: NodeLocator[SearchOnGraphEvent],
                    neighborQueries: Map[Int, QueryInfo],
                    respondTo: Map[Int, (ActorRef[CoordinationEvent], Int)],
                    lastIdUsed: Int,
                    toSend: Map[ActorRef[SearchOnGraphEvent], SOGInfo]): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial {
      case FindNearestNeighbors(queries, k, asker, sendWithDist, moreQueries) =>
        if (moreQueries) {
          asker ! GetMoreQueries(ctx.self)
        }
        var lastQueryId = lastIdUsed
        // choose node to start search from local nodes
        val newQueries = queries.map { case (query, _) =>
          val queryId = lastQueryId + 1
          // initialize candidate pool with random nodes
          // ensure at least one of the initial candidates is local & since the node itself will probably end up in the solution return k + 1
          val initialCandidates = (randomNodes(k, nodeLocator.graphSize) + graph.head._1)
          val (localCandidates, remoteCandidates) = initialCandidates.partition(potentialCandidate => data.isLocal(potentialCandidate))
          val newCandidates = localCandidates.map { candidateIndex =>
            val location = data.get(candidateIndex)
            QueryCandidate(candidateIndex, euclideanDist(query, location), processed = false, currentlyProcessing = false)
          }.toSeq.sortBy(_.distance)
          // candidates for which we don't have the location have to ask for it first
          var waitingOn = 0
          remoteCandidates.foreach(remoteNeighbor => waitingOn += askForLocation(remoteNeighbor, queryId, nodeLocator, toSend))
          val queryInfo = QueryInfo(query, k, newCandidates, waitingOn, sendWithDist)
          ctx.self ! ProcessNextCandidate(queryId)
          lastQueryId = queryId
          (queryId, queryInfo)
        }
        sendMessagesImmediately(toSend)
        val newRespondTo = newQueries.map(_._1).zip(queries.map(_._2)).map { case (localQueryId, senderQueryId) =>
          localQueryId -> (asker, senderQueryId)
        }
        searchOnGraph(graph, data, nodeLocator, neighborQueries ++ newQueries, respondTo ++ newRespondTo, lastQueryId, toSend)

      case FindNearestNeighborsStartingFrom(queries, startingPoint, k, asker, moreQueries) =>
        if (moreQueries) {
          asker ! GetMoreQueries(ctx.self)
        }
        var lastQueryId = lastIdUsed
        val newQueries = queries.map { case (query, _) =>
          val queryId = lastQueryId + 1
          lastQueryId = queryId
          ctx.self ! ProcessNextCandidate(queryId)
          if (data.isLocal(startingPoint)) {
            val location = data.get(startingPoint)
            (queryId, QueryInfo(query, k, Seq(QueryCandidate(startingPoint, euclideanDist(location, query), processed = false, currentlyProcessing = false)), 0))
          } else {
            (queryId, QueryInfo(query, k, Seq.empty, askForLocation(startingPoint, queryId, nodeLocator, toSend)))
          }
        }
        sendMessagesImmediately(toSend)
        val newRespondTo = newQueries.map(_._1).zip(queries.map(_._2)).map { case (localQueryId, senderQueryId) =>
          localQueryId -> (asker, senderQueryId)
        }
        searchOnGraph(graph, data, nodeLocator, neighborQueries ++ newQueries, respondTo ++ newRespondTo, lastQueryId, toSend)

      case GetSearchOnGraphInfo(sender) =>
        if (toSend(sender).nonEmpty) {
          val messagesToSend = toSend(sender).sendMessage(settings.maxMessageSize)
          sender ! SearchOnGraphInfo(messagesToSend, ctx.self)
          toSend(sender).sendImmediately = false
        } else {
          toSend(sender).sendImmediately = true
        }
        searchOnGraph(graph, data, nodeLocator, neighborQueries, respondTo, lastIdUsed, toSend)

      case ProcessNextCandidate(queryId) =>
        if (neighborQueries.contains(queryId)) {
          val nextToProcess = neighborQueries(queryId).candidates.find(query => !query.processed && !query.currentlyProcessing)
          if (nextToProcess.isDefined) {
            nextToProcess.get.currentlyProcessing = true
            askForNeighbors(nextToProcess.get.index, queryId, graph, nodeLocator, toSend)
            sendMessagesImmediately(toSend)
          }
          ctx.self ! ProcessNextCandidate(queryId)
        }
        searchOnGraph(graph, data, nodeLocator, neighborQueries, respondTo, lastIdUsed, toSend)

      case SearchOnGraphInfo(info, sender) =>
        sender ! GetSearchOnGraphInfo(ctx.self)
        var updatedNeighborQueries = neighborQueries
        info.foreach {
          case GetNeighbors(index) =>
            toSend(sender).addMessage(Neighbors(index, graph(index)))

          case Neighbors(processedIndex, neighbors) =>
            waitingOnNeighbors.received(processedIndex).foreach { queryId =>
              if (updatedNeighborQueries.contains(queryId)) {
                val queryInfo = updatedNeighborQueries(queryId)
                updateCandidates(queryInfo, queryId, processedIndex, neighbors, nodeLocator, data, toSend)
                // check if all candidates have been processed
                val allProcessed = queryInfo.candidates.forall(query => query.processed)
                if (allProcessed && updatedNeighborQueries(queryId).waitingOn == 0) {
                  sendKNNResults(queryInfo, respondTo(queryId))
                  updatedNeighborQueries -= queryId
                }
              }
            }

          case GetLocation(index) =>
            toSend(sender).addMessage(Location(index, data.get(index)))

          case Location(index, location) =>
            waitingOnLocation.received(index).foreach {queryId =>
              if (updatedNeighborQueries.contains(queryId)) {
                val queryInfo = updatedNeighborQueries(queryId)
                queryInfo.waitingOn -= 1
                val queryFinished = addCandidate(queryInfo, index, location)
                if (queryFinished) {
                  sendKNNResults(queryInfo, respondTo(queryId))
                  updatedNeighborQueries -= queryId
                }
              }
            }
        }
        sendMessagesImmediately(toSend)
        val queriesToRemove = neighborQueries.keys.toSet.diff(updatedNeighborQueries.keys.toSet)
        searchOnGraph(graph, data, nodeLocator, updatedNeighborQueries, respondTo -- queriesToRemove, lastIdUsed, toSend)

      case AddToGraph(startNode, endNode, sender) =>
        sender ! ReceivedNewEdge
        if (graph.contains(startNode)) {
          val newNeighbors = graph(startNode) :+ endNode
          searchOnGraph(graph + (startNode -> newNeighbors), data, nodeLocator, neighborQueries, respondTo, lastIdUsed, toSend)
        } else {
          searchOnGraph(graph, data, nodeLocator, neighborQueries, respondTo, lastIdUsed, toSend)
        }


      case CheckedNodesOnSearch(endPoints, startingPoint, neighborsWanted, asker, moreQueries) =>
        ctx.self ! CheckedNodesOnSearch(endPoints, startingPoint, neighborsWanted, asker, moreQueries)
        toSend.foreach { case (_, sendInfo) => sendInfo.sendImmediately = true }
        sendMessagesImmediately(toSend)
        searchOnGraphForNSG(graph, data, nodeLocator, Map.empty, Map.empty, QueryResponseLocations(data), toSend)

      case ConnectGraph(graphConnectorSupervisor) =>
        val responsibility = graph.keys.filter(node => nodeLocator.findResponsibleActor(node) == ctx.self).toSeq
        ctx.spawn(GraphConnector(data, graph, responsibility, graphConnectorSupervisor, nodeLocatorHolder, ctx.self), name="graphConnector")
        searchOnGraph(graph, data, nodeLocator, neighborQueries, respondTo, lastIdUsed, toSend)

      case RedistributeGraph(primaryAssignments, secondaryAssignments, redistributionCoordinator) =>
        // if a worker is the primary assignee for a graph_node it should not appear with the secondary assignees
        val nodesExpected = primaryAssignments.numberOfNodes(ctx.self) +
          secondaryAssignments.valuesIterator.count(assignees => assignees.contains(ctx.self))
        val localSecondaryAssignments = secondaryAssignments.keys.toSet.intersect(graph.keys.toSet)
        val toSend = graph.keys.groupBy(index => primaryAssignments.findResponsibleActor(index))
        val toSendWithSecondary = nodeLocator.allActors.map { actor =>
          val toSendPrimary = toSend.getOrElse(actor, Seq.empty)
          val toSendSecondary = localSecondaryAssignments.filter(node => secondaryAssignments(node).contains(actor))
          actor -> (toSendPrimary.toSeq ++ toSendSecondary.toSeq)
        }.toMap
        val graphMessageSize = settings.maxMessageSize / (settings.k + 1)
        nodeLocator.allActors.foreach(graphHolder => graphHolder ! SendPartialGraph(ctx.self))
        redistributeGraph(toSendWithSecondary, graph, primaryAssignments, Map.empty, nodesExpected, graphMessageSize, data, dataUpdated = false, redistributionCoordinator)

      case SendPartialGraph(sender) =>
        ctx.self ! SendPartialGraph(sender)
        searchOnGraph(graph, data, nodeLocator, neighborQueries, respondTo, lastIdUsed, toSend)

      case UpdatedLocalData(newData) =>
        ctx.self ! UpdatedLocalData(newData)
        searchOnGraph(graph, data, nodeLocator, neighborQueries, respondTo, lastIdUsed, toSend)

      case SendGraphForFile(sender) =>
        val graphMessageSize = settings.maxMessageSize / (settings.k + 1)
        ctx.self ! SendGraphForFile(sender)
        // only send the graph information for the nodes for which I am the primary assignee
        val nodesToSend = nodeLocator.nodesOf(ctx.self)
        sendGraphToDataHolder(nodesToSend, graphMessageSize, graph, data, nodeLocator, neighborQueries, respondTo, lastIdUsed, toSend)

      case GraphAndData(newGraph, newData, _) =>
        // use the knngWorker's cache
        assert(neighborQueries.isEmpty)
        clusterCoordinator ! UpdatedGraph
        searchOnGraph(newGraph, newData, nodeLocator, neighborQueries, respondTo, lastIdUsed, toSend)
    }

  def sendGraphToDataHolder(toSend: Seq[Int],
                            graphMessageSize: Int,
                            graph: Map[Int, Seq[Int]],
                            data: LocalData[Float],
                            nodeLocator: NodeLocator[SearchOnGraphEvent],
                            neighborQueries: Map[Int, QueryInfo],
                            respondTo: Map[Int, (ActorRef[CoordinationEvent], Int)],
                            lastIdUsed: Int,
                            sogToSend: Map[ActorRef[SearchOnGraphEvent], SOGInfo]): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial {
      case SendGraphForFile(sender) =>
        val sendNow = toSend.slice(0, graphMessageSize).map(index => (index, graph(index)))
        val sendLater = toSend.slice(graphMessageSize, toSend.length)
        sender ! GraphForFile(sendNow, ctx.self, sendLater.nonEmpty)
        if (sendLater.nonEmpty) {
          sendGraphToDataHolder(sendLater, graphMessageSize, graph, data, nodeLocator, neighborQueries, respondTo, lastIdUsed, sogToSend)
        } else {
          searchOnGraph(graph, data, nodeLocator, neighborQueries, respondTo, lastIdUsed, sogToSend)
        }
    }

  def redistributeGraph(toSend: Map[ActorRef[SearchOnGraphEvent], Seq[Int]],
                        oldGraph: Map[Int, Seq[Int]],
                        nodeLocator: NodeLocator[SearchOnGraphEvent],
                        newGraph: Map[Int, Seq[Int]],
                        nodesExpected: Int,
                        graphMessageSize: Int,
                        data: LocalData[Float],
                        dataUpdated: Boolean,
                        redistributionCoordinator: ActorRef[RedistributionCoordinationEvent]): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial {
      case UpdatedLocalData(newData) =>
        checkIfRedistributionDone(toSend, oldGraph, nodeLocator, newGraph, nodesExpected, graphMessageSize, newData, true, redistributionCoordinator)

      case PartialGraph(partialGraph, sender, moreToSend) =>
        val updatedGraph = newGraph ++ partialGraph
        if (moreToSend) {
          // else this was the last piece of graph from this actor
          sender ! SendPartialGraph(ctx.self)
        }
        checkIfRedistributionDone(toSend, oldGraph, nodeLocator, updatedGraph, nodesExpected, graphMessageSize, data, dataUpdated, redistributionCoordinator)

      case SendPartialGraph(sender) =>
        val stillToSend = toSend(sender)
        val partialGraph = stillToSend.slice(0, graphMessageSize).map(node => (node, oldGraph(node)))
        val toSendLater = stillToSend.slice(graphMessageSize, stillToSend.size)
        sender ! PartialGraph(partialGraph, ctx.self, toSendLater.nonEmpty)
        val updatedToSend = toSend + (sender -> toSendLater)
        checkIfRedistributionDone(updatedToSend, oldGraph, nodeLocator, newGraph, nodesExpected, graphMessageSize, data, dataUpdated, redistributionCoordinator)
  }

  def checkIfRedistributionDone(toSend: Map[ActorRef[SearchOnGraphEvent], Seq[Int]],
                                oldGraph: Map[Int, Seq[Int]],
                                nodeLocator: NodeLocator[SearchOnGraphEvent],
                                newGraph: Map[Int, Seq[Int]],
                                nodesExpected: Int,
                                graphMessageSize: Int,
                                data: LocalData[Float],
                                dataUpdated: Boolean,
                                redistributionCoordinator: ActorRef[RedistributionCoordinationEvent]): Behavior[SearchOnGraphEvent] = {
    val everythingSent = !toSend.valuesIterator.exists(_.nonEmpty)
    if (newGraph.size == nodesExpected && dataUpdated && everythingSent) {
      ctx.log.info("Sent and received everything")
      redistributionCoordinator ! DoneWithRedistribution
      val searchToSend = nodeLocator.allActors.map(worker => worker -> new SOGInfo).toMap
      searchOnGraph(newGraph, data, nodeLocator, Map.empty, Map.empty, lastIdUsed = -1, searchToSend)
    } else {
      redistributeGraph(toSend, oldGraph, nodeLocator, newGraph, nodesExpected, graphMessageSize, data, dataUpdated, redistributionCoordinator)
    }
  }

  def searchOnGraphForNSG(graph: Map[Int, Seq[Int]],
                          data: LocalData[Float],
                          nodeLocator: NodeLocator[SearchOnGraphEvent],
                          pathQueries: Map[Int, QueryInfo],
                          respondTo: Map[Int, ActorRef[BuildNSGEvent]],
                          responseLocations: QueryResponseLocations[Float],
                          toSend: Map[ActorRef[SearchOnGraphEvent], SOGInfo]): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial {
      case CheckedNodesOnSearch(endPoints, startingPoint, neighborsWanted, asker, moreQueries) =>
        if (moreQueries) {
          asker ! GetMorePathQueries(ctx.self)
        }
        // the end point should always be local, because that is how the SoG Actor is chosen
        val newQueries = endPoints.map { endPoint =>
          val query = data.get(endPoint)
          val queryId = endPoint
          ctx.self ! ProcessNextCandidate(queryId)
          // starting point is navigating node, so as of yet not always local
          val pathQueryInfo = if (responseLocations.hasLocation(startingPoint)) {
            val location = responseLocations.location(startingPoint)
            responseLocations.addedToCandidateList(startingPoint, location)
            QueryInfo(query, neighborsWanted, Seq(QueryCandidate(startingPoint, euclideanDist(location, query), processed = false, currentlyProcessing = false)), 0)
          } else {
            val queryInfo = QueryInfo(query, neighborsWanted, Seq.empty, askForLocation(startingPoint, queryId, nodeLocator, toSend))
            queryInfo
          }
          (queryId, pathQueryInfo)
        }
        val newRespondTo = newQueries.map { case(queryId, _) => (queryId, asker)}
        sendMessagesImmediately(toSend)
        searchOnGraphForNSG(graph, data, nodeLocator,
          pathQueries ++ newQueries,
          respondTo  ++ newRespondTo,
          responseLocations,
          toSend)

      case ProcessNextCandidate(queryId) =>
        if (pathQueries.contains(queryId)) {
          val nextToProcess = pathQueries(queryId).candidates.find(query => !query.processed && !query.currentlyProcessing)
          if (nextToProcess.isDefined) {
            nextToProcess.get.currentlyProcessing = true
            askForNeighbors(nextToProcess.get.index, queryId, graph, nodeLocator, toSend)
            sendMessagesImmediately(toSend)
          }
          ctx.self ! ProcessNextCandidate(queryId)
        }
        searchOnGraphForNSG(graph, data, nodeLocator, pathQueries, respondTo, responseLocations, toSend)

      case GetSearchOnGraphInfo(sender) =>
        if (toSend(sender).nonEmpty) {
          val messagesToSend = toSend(sender).sendMessage(settings.maxMessageSize)
          sender ! SearchOnGraphInfo(messagesToSend, ctx.self)
          toSend(sender).sendImmediately = false
        } else {
          toSend(sender).sendImmediately = true
        }
        searchOnGraphForNSG(graph, data, nodeLocator, pathQueries, respondTo, responseLocations, toSend)

      case SearchOnGraphInfo(info, sender) =>
        sender ! GetSearchOnGraphInfo(ctx.self)
        var updatedPathQueries = pathQueries
        info.foreach {
          case GetNeighbors(index) =>
            toSend(sender).addMessage(Neighbors(index, graph(index)))

          case Neighbors(processedIndex, neighbors) =>
            waitingOnNeighbors.received(processedIndex).foreach { queryId =>
              if (updatedPathQueries.contains(queryId)) {
                val queryInfo = updatedPathQueries(queryId)
                updatePathCandidates(queryInfo,  queryId, processedIndex, neighbors, responseLocations, nodeLocator, toSend)
                // check if all candidates have been processed
                val allProcessed = queryInfo.candidates.forall(query => query.processed)
                if (allProcessed && queryInfo.waitingOn == 0) {
                  sendPathResults(queryId, queryInfo, responseLocations, respondTo(queryId))
                  updatedPathQueries -= queryId
                }
              }
            }

          case GetLocation(index) =>
            toSend(sender).addMessage(Location(index, data.get(index)))

          case Location(index, location) =>
            waitingOnLocation.received(index).foreach {queryId =>
              if (updatedPathQueries.contains(queryId)){
                val queryInfo = updatedPathQueries(queryId)
                queryInfo.waitingOn -= 1
                val oldNumberOfCandidates = queryInfo.candidates.length
                val queryFinished = addCandidate(queryInfo, index, location)
                if (queryInfo.candidates.length > oldNumberOfCandidates) { // the candidate has been added
                  responseLocations.addedToCandidateList(index, location)
                }
                if (queryFinished) {
                  updatedPathQueries -= queryId
                  sendPathResults(queryId, queryInfo, responseLocations, respondTo(queryId))
                }
              }
            }
        }
        sendMessagesImmediately(toSend)
        val queriesToRemove = pathQueries.keys.toSet.diff(updatedPathQueries.keys.toSet)
        searchOnGraphForNSG(graph, data, nodeLocator, updatedPathQueries, respondTo -- queriesToRemove, responseLocations, toSend)

      case GetNSGFrom(nsgMerger) =>
        nsgMerger ! GetPartialNSG(graph.keys.toSet, ctx.self)
        waitForNSG(nodeLocator, data)
    }

  def waitForNSG(nodeLocator: NodeLocator[SearchOnGraphEvent], data: LocalData[Float]): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial{
      case PartialNSG(graph) =>
        val myResponsibility = nodeLocator.nodesOf(ctx.self)
        val responsibilityMidPoint = (0 until data.dimension).map(dim => myResponsibility.map(index => data.get(index)).map(_(dim)).sum / myResponsibility.length).toArray
        clusterCoordinator ! NSGonSOG(responsibilityMidPoint, ctx.self)
        val toSend = nodeLocator.allActors.map(worker => worker -> new SOGInfo).toMap
        searchOnGraph(graph, data, nodeLocator, Map.empty, Map.empty, lastIdUsed = -1, toSend)
    }
}



