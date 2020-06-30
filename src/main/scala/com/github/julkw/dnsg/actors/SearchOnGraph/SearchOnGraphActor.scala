package com.github.julkw.dnsg.actors.SearchOnGraph

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.{CoordinationEvent, GetMoreQueries, KNearestNeighbors, NSGonSOG}
import com.github.julkw.dnsg.actors.Coordinators.GraphConnectorCoordinator.{ConnectionCoordinationEvent, ReceivedNewEdge}
import com.github.julkw.dnsg.actors.Coordinators.GraphRedistributionCoordinator.{DoneWithRedistribution, RedistributionCoordinationEvent}
import com.github.julkw.dnsg.actors.DataHolder.{GraphForFile, LoadDataEvent}
import com.github.julkw.dnsg.actors.GraphConnector
import com.github.julkw.dnsg.actors.NodeLocatorHolder.{NodeLocationEvent, SearchOnGraphGotGraphFrom}
import com.github.julkw.dnsg.actors.SearchOnGraph.SOGInfo.{GetLocation, GetNeighbors, Location, Neighbors, SOGEvent}
import com.github.julkw.dnsg.actors.createNSG.NSGMerger.{GetPartialNSG, MergeNSGEvent}
import com.github.julkw.dnsg.actors.createNSG.NSGWorker.{BuildNSGEvent, GetMorePathQueries}
import com.github.julkw.dnsg.actors.nndescent.KnngWorker.BuildGraphEvent
import com.github.julkw.dnsg.util.Data.{CacheData, LocalData}
import com.github.julkw.dnsg.util._

import scala.language.postfixOps

object SearchOnGraphActor {

  sealed trait SearchOnGraphEvent extends dNSGSerializable

  // setup
  final case class GraphAndData(graph: Map[Int, Seq[Int]], cacheData: CacheData[Float], sender: ActorRef[BuildGraphEvent]) extends SearchOnGraphEvent

  final case class GraphReceived(graphHolder: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class GraphDistribution(nodeLocator: NodeLocator[SearchOnGraphEvent]) extends SearchOnGraphEvent

  // redistribution
  final case class RedistributeGraph(primaryAssignments: NodeLocator[SearchOnGraphEvent], secondaryAssignments: Map[Int, Set[ActorRef[SearchOnGraphEvent]]], redistributionCoordinator: ActorRef[RedistributionCoordinationEvent]) extends SearchOnGraphEvent

  final case class SendPartialGraph(size: Int, sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class PartialGraph(partialGraph: Seq[(Int, Seq[Int])], sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class UpdatedLocalData(data: LocalData[Float]) extends  SearchOnGraphEvent

  // queries
  final case class FindNearestNeighbors(queries: Seq[Seq[Float]], k: Int, asker: ActorRef[CoordinationEvent], moreQueries: Boolean) extends SearchOnGraphEvent

  final case class FindNearestNeighborsStartingFrom(queries: Seq[Seq[Float]], startingPoint: Int,  k: Int, asker: ActorRef[CoordinationEvent], moreQueries: Boolean) extends SearchOnGraphEvent

  final case class CheckedNodesOnSearch(queries: Seq[Int], startingPoint: Int, k: Int, asker: ActorRef[BuildNSGEvent], moreQueries: Boolean) extends SearchOnGraphEvent

  // search
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
    new SearchOnGraphActor(clusterCoordinator, nodeLocatorHolder, new WaitingOnLocation, settings, ctx).waitForLocalGraph()
  }
}

class SearchOnGraphActor(clusterCoordinator: ActorRef[CoordinationEvent],
                         nodeLocatorHolder: ActorRef[NodeLocationEvent],
                         waitingOnLocation: WaitingOnLocation,
                         settings: Settings,
                         ctx: ActorContext[SearchOnGraphActor.SearchOnGraphEvent])
  extends SearchOnGraph(waitingOnLocation, settings.maxMessageSize, settings.maxNeighborCandidates, ctx) {
  import SearchOnGraphActor._

  def waitForLocalGraph(): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial{
      case GraphAndData(graph, localData, sender) =>
        // The data is send over with cache because there is a 1:1 mapping between knngWorkers and searchOnGraphActors and the first stops using the data after it has send it to the latter
        nodeLocatorHolder ! SearchOnGraphGotGraphFrom(sender, ctx.self)
        waitForDistributionInfo(graph, localData)
    }

  def waitForDistributionInfo(graph: Map[Int, Seq[Int]], data: CacheData[Float]): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial{
      case GraphDistribution(nodeLocator) =>
        val toSend = nodeLocator.allActors.map(worker => worker -> new SOGInfo).toMap
        searchOnGraph(graph, data, nodeLocator, Map.empty, Map.empty, lastIdUsed = -1, toSend)

      case FindNearestNeighbors(queries, k, asker, moreQueries) =>
        ctx.self ! FindNearestNeighbors(queries, k, asker, moreQueries)
        waitForDistributionInfo(graph, data)

      case GetSearchOnGraphInfo(sender) =>
        ctx.self ! GetSearchOnGraphInfo(sender)
        waitForDistributionInfo(graph, data)

      case SendGraphForFile(sender) =>
        ctx.self ! SendGraphForFile(sender)
        waitForDistributionInfo(graph, data)
    }

  def searchOnGraph(graph: Map[Int, Seq[Int]],
                    data: CacheData[Float],
                    nodeLocator: NodeLocator[SearchOnGraphEvent],
                    neighborQueries: Map[Int, QueryInfo],
                    respondTo: Map[Int, ActorRef[CoordinationEvent]],
                    lastIdUsed: Int,
                    toSend: Map[ActorRef[SearchOnGraphEvent], SOGInfo]): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial {
      case FindNearestNeighbors(queries, k, asker, moreQueries) =>
        if (moreQueries) {
          asker ! GetMoreQueries(ctx.self)
        }
        var lastQueryId = lastIdUsed

        // choose node to start search from local nodes
        val newQueries = queries.map { query =>
          val queryId = lastQueryId + 1
          // initialize candidate pool with random nodes
          val r = scala.util.Random
          val graphSize = nodeLocator.graphSize
          // ensure at least one of the initial candidates is local
          val initialCandidates = ((0 until k - 1).map(_ => r.nextInt(graphSize)) :+ graph.head._1).toSet
          val (localCandidates, remoteCandidates) = initialCandidates.partition(potentialCandidate => data.isLocal(potentialCandidate))
          val newCandidates = localCandidates.map { candidateIndex =>
            val location = data.get(candidateIndex)
            QueryCandidate(candidateIndex, euclideanDist(query, location), processed = false)
          }.toSeq.sortBy(_.distance).slice(0, k)
          // candidates for which we don't have the location have to ask for it first
          var waitingOn = 0
          remoteCandidates.foreach(remoteNeighbor => waitingOn += askForLocation(remoteNeighbor, queryId, nodeLocator, toSend))
          val queryInfo = QueryInfo(query, k, newCandidates, waitingOn)
          askForNeighbors(newCandidates.head.index, queryId, graph, nodeLocator, toSend)
          lastQueryId = queryId
          (queryId, queryInfo)
        }
        sendMessagesImmediately(toSend)
        val newRespondTo = newQueries.map { case (queryId, _) => (queryId, asker) }
        searchOnGraph(graph, data, nodeLocator, neighborQueries ++ newQueries, respondTo ++ newRespondTo, lastQueryId, toSend)

      case FindNearestNeighborsStartingFrom(queries, startingPoint, k, asker, moreQueries) =>
        if (moreQueries) {
          asker ! GetMoreQueries(ctx.self)
        }
        var lastQueryId = lastIdUsed
        val newQueries = queries.map { query =>
          val queryId = lastQueryId + 1
          val queryInfo = if (data.isLocal(startingPoint)) {
            val location = data.get(startingPoint)
            askForNeighbors(startingPoint, queryId, graph, nodeLocator, toSend)
            toSend(nodeLocator.findResponsibleActor(startingPoint)).addMessage(GetNeighbors(startingPoint, queryId))
            QueryInfo(query, k, Seq(QueryCandidate(startingPoint, euclideanDist(location, query), processed = false)), 0)
          } else {
            val qi = QueryInfo(query, k, Seq.empty, askForLocation(startingPoint, queryId, nodeLocator, toSend))
            qi
          }
          lastQueryId = queryId
          (queryId, queryInfo)
        }
        sendMessagesImmediately(toSend)
        val newRespondTo = newQueries.map { case (queryId, _) => (queryId, asker) }
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

      case SearchOnGraphInfo(info, sender) =>
        var updatedNeighborQueries = neighborQueries
        info.foreach {
          case GetNeighbors(index, queryId) =>
            toSend(sender).addMessage(Neighbors(queryId, index, graph(index)))

          case Neighbors(queryId, processedIndex, neighbors) =>
            if (updatedNeighborQueries.contains(queryId)) {
              val queryInfo = updatedNeighborQueries(queryId)
              updateCandidates(queryInfo, queryId, processedIndex, neighbors.diff(queryInfo.candidates), nodeLocator, data, toSend)
              // check if all candidates have been processed
              val nextCandidateToProcess = queryInfo.candidates.find(query => !query.processed)
              nextCandidateToProcess match {
                case Some(nextCandidate) =>
                  // find the neighbors of the next candidate to be processed and update queries
                  askForNeighbors(nextCandidate.index, queryId, graph, nodeLocator, toSend)
                case None =>
                  if (updatedNeighborQueries(queryId).waitingOn == 0) {
                    val finalNeighbors = queryInfo.candidates.map(_.index)
                    respondTo(queryId) ! KNearestNeighbors(queryInfo.query, finalNeighbors)
                    updatedNeighborQueries -= queryId
                  }
              }
            }

          case GetLocation(index) =>
            toSend(sender).addMessage(Location(index, data.get(index)))

          case Location(index, location) =>
            data.add(index, location)
            waitingOnLocation.received(index).foreach {queryId =>
              if (updatedNeighborQueries.contains(queryId)) {
                val queryInfo = updatedNeighborQueries(queryId)
                queryInfo.waitingOn -= 1
                val queryFinished = addCandidate(queryInfo, queryId, index, location, graph, nodeLocator, toSend)
                if (queryFinished) {
                  val finalNeighbors = queryInfo.candidates.map(_.index)
                  respondTo(queryId) ! KNearestNeighbors(queryInfo.query, finalNeighbors)
                  updatedNeighborQueries -= queryId
                }
              }
            }
        }
        sender ! GetSearchOnGraphInfo(ctx.self)
        sendMessagesImmediately(toSend)
        val queriesToRemove = neighborQueries.keys.toSet.diff(updatedNeighborQueries.keys.toSet)
        searchOnGraph(graph, data, nodeLocator, updatedNeighborQueries, respondTo -- queriesToRemove, lastIdUsed, toSend)

      case AddToGraph(startNode, endNode, sender) =>
        sender ! ReceivedNewEdge
        val newNeighbors = graph(startNode) :+ endNode
        searchOnGraph(graph + (startNode -> newNeighbors), data, nodeLocator, neighborQueries, respondTo, lastIdUsed, toSend)

      case CheckedNodesOnSearch(endPoints, startingPoint, neighborsWanted, asker, moreQueries) =>
        ctx.self ! CheckedNodesOnSearch(endPoints, startingPoint, neighborsWanted, asker, moreQueries)
        toSend.foreach { case (_, sendInfo) => sendInfo.sendImmediately = true }
        sendMessagesImmediately(toSend)
        searchOnGraphForNSG(graph, data, nodeLocator, Map.empty, Map.empty, QueryResponseLocations(data), toSend)

      case ConnectGraph(graphConnectorSupervisor) =>
        val responsibility = graph.keys.filter(node => nodeLocator.findResponsibleActor(node) == ctx.self).toSeq
        ctx.spawn(GraphConnector(data.data, graph, responsibility, graphConnectorSupervisor, nodeLocatorHolder, ctx.self), name="graphConnector")
        searchOnGraph(graph, data, nodeLocator, neighborQueries, respondTo, lastIdUsed, toSend)

      case RedistributeGraph(primaryAssignments, secondaryAssignments, redistributionCoordinator) =>
        // if a worker is the primary assignee for a graph_node it should not appear with the secondary assignees
        val nodesExpected = primaryAssignments.numberOfNodes(ctx.self) +
          secondaryAssignments.valuesIterator.count(assignees => assignees.contains(ctx.self))
        val toSend = graph.keys.groupBy(index => primaryAssignments.findResponsibleActor(index)).transform { (worker, nodes) =>
          val alsoSend = secondaryAssignments.keys.filter(node => graph.contains(node) && secondaryAssignments(node).contains(worker))
          (nodes ++ alsoSend).toSeq
        }
        val graphMessageSize = settings.maxMessageSize / (settings.k + 1)
        nodeLocator.allActors.foreach(graphHolder => graphHolder ! SendPartialGraph(graphMessageSize, ctx.self))
        redistributeGraph(toSend, graph, primaryAssignments, Map.empty, nodesExpected, graphMessageSize, data, dataUpdated = false, redistributionCoordinator)

      case SendPartialGraph(size, sender) =>
        ctx.self ! SendPartialGraph(size, sender)
        searchOnGraph(graph, data, nodeLocator, neighborQueries, respondTo, lastIdUsed, toSend)

      case SendGraphForFile(sender) =>
        val graphMessageSize = settings.maxMessageSize / (settings.k + 1)
        ctx.self ! SendGraphForFile(sender)
        // only send the graph information for the nodes for which I am the primary assignee
        val nodesToSend = nodeLocator.nodesOf(ctx.self)
        sendGraphToDataHolder(nodesToSend, graphMessageSize, sender, graph, data, nodeLocator, neighborQueries, respondTo, lastIdUsed, toSend)
    }

  def sendGraphToDataHolder(toSend: Seq[Int],
                            graphMessageSize: Int,
                            sendTo: ActorRef[LoadDataEvent],
                            graph: Map[Int, Seq[Int]],
                            data: CacheData[Float],
                            nodeLocator: NodeLocator[SearchOnGraphEvent],
                            neighborQueries: Map[Int, QueryInfo],
                            respondTo: Map[Int, ActorRef[CoordinationEvent]],
                            lastIdUsed: Int,
                            sogToSend: Map[ActorRef[SearchOnGraphEvent], SOGInfo]): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial {
      case SendGraphForFile(sender) =>
        val sendNow = toSend.slice(0, graphMessageSize).map(index => (index, graph(index)))
        val sendLater = toSend.slice(graphMessageSize, toSend.length)
        sendTo ! GraphForFile(sendNow, ctx.self, sendLater.nonEmpty)
        if (sendLater.nonEmpty) {
          sendGraphToDataHolder(sendLater, graphMessageSize, sendTo, graph, data, nodeLocator, neighborQueries, respondTo, lastIdUsed, sogToSend)
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
                        data: CacheData[Float],
                        dataUpdated: Boolean,
                        redistributionCoordinator: ActorRef[RedistributionCoordinationEvent]): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial {
      case UpdatedLocalData(newData) =>
        val updatedData = CacheData(settings.cacheSize, newData)
        checkIfRedistributionDone(toSend, oldGraph, nodeLocator, newGraph, nodesExpected, graphMessageSize, updatedData, dataUpdated = true, redistributionCoordinator)

      case PartialGraph(partialGraph, sender) =>
        val updatedGraph = newGraph ++ partialGraph
        //ctx.log.info("Received {} nodes of {}", updatedGraph.size, nodesExpected)
        if (partialGraph.size == graphMessageSize) {
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
      val searchToSend = nodeLocator.allActors.map(worker => worker -> new SOGInfo).toMap
      searchOnGraph(newGraph, data, nodeLocator, Map.empty, Map.empty, lastIdUsed = -1, searchToSend)
    } else {
      redistributeGraph(toSend, oldGraph, nodeLocator, newGraph, nodesExpected, graphMessageSize, data, dataUpdated, redistributionCoordinator)
    }
  }

  def searchOnGraphForNSG(graph: Map[Int, Seq[Int]],
                          data: CacheData[Float],
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
          // starting point is navigating node, so as of yet not always local
          val pathQueryInfo = if (responseLocations.hasLocation(startingPoint)) {
            val location = responseLocations.location(startingPoint)
            responseLocations.addedToCandidateList(startingPoint, location)
            askForNeighbors(startingPoint, queryId, graph, nodeLocator, toSend)
            QueryInfo(query, neighborsWanted, Seq(QueryCandidate(startingPoint, euclideanDist(location, query), processed = false)), 0)
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
        var updatedPathQueries = pathQueries
        info.foreach {
          case GetNeighbors(index, query) =>
            toSend(sender).addMessage(Neighbors(query, index, graph(index)))

          case Neighbors(queryId, processedIndex, neighbors) =>
            val queryInfo = updatedPathQueries(queryId)
            updatePathCandidates(queryInfo,  queryId, processedIndex, neighbors, responseLocations, nodeLocator, toSend)
            // check if all candidates have been processed
            val nextCandidateToProcess = queryInfo.candidates.find(query => !query.processed)
            nextCandidateToProcess match {
              case Some(nextCandidate) =>
                // find the neighbors of the next candidate to be processed and update queries
                askForNeighbors(nextCandidate.index, queryId, graph, nodeLocator, toSend)
              case None =>
                if (updatedPathQueries(queryId).waitingOn > 0) {
                  // do nothing for now
                } else {
                  sendResults(queryId, queryInfo, responseLocations, respondTo(queryId))
                  updatedPathQueries -= queryId
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
                val queryFinished = addCandidate(queryInfo, queryId, index, location, graph, nodeLocator, toSend)
                if (queryInfo.candidates.length > oldNumberOfCandidates) { // the candidate has been added
                  responseLocations.addedToCandidateList(index, location)
                }
                if (queryFinished) {
                  updatedPathQueries -= queryId
                  sendResults(queryId, queryInfo, responseLocations, respondTo(queryId))
                }
              }
            }
        }
        sender ! GetSearchOnGraphInfo(ctx.self)
        sendMessagesImmediately(toSend)
        val queriesToRemove = pathQueries.keys.toSet.diff(updatedPathQueries.keys.toSet)
        searchOnGraphForNSG(graph, data, nodeLocator, updatedPathQueries, respondTo -- queriesToRemove, responseLocations, toSend)

      case GetNSGFrom(nsgMerger) =>
        nsgMerger ! GetPartialNSG(graph.keys.toSet, ctx.self)
        waitForNSG(nodeLocator, data)
    }

  def waitForNSG(nodeLocator: NodeLocator[SearchOnGraphEvent], data: CacheData[Float]): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial{
      case PartialNSG(graph) =>
        val myResponsibility = nodeLocator.nodesOf(ctx.self)
        val responsibilityMidPoint = (0 until data.data.dimension).map(dim => myResponsibility.map(index => data.get(index)).map(_(dim)).sum / myResponsibility.length)
        clusterCoordinator ! NSGonSOG(responsibilityMidPoint, ctx.self)
        val toSend = nodeLocator.allActors.map(worker => worker -> new SOGInfo).toMap
        searchOnGraph(graph, data, nodeLocator, Map.empty, Map.empty, lastIdUsed = -1, toSend)
    }
}



