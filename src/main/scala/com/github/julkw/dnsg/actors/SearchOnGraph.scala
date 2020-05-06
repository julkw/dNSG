package com.github.julkw.dnsg.actors

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.julkw.dnsg.actors.ClusterCoordinator.{AllConnected, CoordinationEvent, FinishedUpdatingConnectivity, KnngDistributionInfo, SearchOnGraphDistributionInfo, UnconnectedNode, UpdatedToNSG}
import com.github.julkw.dnsg.actors.createNSG.NSGMerger.{GetPartialGraph, MergeNSGEvent}
import com.github.julkw.dnsg.actors.createNSG.NSGWorker.{BuildNSGEvent, Responsibility}
import com.github.julkw.dnsg.util.{Distance, LocalData, NodeLocator, dNSGSerializable}

import scala.collection.mutable
import scala.language.postfixOps


object SearchOnGraph {

  sealed trait SearchOnGraphEvent extends dNSGSerializable

  // setup
  final case class Graph(graph: Map[Int, Seq[Int]], sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class GraphReceived(graphHolder: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class GraphDistribution(nodeLocator: NodeLocator[SearchOnGraphEvent]) extends SearchOnGraphEvent

  // queries
  final case class FindNearestNeighbors(query: Seq[Float], asker: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class FindNearestNeighborsStartingFrom(query: Seq[Float], startingPoint: Int, asker: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class CheckedNodesOnSearch(endPoint: Int, startingPoint: Int, asker: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  // search
  final case class GetNeighbors(index: Int, query: Query, sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class Neighbors(query: Query, index: Int, neighbors: Seq[Int]) extends SearchOnGraphEvent

  final case class GetLocation(index: Int, query: Query, sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  final case class Location(index: Int, query: Query, location: Seq[Float]) extends SearchOnGraphEvent

  // answering queries
  final case class KNearestNeighbors(query: Seq[Float], neighbors: Seq[Int]) extends SearchOnGraphEvent

  final case class SortedCheckedNodes(queryIndex: Int, checkedNodes: Seq[(Int, Seq[Float])]) extends SearchOnGraphEvent

  // send responsiblities to NSG workers
  final case class SendResponsibleIndicesTo(nsgWorker: ActorRef[BuildNSGEvent]) extends SearchOnGraphEvent
  // get NSG from NSGMerger
  final case class GetNSGFrom(nsgMerger: ActorRef[MergeNSGEvent]) extends SearchOnGraphEvent

  final case class PartialNSG(graph: Map[Int, Seq[Int]]) extends SearchOnGraphEvent

  // check for Connectivity
  final case class UpdateConnectivity(root: Int) extends SearchOnGraphEvent

  final case class IsConnected(connectedNode: Int, parent: Int) extends SearchOnGraphEvent

  final case class DoneConnectingChildren(nodeAwaitingAnswer: Int) extends SearchOnGraphEvent

  final case class FindUnconnectedNode(sendTo: ActorRef[CoordinationEvent], notAskedYet: Set[ActorRef[SearchOnGraphEvent]]) extends SearchOnGraphEvent

  final case class AddToGraph(startNode: Int, endNode: Int) extends SearchOnGraphEvent

  // safe knng to file
  final case class GetGraph(sender: ActorRef[SearchOnGraphEvent]) extends SearchOnGraphEvent

  // data type for more readable code
  protected case class Query(point: Seq[Float], asker: ActorRef[SearchOnGraphEvent])

  protected case class QueryCandidate(index: Int, distance: Double, var processed: Boolean)

  protected case class PathQueryInfo(queryIndex: Int, var checkedCandidates: Seq[(Int, Double)])

  protected case class MessageCounter(var waitingForMessages: Int, parentNode: Int)

  protected case class ConnectivityInfo(connectedNodes: mutable.Set[Int], messageTracker: mutable.Map[Int, MessageCounter])

  // TODO change to class that deletes entries when usedIn == 0
  protected case class ResponseLocation(data: Seq[Float], var usedIn: Int)

  protected case class CandidateList(var candidates: Seq[QueryCandidate], var waitingOn: Int)

  def apply(supervisor: ActorRef[CoordinationEvent],
            data: LocalData[Float],
            k: Int): Behavior[SearchOnGraphEvent] = Behaviors.setup { ctx =>
    //ctx.log.info("Started SearchOnGraph")
    Behaviors.setup(ctx => new SearchOnGraph(supervisor, data, k, ctx).waitForLocalGraph())
  }
}

class SearchOnGraph(supervisor: ActorRef[CoordinationEvent],
                    data: LocalData[Float],
                    k: Int,
                    ctx: ActorContext[SearchOnGraph.SearchOnGraphEvent]) extends Distance {
  import SearchOnGraph._

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
        searchOnGraph(graph, nodeLocator, Map.empty, Map.empty, Map.empty, None)
    }

  def searchOnGraph(graph: Map[Int, Seq[Int]],
                    nodeLocator: NodeLocator[SearchOnGraphEvent],
                    neighborQueries: Map[Query, CandidateList],
                    pathQueries: Map[Query, PathQueryInfo],
                    responseLocations: Map[Int, ResponseLocation],
                    connectivityInfo: Option[ConnectivityInfo]): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessage {
      case FindNearestNeighbors(query, asker) =>
        // choose node to start search from local nodes
        val startingNodeIndex: Int = graph.keys.head
        val candidateList = CandidateList(Seq(QueryCandidate(startingNodeIndex, euclideanDist(data.get(startingNodeIndex), query), processed=false)), 0)
        // since this node is located locally, just ask self
        ctx.self ! GetNeighbors(startingNodeIndex, Query(query, asker), ctx.self)
        searchOnGraph(graph, nodeLocator, neighborQueries + (Query(query, asker) -> candidateList), pathQueries, responseLocations, connectivityInfo)

      case FindNearestNeighborsStartingFrom(query, startingPoint, asker) =>
        val candidateList = CandidateList(Seq(QueryCandidate(startingPoint, euclideanDist(data.get(startingPoint), query), processed=false)), 0)
        nodeLocator.findResponsibleActor(startingPoint) ! GetNeighbors(startingPoint, Query(query, asker), ctx.self)
        searchOnGraph(graph, nodeLocator, neighborQueries + (Query(query, asker) -> candidateList), pathQueries, responseLocations, connectivityInfo)

      case CheckedNodesOnSearch(endPoint, startingPoint, asker) =>
        val query = data.get(endPoint)
        val candidateList = CandidateList(Seq(QueryCandidate(startingPoint, euclideanDist(data.get(startingPoint), query), processed=false)), 0)
        nodeLocator.findResponsibleActor(startingPoint) ! GetNeighbors(startingPoint, Query(query, asker), ctx.self)
        searchOnGraph(graph, nodeLocator,
          neighborQueries + (Query(query, asker) -> candidateList),
          pathQueries + (Query(query, asker) -> PathQueryInfo(endPoint, Seq.empty)),
          responseLocations,
          connectivityInfo)

      case GetNeighbors(index, query, sender) =>
        sender ! Neighbors(query, index, graph(index))
        searchOnGraph(graph, nodeLocator, neighborQueries, pathQueries, responseLocations, connectivityInfo)

      case Neighbors(query, processedIndex, neighbors) =>
        val updatedCandidates = updateCandidates(neighborQueries(query).candidates, query, processedIndex, neighbors, pathQueries)
        neighborQueries(query).candidates = updatedCandidates
        // check if all candidates have been processed
        val nextCandidateToProcess = updatedCandidates.find(query => !query.processed)
        nextCandidateToProcess match {
          case Some(nextCandidate) =>
            // find the neighbors of the next candidate to be processed and update queries
            nodeLocator.findResponsibleActor(nextCandidate.index) ! GetNeighbors(nextCandidate.index, query, ctx.self)
            searchOnGraph(graph, nodeLocator, neighborQueries, pathQueries, responseLocations, connectivityInfo)
          case None =>
            val stillWaitingOnLocations = neighborQueries(query).waitingOn > 0
            if (stillWaitingOnLocations) {
              // do nothing for now
              searchOnGraph(graph, nodeLocator, neighborQueries, pathQueries, responseLocations, connectivityInfo)
            } else if (pathQueries.contains(query)) {
              // send back the checked nodes instead of the result
              val result = pathQueries(query)
              val sortedCandidates = result.checkedCandidates.sortBy(_._2).map { node =>
                val nodeData = responseLocations(node._1)
                nodeData.usedIn -= 1
                //TODO  if 0 can be remove
                (node._1, nodeData.data)
              }
              query.asker ! SortedCheckedNodes(result.queryIndex, sortedCandidates)
              searchOnGraph(graph, nodeLocator, neighborQueries - query, pathQueries - query, responseLocations, connectivityInfo)
            } else {
              val nearestNeighbors: Seq[Int] = updatedCandidates.map(_.index)
              query.asker ! KNearestNeighbors(query.point, nearestNeighbors)
              searchOnGraph(graph, nodeLocator, neighborQueries - query, pathQueries - query, responseLocations, connectivityInfo)
            }
            searchOnGraph(graph, nodeLocator, neighborQueries - query, pathQueries - query, responseLocations, connectivityInfo)
        }

      case GetLocation(index, query, sender) =>
        // TODO actually call at some point
        sender ! Location(index, query, data.get(index))
        searchOnGraph(graph, nodeLocator, neighborQueries, pathQueries, responseLocations, connectivityInfo)

      case Location(index, query, location) =>
        val currentCandidates = neighborQueries(query).candidates
        neighborQueries(query).waitingOn -= 1
        // TODO if 0 check if all processed -> if yes send of
        val allProcessed = !currentCandidates.exists(_.processed == false)
        // check if valid candidate
        val alreadyAdded = currentCandidates.exists(candidate => candidate.index == index)
        val dist = euclideanDist(query.point, location)
        if (!alreadyAdded && currentCandidates(currentCandidates.length).distance > dist) {
          // update candidates
          val updatedCandidates = (currentCandidates :+ QueryCandidate(index, euclideanDist(query.point, location), false)).sortBy(candidate => candidate.distance).slice(0, k)
          neighborQueries(query).candidates = updatedCandidates
          // If all other candidates have already been processed, the new now needs to be processed
          if (allProcessed) {
            nodeLocator.findResponsibleActor(index) ! GetNeighbors(index, query, ctx.self)
          }
          // update responseLocations
          // TODO remove if set to 0
          responseLocations(currentCandidates(currentCandidates.length).index).usedIn -= 1
          // TODO move this code to class. Seriously.
          val oldNumberOfUsers =
            if (responseLocations.contains(index)) {
              responseLocations(index).usedIn
            } else {
              0
            }
          val updatedResponseLocations = responseLocations + (index -> ResponseLocation(location, oldNumberOfUsers + 1))
          searchOnGraph(graph, nodeLocator, neighborQueries, pathQueries, updatedResponseLocations, connectivityInfo)
        } else {
          // TODO if waitingOn now to 0 and allProcessed send Results (which should also be a function)
          searchOnGraph(graph, nodeLocator, neighborQueries, pathQueries, responseLocations, connectivityInfo)
        }


      case UpdateConnectivity(root) =>
        ctx.log.info("Updating connectivity")
        connectivityInfo match {
          case Some(cInfo) =>
            cInfo.connectedNodes.add(root)
            updateNeighbors(root, root, cInfo, graph, nodeLocator)
            searchOnGraph(graph, nodeLocator, neighborQueries, pathQueries, responseLocations, connectivityInfo)

          case None =>
            val cInfo = ConnectivityInfo(mutable.Set(root), mutable.Map.empty)
            updateNeighbors(root, root, cInfo, graph, nodeLocator)
            searchOnGraph(graph, nodeLocator, neighborQueries, pathQueries, responseLocations, Some(cInfo))
        }

      case IsConnected(connectedNode, parent) =>
        connectivityInfo match {
          case Some(cInfo) =>
            // in case the parent is placed on another node this might not be known here
            cInfo.connectedNodes.add(parent)
            if (cInfo.connectedNodes.contains(connectedNode)) {
              nodeLocator.findResponsibleActor(parent) ! DoneConnectingChildren(parent)
            } else {
              cInfo.connectedNodes.add(connectedNode)
              updateNeighbors(connectedNode, parent, cInfo, graph, nodeLocator)
            }
            searchOnGraph(graph, nodeLocator, neighborQueries, pathQueries, responseLocations, connectivityInfo)

          case None =>
            val cInfo = ConnectivityInfo(mutable.Set(connectedNode, parent), mutable.Map.empty)
            updateNeighbors(connectedNode, parent, cInfo, graph, nodeLocator)
            searchOnGraph(graph, nodeLocator, neighborQueries, pathQueries, responseLocations, Some(cInfo))
        }

      case DoneConnectingChildren(nodeAwaitingAnswer) =>
        connectivityInfo match {
          case Some(cInfo) =>
            val messageCounter = cInfo.messageTracker(nodeAwaitingAnswer)
            messageCounter.waitingForMessages -= 1
            if (messageCounter.waitingForMessages == 0) {
              if (messageCounter.parentNode == nodeAwaitingAnswer) {
                supervisor ! FinishedUpdatingConnectivity
                ctx.log.info("Done with updating connectivity")
              } else {
                nodeLocator.findResponsibleActor(messageCounter.parentNode) !
                  DoneConnectingChildren(messageCounter.parentNode)
                cInfo.messageTracker -= nodeAwaitingAnswer
              }
            }
        }
        searchOnGraph(graph, nodeLocator, neighborQueries, pathQueries, responseLocations, connectivityInfo)

      case FindUnconnectedNode(sendTo, notAskedYet) =>
        connectivityInfo match {
          case None =>
            ctx.log.info("Asked for unconnected nodes before updating connectivity")
            sendTo ! UnconnectedNode(graph.head._1, data.get(graph.head._1))
          case Some(cInfo) =>
            val unconnectedNodes = graph.keys.toSet -- cInfo.connectedNodes
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
        }
        searchOnGraph(graph, nodeLocator, neighborQueries, pathQueries, responseLocations, connectivityInfo)

      case AddToGraph(startNode, endNode) =>
        ctx.log.info("Add edge to graph to ensure connectivity")
        val updatedNeighbors = graph(startNode) :+ endNode
        searchOnGraph(graph + (startNode -> updatedNeighbors), nodeLocator, neighborQueries, pathQueries, responseLocations, connectivityInfo)

      case GetNSGFrom(nsgMerger) =>
        //ctx.log.info("Asking NSG Merger for my part of the NSG")
        nsgMerger ! GetPartialGraph(graph.keys.toSet, ctx.self)
        waitForNSG(nodeLocator)

      case GetGraph(sender) =>
        //ctx.log.info("Asked for graph info")
        sender ! Graph(graph, ctx.self)
        searchOnGraph(graph, nodeLocator, neighborQueries, pathQueries, responseLocations, connectivityInfo)

      case SendResponsibleIndicesTo(nsgWorker) =>
        nsgWorker ! Responsibility(graph.keys.toSeq)
        searchOnGraph(graph, nodeLocator, neighborQueries, pathQueries, responseLocations, connectivityInfo)
    }

  def waitForNSG(nodeLocator: NodeLocator[SearchOnGraphEvent]): Behavior[SearchOnGraphEvent] =
    Behaviors.receiveMessagePartial{
      case PartialNSG(graph) =>
        ctx.log.info("Received nsg, ready for queries/establishing connectivity")
        supervisor ! UpdatedToNSG
        searchOnGraph(graph, nodeLocator, Map.empty, Map.empty, Map.empty, None)
    }

  def updateNeighbors(node: Int,
                      parent: Int,
                      connectivityInfo: ConnectivityInfo,
                      graph: Map[Int, Seq[Int]],
                      nodeLocator: NodeLocator[SearchOnGraphEvent]): Unit = {
    var sendMessages = 0
    // tell all neighbors they are connected
    graph(node).foreach { neighborIndex =>
      if (!connectivityInfo.connectedNodes.contains(neighborIndex)) {
        nodeLocator.findResponsibleActor(neighborIndex) ! IsConnected(neighborIndex, node)
        sendMessages += 1
      }
    }
    if (sendMessages > 0) {
      connectivityInfo.messageTracker += (node -> MessageCounter(sendMessages, parent))
    } else if (node != parent) {
      nodeLocator.findResponsibleActor(parent) ! DoneConnectingChildren(parent)
    } else { // no neighbors updated and this is the root
      supervisor ! FinishedUpdatingConnectivity
      ctx.log.info("None of the previously unconnected nodes are connected to the root")
    }
  }

  def updateCandidates(currentCandidates: Seq[QueryCandidate],
                       query: Query,
                       processedIndex: Int,
                       neighbors: Seq[Int],
                       pathQueries: Map[Query, PathQueryInfo]): Seq[QueryCandidate] = {
    val processedCandidate = currentCandidates.find(query => query.index == processedIndex)
    // check if I still care about these neighbors or if the node they belong to has already been kicked out of the candidate list
    processedCandidate match {
      case Some(candidate) =>
        // update candidates
        val currentCandidateIndices = currentCandidates.map(_.index)
        // only add candidates that are not already in the candidateList
        // TODO check if candidate is local and if not ask for location and count waitinOn up
        val newCandidates = neighbors.diff(currentCandidateIndices).map(
          candidateIndex => QueryCandidate(candidateIndex, euclideanDist(query.point, data.get(candidateIndex)), processed=false))
        val updatedCandidates = (currentCandidates ++: newCandidates).sortBy(_.distance).slice(0, k)
        // mark candidate as processed
        candidate.processed = true
        if (pathQueries.contains(query)) {
          pathQueries(query).checkedCandidates = pathQueries(query).checkedCandidates :+ (candidate.index, candidate.distance)
        }
        updatedCandidates
      case None =>
        currentCandidates
    }
  }
}




