package com.github.julkw.dnsg.actors.SearchOnGraph

import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.ActorContext
import com.github.julkw.dnsg.actors.ClusterCoordinator.{CoordinationEvent, FinishedUpdatingConnectivity}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.{DoneConnectingChildren, GetLocation, GetNeighbors, IsConnected, SearchOnGraphEvent}
import com.github.julkw.dnsg.actors.createNSG.NSGWorker.{BuildNSGEvent, SortedCheckedNodes}
import com.github.julkw.dnsg.util.Data.{CacheData, LocalData}
import com.github.julkw.dnsg.util.{Distance, NodeLocator, WaitingOnLocation}

import scala.collection.mutable

abstract class SearchOnGraph(supervisor: ActorRef[CoordinationEvent], waitingOnLocation: WaitingOnLocation, ctx: ActorContext[SearchOnGraphActor.SearchOnGraphEvent]) extends Distance {
  // data type for more readable code
  protected case class QueryCandidate(index: Int, distance: Double, var processed: Boolean)
  // TODO rename to something that includes all information contained
  // also refactor to remove vars?
  protected case class QueryInfo(query: Seq[Float], neighborsWanted: Int, var candidates: Seq[QueryCandidate], var waitingOn: Int)

  //protected case class CandidateList(var candidates: Seq[QueryCandidate], var waitingOn: Int, neighborsWanted: Int)

  protected case class MessageCounter(var waitingForMessages: Int, parentNode: Int)

  protected case class ConnectivityInfo(connectedNodes: mutable.Set[Int], messageTracker: mutable.Map[Int, MessageCounter])


  def updateNeighborConnectedness(node: Int,
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

  // TODO the next two functions are not great because of code duplication and working on data structures in place. Maybe Refactor
  def updateCandidates(queryInfo: QueryInfo,
                       queryId: Int,
                       processedIndex: Int,
                       potentialNewCandidates: Seq[Int],
                       nodeLocator: NodeLocator[SearchOnGraphEvent],
                       data: CacheData[Float]): Unit = {
    val oldCandidates = queryInfo.candidates
    val processedCandidate = oldCandidates.find(query => query.index == processedIndex)
    // check if I still care about these neighbors or if the node they belong to has already been kicked out of the candidate list
    processedCandidate match {
      case Some(candidate) =>
        // update candidates
        val currentCandidateIndices = oldCandidates.map(_.index)
        // only add candidates that are not already in the candidateList
        // candidates for which we have the location can be added immediately
        val newCandidates = potentialNewCandidates.diff(currentCandidateIndices)
          .filter(candidateIndex => data.isLocal(candidateIndex))
          .map { candidateIndex =>
            val location = data.get(candidateIndex)
            QueryCandidate(candidateIndex, euclideanDist(queryInfo.query, location), processed = false)
          }
        // candidates for which we don't have the location have to ask for it first
        val remoteCandidates = potentialNewCandidates.diff(currentCandidateIndices)
          .filter(candidateIndex => !data.isLocal(candidateIndex))
        remoteCandidates.foreach(candidate => askForLocation(candidate, queryId, queryInfo, nodeLocator))

        val updatedCandidates = (oldCandidates ++: newCandidates).sortBy(_.distance).slice(0, queryInfo.neighborsWanted)
        candidate.processed = true
        queryInfo.candidates = updatedCandidates
      case None =>
      // the new candidates do not need to be used as they were generated with a now obsolete old candidate
    }
  }

  // the difference to the previous function is that processed candidates aren't discarded and that in this stage we have responseLocations
  def updatePathCandidates(queryInfo: QueryInfo,
                           queryId: Int,
                           processedIndex: Int,
                           potentialNewCandidates: Seq[Int],
                           responseLocations: QueryResponseLocations[Float],
                           nodeLocator: NodeLocator[SearchOnGraphEvent]): Unit = {
    val oldCandidates = queryInfo.candidates
    val processedCandidate = oldCandidates.find(query => query.index == processedIndex)
    // check if I still care about these neighbors or if the node they belong to has already been kicked out of the candidate list
    processedCandidate match {
      case Some(candidate) =>
        // update candidates
        val currentCandidateIndices = oldCandidates.map(_.index)
        // only add candidates that are not already in the candidateList
        // candidates for which we have the location can be added immediately
        val newCandidates = potentialNewCandidates.diff(currentCandidateIndices)
          .filter(candidateIndex => responseLocations.hasLocation(candidateIndex))
          .map { candidateIndex =>
            val location = responseLocations.location(candidateIndex)
            QueryCandidate(candidateIndex, euclideanDist(queryInfo.query, location), processed = false)
          }
        // candidates for which we don't have the location have to ask for it first
        val remoteCandidates = potentialNewCandidates.diff(currentCandidateIndices)
          .filter(candidateIndex => !responseLocations.hasLocation(candidateIndex))
        remoteCandidates.foreach(candidate => askForLocation(candidate, queryId, queryInfo, nodeLocator))

        val mergedCandidates = (oldCandidates ++: newCandidates).sortBy(_.distance)
        // update responseLocations
        newCandidates.foreach(newCandidate => responseLocations.addedToCandidateList(newCandidate.index, responseLocations.location(newCandidate.index)))
        val updatedCandidates = mergedCandidates.slice(0, queryInfo.neighborsWanted) ++
          mergedCandidates.slice(queryInfo.neighborsWanted, mergedCandidates.length).filter { mergedCandidate =>
            if (!mergedCandidate.processed) {
              // candidate can be removed as it is needed neither in the response nor in the search
              responseLocations.removedFromCandidateList(mergedCandidate.index)
            }
            mergedCandidate.processed
          }
        candidate.processed = true
        queryInfo.candidates = updatedCandidates
      case None =>
      // the new candidates do not need to be used as they were generated with a now obsolete old candidate
    }
  }

  def askForLocation(remoteIndex: Int, queryId: Int, queryInfo: QueryInfo, nodeLocator: NodeLocator[SearchOnGraphEvent]): Unit = {
    if (!waitingOnLocation.alreadyIn(remoteIndex, queryId)) {
      queryInfo.waitingOn += 1
      val askForLocation = waitingOnLocation.insert(remoteIndex, queryId)
      if (askForLocation) {
        // TODO add timer to guarantee response (resend if not gotten one)
        nodeLocator.findResponsibleActor(remoteIndex) ! GetLocation(remoteIndex, ctx.self)
      }
    }
  }

  def addCandidate(queryInfo: QueryInfo,
                   queryId: Int,
                   candidateId: Int,
                   candidateLocation: Seq[Float],
                   nodeLocator: NodeLocator[SearchOnGraphEvent]): Boolean = {
    // return if this means the query is finished
    val currentCandidates = queryInfo.candidates
    val allProcessed = !currentCandidates.exists(_.processed == false)
    val dist = euclideanDist(queryInfo.query, candidateLocation)
    val closeEnough =
      if (currentCandidates.length >= queryInfo.neighborsWanted) {
        val lastCandidateToBeat = math.min(currentCandidates.length - 1, queryInfo.neighborsWanted - 1)
        currentCandidates(lastCandidateToBeat).distance > dist
      } else { true }
    val usableCandidate = closeEnough && !currentCandidates.exists(candidate => candidate.index == candidateId)
    if (usableCandidate) {
      // update candidates
      val updatedCandidates = (currentCandidates :+ QueryCandidate(candidateId, dist, processed=false)).sortBy(candidate => candidate.distance)
      queryInfo.candidates = updatedCandidates
      // If all other candidates have already been processed, the new now needs to be processed
      if (allProcessed) {
        nodeLocator.findResponsibleActor(candidateId) ! GetNeighbors(candidateId, queryId, ctx.self)
      }
    }
    // return if the query is finished
    allProcessed && queryInfo.waitingOn == 0 && !usableCandidate
  }

  def sendResults(queryId: Int, queryInfo: QueryInfo, responseLocations: QueryResponseLocations[Float], respondTo: ActorRef[BuildNSGEvent]): Unit = {
    val checkedNodes = queryInfo.candidates.map { candidate =>
      (candidate.index, responseLocations.location(candidate.index))
    }
    respondTo ! SortedCheckedNodes(queryId, checkedNodes)
    queryInfo.candidates.foreach(candidate => responseLocations.removedFromCandidateList(candidate.index))
    //ctx.log.info("responseLocations size: {}", responseLocations.size())
  }
}
