package com.github.julkw.dnsg.actors.SearchOnGraph

import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.ActorContext
import com.github.julkw.dnsg.actors.SearchOnGraph.SOGInfo.{GetLocation, GetNeighbors}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.{SearchOnGraphEvent, SearchOnGraphInfo}
import com.github.julkw.dnsg.actors.createNSG.NSGWorker.{BuildNSGEvent, SortedCheckedNodes}
import com.github.julkw.dnsg.util.Data.CacheData
import com.github.julkw.dnsg.util.{Distance, NodeLocator, WaitingOnLocation}


abstract class SearchOnGraph(waitingOnLocation: WaitingOnLocation,
                             maxMessageSize: Int,
                             ctx: ActorContext[SearchOnGraphActor.SearchOnGraphEvent]) extends Distance {
  // data type for more readable code
  protected case class QueryCandidate(index: Int, distance: Double, var processed: Boolean)
  // also refactor to remove vars?
  protected case class QueryInfo(query: Seq[Float], neighborsWanted: Int, var candidates: Seq[QueryCandidate], var waitingOn: Int)

  def sendMessagesImmediately(toSend: Map[ActorRef[SearchOnGraphEvent], SOGInfo]): Unit = {
    toSend.foreach { case (graphHolder, sogInfo) =>
      if (sogInfo.sendImmediately && sogInfo.nonEmpty) {
        val messageToSend = sogInfo.sendMessage(maxMessageSize)
        graphHolder ! SearchOnGraphInfo(messageToSend, ctx.self)
        sogInfo.sendImmediately = false
      }
    }
  }

  def updateCandidates(queryInfo: QueryInfo,
                       queryId: Int,
                       processedIndex: Int,
                       potentialNewCandidates: Seq[Int],
                       nodeLocator: NodeLocator[SearchOnGraphEvent],
                       data: CacheData[Float],
                       toSend: Map[ActorRef[SearchOnGraphEvent], SOGInfo]): Unit = {
    val oldCandidates = queryInfo.candidates
    val processedCandidate = oldCandidates.find(query => query.index == processedIndex)
    // check if I still care about these neighbors or if the node they belong to has already been kicked out of the candidate list
    processedCandidate match {
      case Some(candidate) =>
        // update candidates
        val currentCandidateIndices = oldCandidates.map(_.index)
        // only add candidates that are not already in the candidateList
        // candidates for which we have the location can be added immediately
        val (localCandidates, remoteCandidates) = potentialNewCandidates.diff(currentCandidateIndices).partition(potentialCandidate => data.isLocal(potentialCandidate))
        val newCandidates = localCandidates.map { candidateIndex =>
            val location = data.get(candidateIndex)
            QueryCandidate(candidateIndex, euclideanDist(queryInfo.query, location), processed = false)
          }
        // candidates for which we don't have the location have to ask for it first
        remoteCandidates.foreach(remoteNeighbor => askForLocation(remoteNeighbor, queryId, queryInfo, nodeLocator, toSend))

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
                           nodeLocator: NodeLocator[SearchOnGraphEvent],
                           toSend: Map[ActorRef[SearchOnGraphEvent], SOGInfo]): Unit = {
    val oldCandidates = queryInfo.candidates
    val processedCandidate = oldCandidates.find(query => query.index == processedIndex)
    // check if I still care about these neighbors or if the node they belong to has already been kicked out of the candidate list
    processedCandidate match {
      case Some(candidate) =>
        // update candidates
        val currentCandidateIndices = oldCandidates.map(_.index)
        // only add candidates that are not already in the candidateList
        // candidates for which we have the location can be added immediately
        val (localCandidates, remoteCandidates) = potentialNewCandidates.diff(currentCandidateIndices).partition(candidateIndex =>
          responseLocations.hasLocation(candidateIndex)
        )
        val newCandidates = localCandidates.map { candidateIndex =>
            val location = responseLocations.location(candidateIndex)
            responseLocations.addedToCandidateList(candidateIndex, location)
            QueryCandidate(candidateIndex, euclideanDist(queryInfo.query, location), processed = false)
          }
        // candidates for which we don't have the location have to ask for it first
        remoteCandidates.foreach(remoteCandidate => askForLocation(remoteCandidate, queryId, queryInfo, nodeLocator, toSend))

        val mergedCandidates = (oldCandidates ++: newCandidates).sortBy(_.distance)
        // update responseLocations
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

  def aksForNeighbors(node: Int,
                      queryId: Int,
                      graph: Map[Int, Seq[Int]],
                      nodeLocator: NodeLocator[SearchOnGraphEvent],
                      toSend: Map[ActorRef[SearchOnGraphEvent], SOGInfo]): Unit = {
    // in case of data replication the actor should check if it has the required information itself before asking the primary assignee
    if (graph.contains(node)) {
      toSend(ctx.self).addMessage(GetNeighbors(node, queryId))
    } else {
      toSend(nodeLocator.findResponsibleActor(node)).addMessage(GetNeighbors(node, queryId))
    }
  }

  def askForLocation(remoteIndex: Int,
                     queryId: Int,
                     queryInfo: QueryInfo,
                     nodeLocator: NodeLocator[SearchOnGraphEvent],
                     toSend: Map[ActorRef[SearchOnGraphEvent], SOGInfo]): Unit = {
    if (!waitingOnLocation.alreadyIn(remoteIndex, queryId)) {
      queryInfo.waitingOn += 1
      val askForLocation = waitingOnLocation.insert(remoteIndex, queryId)
      if (askForLocation) {
        toSend(nodeLocator.findResponsibleActor(remoteIndex)).addMessage(GetLocation(remoteIndex))
      }
    }
  }

  def addCandidate(queryInfo: QueryInfo,
                   queryId: Int,
                   candidateId: Int,
                   candidateLocation: Seq[Float],
                   graph: Map[Int, Seq[Int]],
                   nodeLocator: NodeLocator[SearchOnGraphEvent],
                   toSend: Map[ActorRef[SearchOnGraphEvent], SOGInfo]): Boolean = {
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
        aksForNeighbors(candidateId, queryId, graph, nodeLocator, toSend)
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
  }
}
