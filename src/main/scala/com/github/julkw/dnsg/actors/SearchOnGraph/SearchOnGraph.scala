package com.github.julkw.dnsg.actors.SearchOnGraph

import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.ActorContext
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.{CoordinationEvent, KNearestNeighbors, KNearestNeighborsWithDist}
import com.github.julkw.dnsg.actors.SearchOnGraph.SOGInfo.{GetLocation, GetNeighbors}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.{SearchOnGraphEvent, SearchOnGraphInfo}
import com.github.julkw.dnsg.actors.createNSG.NSGWorker.{BuildNSGEvent, SortedCheckedNodes}
import com.github.julkw.dnsg.util.Data.LocalData
import com.github.julkw.dnsg.util.{Distance, NodeLocator, WaitingOnLocation}


abstract class SearchOnGraph(waitingOnLocation: WaitingOnLocation[Int],
                             waitingOnNeighbors: WaitingOnLocation[Int],
                             maxMessageSize: Int,
                             maxNSGCandidates: Int,
                             ctx: ActorContext[SearchOnGraphActor.SearchOnGraphEvent]) extends Distance {
  // data type for more readable code
  protected case class QueryCandidate(index: Int, distance: Double, var processed: Boolean, var currentlyProcessing: Boolean)
  // also refactor to remove vars?
  protected case class QueryInfo(query: Array[Float], neighborsWanted: Int, var candidates: Seq[QueryCandidate], var waitingOn: Int, sendWithDist: Boolean = false)

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
                       data: LocalData[Float],
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
            QueryCandidate(candidateIndex, euclideanDist(queryInfo.query, location), processed = false, currentlyProcessing = false)
          }
        // candidates for which we don't have the location have to ask for it first
        remoteCandidates.foreach(remoteNeighbor => queryInfo.waitingOn += askForLocation(remoteNeighbor, queryId, nodeLocator, toSend))
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
            QueryCandidate(candidateIndex, euclideanDist(queryInfo.query, location), processed = false, currentlyProcessing = false)
          }
        // candidates for which we don't have the location have to ask for it first
        remoteCandidates.foreach(remoteCandidate => queryInfo.waitingOn += askForLocation(remoteCandidate, queryId, nodeLocator, toSend))

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

  def askForNeighbors(node: Int,
                      queryId: Int,
                      graph: Map[Int, Seq[Int]],
                      nodeLocator: NodeLocator[SearchOnGraphEvent],
                      toSend: Map[ActorRef[SearchOnGraphEvent], SOGInfo]): Unit = {
    // in case of data replication the actor should check if it has the required information itself before asking the primary assignee
    val haveToAsk = waitingOnNeighbors.insert(node, queryId)
    if (haveToAsk) {
      if (graph.contains(node)) {
        toSend(ctx.self).addMessage(GetNeighbors(node))
      } else {
        toSend(nodeLocator.findResponsibleActor(node)).addMessage(GetNeighbors(node))
      }
    }
  }

  def askForLocation(remoteIndex: Int,
                     queryId: Int,
                     nodeLocator: NodeLocator[SearchOnGraphEvent],
                     toSend: Map[ActorRef[SearchOnGraphEvent], SOGInfo]): Int = {
    // if this query has already asked for this location, its waitingOn does not need to be increased
    if (!waitingOnLocation.alreadyIn(remoteIndex, queryId)) {
      val askForLocation = waitingOnLocation.insert(remoteIndex, queryId)
      if (askForLocation) {
        toSend(nodeLocator.findResponsibleActor(remoteIndex)).addMessage(GetLocation(remoteIndex))
      }
      1
    } else {
      0
    }
  }

  def addCandidate(queryInfo: QueryInfo,
                   candidateId: Int,
                   candidateLocation: Array[Float]): Boolean = {
    // return if this means the query is finished
    val currentCandidates = queryInfo.candidates
    val dist = euclideanDist(queryInfo.query, candidateLocation)
    val closeEnough =
      if (currentCandidates.length >= queryInfo.neighborsWanted) {
        val lastCandidateToBeat = math.min(currentCandidates.length - 1, queryInfo.neighborsWanted - 1)
        currentCandidates(lastCandidateToBeat).distance > dist
      } else { true }
    val usableCandidate = closeEnough && !currentCandidates.exists(candidate => candidate.index == candidateId)
    if (usableCandidate) {
      // update candidates
      // TODO instead of sorting find correct place to insert into the list
      val updatedCandidates = (currentCandidates :+ QueryCandidate(candidateId, dist, processed=false, currentlyProcessing = false)).sortBy(candidate => candidate.distance)
      queryInfo.candidates = updatedCandidates
    }
    // return if the query is finished
    queryInfo.candidates.forall(_.processed == true) && queryInfo.waitingOn == 0
  }

  def sendPathResults(queryId: Int, queryInfo: QueryInfo, responseLocations: QueryResponseLocations[Float], respondTo: ActorRef[BuildNSGEvent]): Unit = {
    val checkedNodes = queryInfo.candidates.map { candidate =>
      (candidate.index, responseLocations.location(candidate.index))
    }
    respondTo ! SortedCheckedNodes(queryId, checkedNodes.slice(0, maxNSGCandidates))
    queryInfo.candidates.foreach(candidate => responseLocations.removedFromCandidateList(candidate.index))
  }

  def sendKNNResults(queryInfo: QueryInfo, sendResults: (ActorRef[CoordinationEvent], Int)): Unit = {
    if (queryInfo.sendWithDist) {
      val finalNeighbors = queryInfo.candidates.map(qi => (qi.index, qi.distance))
      sendResults._1 ! KNearestNeighborsWithDist(sendResults._2, finalNeighbors)
    } else {
      val finalNeighbors = queryInfo.candidates.map(_.index)
      sendResults._1 ! KNearestNeighbors(sendResults._2, finalNeighbors)
    }
  }

  def randomNodes(nodesNeeded: Int, graphSize: Int): Set[Int] = {
    val r = scala.util.Random
    var nodes: Set[Int] = Set.empty
    while (nodes.size < nodesNeeded) {
      nodes += r.nextInt(graphSize)
    }
    nodes
  }
}
