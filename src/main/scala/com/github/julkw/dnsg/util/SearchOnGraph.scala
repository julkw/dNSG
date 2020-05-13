package com.github.julkw.dnsg.util

import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.ActorContext
import com.github.julkw.dnsg.actors.ClusterCoordinator.{CoordinationEvent, FinishedUpdatingConnectivity, KNearestNeighbors}
import com.github.julkw.dnsg.actors.SearchOnGraphActor
import com.github.julkw.dnsg.actors.SearchOnGraphActor.{DoneConnectingChildren, GetLocation, GetNeighbors, IsConnected, SearchOnGraphEvent}
import com.github.julkw.dnsg.actors.createNSG.NSGWorker.{BuildNSGEvent, SortedCheckedNodes}

import scala.collection.mutable

abstract class SearchOnGraph(supervisor: ActorRef[CoordinationEvent], ctx: ActorContext[SearchOnGraphActor.SearchOnGraphEvent]) extends Distance {
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
                       waitingOnLocations: mutable.Map[Int, Set[Int]],
                       data: LocalData[Float]): Unit = {
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
        askForLocations(remoteCandidates, queryId, queryInfo, nodeLocator, waitingOnLocations)

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
                           waitingOnLocations: mutable.Map[Int, Set[Int]]): Unit = {
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
        askForLocations(remoteCandidates, queryId, queryInfo, nodeLocator, waitingOnLocations)

        val mergedCandidates = (oldCandidates ++: newCandidates).sortBy(_.distance)
        //remove unprocessed candidates from behind neighborsWanted (they will not be needed for response)
        // do this for all so I can take all after wantedNeighbors out without worrying
        // TODO?
        newCandidates.foreach(candidate => responseLocations.addedToCandidateList(candidate.index, responseLocations.location(candidate.index)))
        val updatedCandidates = mergedCandidates.slice(0, queryInfo.neighborsWanted) ++
          mergedCandidates.slice(queryInfo.neighborsWanted, mergedCandidates.length).filter { candidate =>
            if (!candidate.processed) {
              // candidate can be removed as it is needed neither in the response nor in the search
              // TODO HOW IN THE WORLD DOES THIS REMOVE TOO MUCH? WHERE DO I FORGET TO ADD???
              responseLocations.removedFromCandidateList(candidate.index)
            }
            candidate.processed
          }
        /*
        val newMaxDist = updatedCandidates(updatedCandidates.length-1).distance
        // only candidates with local locations have been added so this should be safe
        newCandidates.filter(candidate => candidate.distance <= newMaxDist).foreach ( addedCandidate =>
          responseLocations.addedToCandidateList(addedCandidate.index, responseLocations.location(addedCandidate.index))
        )
         */
        candidate.processed = true
        queryInfo.candidates = updatedCandidates
      case None =>
      // the new candidates do not need to be used as they were generated with a now obsolete old candidate
    }
  }

  def askForLocations(remoteCandidates: Seq[Int],
                      queryId: Int,
                      queryInfo: QueryInfo,
                      nodeLocator: NodeLocator[SearchOnGraphEvent],
                      waitingOnLocation: mutable.Map[Int, Set[Int]]): Unit = {
    // update for locations we have already asked for
    waitingOnLocation ++= remoteCandidates.filter(candidate => waitingOnLocation.contains(candidate))
      .map { candidate =>
        if (!waitingOnLocation(candidate).contains(queryId)) {
          queryInfo.waitingOn += 1
        }
        candidate -> (waitingOnLocation(candidate) + queryId)
      }
    // add locations that we are just now asking for
    waitingOnLocation ++= remoteCandidates.filter(candidate => !waitingOnLocation.contains(candidate))
      .map { candidate =>
        nodeLocator.findResponsibleActor(candidate) ! GetLocation(candidate, ctx.self)
        candidate -> Set(queryId)
      }
  }

  def addCandidate(queryId: Int,
                   candidateId: Int,
                   candidateLocation: Seq[Float],
                   queries: Map[Int, QueryInfo],
                   nodeLocator: NodeLocator[SearchOnGraphEvent]): Boolean = {
    // return if the means the query is finished
    val queryInfo = queries(queryId)
    queryInfo.waitingOn -= 1
    val currentCandidates = queryInfo.candidates
    val allProcessed = !currentCandidates.exists(_.processed == false)
    val dist = euclideanDist(queries(queryId).query, candidateLocation)
    // if the starting node wasn't local, current candidates is empty
    val closeEnough =
      if (currentCandidates.nonEmpty) {
        currentCandidates(currentCandidates.length-1).distance > dist
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
    queryInfo.candidates.foreach(candidate => responseLocations.removedFromCandidateList(candidate.index))
    ctx.log.info("Left in responseLocations: {}", responseLocations.size())
    respondTo ! SortedCheckedNodes(queryId, checkedNodes)
  }
}
