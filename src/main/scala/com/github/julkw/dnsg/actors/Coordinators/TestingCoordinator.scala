package com.github.julkw.dnsg.actors.Coordinators

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.{CoordinationEvent, FinishedTesting, GetMoreQueries, KNearestNeighbors}
import com.github.julkw.dnsg.actors.DataHolder.{LoadDataEvent, ReadTestQueries}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.{FindNearestNeighborsStartingFrom, SearchOnGraphEvent}
import com.github.julkw.dnsg.util.{QueryNodeLocator, Settings}

object TestingCoordinator {
  trait TestingEvent

  final case class TestQueries(queries: Seq[(Array[Float], Seq[Int])]) extends TestingEvent

  final case class WrappedCoordinationEvent(event: CoordinationEvent) extends TestingEvent

  def apply(queryFile: String,
            queryResultFile: String,
            maxMessageSize: Int,
            navigatingNodeIndex: Int,
            nodeLocator: QueryNodeLocator[SearchOnGraphEvent],
            dataHolder: ActorRef[LoadDataEvent],
            clusterCoordinator: ActorRef[CoordinationEvent]): Behavior[TestingEvent] = Behaviors.setup { ctx =>
    val coordinationEventAdapter: ActorRef[CoordinationEvent] =
      ctx.messageAdapter { event => WrappedCoordinationEvent(event)}
    val settings = Settings(ctx.system.settings.config)
    new TestingCoordinator(settings, navigatingNodeIndex, nodeLocator, dataHolder, clusterCoordinator, coordinationEventAdapter, ctx).startTesting()
  }
}

class TestingCoordinator(settings: Settings,
                         navigatingNodeIndex: Int,
                         nodeLocator: QueryNodeLocator[SearchOnGraphEvent],
                         dataHolder: ActorRef[LoadDataEvent],
                         clusterCoordinator: ActorRef[CoordinationEvent],
                         coordinationAdapter: ActorRef[CoordinationEvent],
                         ctx: ActorContext[TestingCoordinator.TestingEvent]) {
  import TestingCoordinator._

  def startTesting(): Behavior[TestingEvent] = {
    dataHolder ! ReadTestQueries(settings.queryFilePath, settings.queryResultFilePath, ctx.self)
    waitForQueries()
  }

  def waitForQueries(): Behavior[TestingEvent] = Behaviors.receiveMessagePartial {
    case TestQueries(testQueries) =>
      val fistCQSize = testQueries.head._2.length
      val candidateQueueSizes = (0 to settings.candidateQueueIncreaseTimes).map(i => fistCQSize + i * settings.candidateQueueIncreaseBy)
      startSearch(testQueries, candidateQueueSizes)
  }

  def startSearch(testQueries: Seq[(Array[Float], Seq[Int])],
                  candidateQueueSizes: Seq[Int]): Behavior[TestingEvent] = {
    val maxCorrectNeighbors = testQueries.size * testQueries.head._2.length
    val currentQueueSize = candidateQueueSizes.head
    val queriesWithIds = testQueries.map(_._1).zipWithIndex
    val queryResults = testQueries.indices.zip(testQueries.map(_._2)).toMap

    // If no redistribution just assign queries randomly but evenly
    val toSend = if (settings.dataRedistribution == "noRedistribution") {
      val queriesPerActor = 1 + testQueries.length / nodeLocator.allActors.size
      nodeLocator.allActors.zip(queriesWithIds.grouped(queriesPerActor)).toMap
    } else {
        queriesWithIds.groupBy( query => nodeLocator.findResponsibleActor(query._1))
    }

    ctx.log.info("Testing {} queries. Perfect answer would be {} correct nearest neighbors found.", testQueries.size, maxCorrectNeighbors)
    val maxQueriesToAskFor = 1 + settings.maxMessageSize / testQueries.head._1.length
    ctx.log.info("Start testing queries with candidateQueueSize {}", currentQueueSize)

    // for debugging
    var maxQueriesPerActor = 0
    // send queries to the responsible actors
    val newToSend = toSend.transform { (actor, queries) =>
      if (queries.length > maxQueriesPerActor) {
        maxQueriesPerActor = queries.length
      }
      val queriesToAskForNow = queries.slice(0, maxQueriesToAskFor)
      val queriesToAskForLater = queries.slice(maxQueriesToAskFor, queries.length)
      actor ! FindNearestNeighborsStartingFrom(queriesToAskForNow, navigatingNodeIndex, currentQueueSize, coordinationAdapter, queriesToAskForLater.nonEmpty)
      queriesToAskForLater
    }
    ctx.log.info("Max Queries per actor: {}", maxQueriesPerActor)
    testNSG(testQueries,
      queryResults,
      newToSend,
      maxQueriesToAskFor,
      sumOfExactNeighborFound = 0,
      sumOfNeighborsFound = 0,
      maxCorrectNeighbors,
      candidateQueueSizes)
  }

  def testNSG(queries: Seq[(Array[Float], Seq[Int])],
              queryResults: Map[Int, Seq[Int]],
              toSend: Map[ActorRef[SearchOnGraphEvent], Seq[(Array[Float], Int)]],
              maxQueriesToAskFor: Int,
              sumOfExactNeighborFound: Int,
              sumOfNeighborsFound: Int,
              sumOfNearestNeighbors: Int,
              candidateQueueSizes: Seq[Int]): Behavior[TestingEvent] =
    Behaviors.receiveMessagePartial{
      case WrappedCoordinationEvent(event) =>
        event match {
          case GetMoreQueries(sender) =>
            val toSendTo = toSend(sender)
            val toSendNow = toSendTo.slice(0, maxQueriesToAskFor)
            val toSendLater = toSendTo.slice(maxQueriesToAskFor, toSendTo.size)
            sender !  FindNearestNeighborsStartingFrom(toSendNow, navigatingNodeIndex, candidateQueueSizes.head, coordinationAdapter, toSendLater.nonEmpty)
            val updatedToSend = toSend + (sender -> toSendLater)
            testNSG(queries, queryResults, updatedToSend, maxQueriesToAskFor, sumOfExactNeighborFound, sumOfNeighborsFound, sumOfNearestNeighbors, candidateQueueSizes)

          case KNearestNeighbors(queryId, neighbors) =>
            val correctNeighborIndices = queryResults(queryId)
            val newSum = sumOfNeighborsFound + correctNeighborIndices.intersect(neighbors).length
            val firstNearestNeighborFound = if (neighbors.head == correctNeighborIndices.head) { 1 } else { 0 }
            //ctx.log.info("Still waiting on {} queries: {}", queries.size - 1)
            if (queryResults.size == 1) {
              ctx.log.info("Overall 1st nearest neighbors found: {}", sumOfExactNeighborFound + firstNearestNeighborFound)
              ctx.log.info("Overall correct neighbors found: {}", newSum)
              ctx.log.info("Precision: {}", newSum.toFloat / sumOfNearestNeighbors.toFloat)
              val nextCandidateQueueSizes = candidateQueueSizes.slice(1, candidateQueueSizes.length)
              if (nextCandidateQueueSizes.isEmpty) {
                clusterCoordinator ! FinishedTesting
                Behaviors.stopped
              } else {
                startSearch(queries, nextCandidateQueueSizes)
              }
            } else {
              testNSG(queries, queryResults - queryId, toSend, maxQueriesToAskFor, sumOfExactNeighborFound + firstNearestNeighborFound, newSum, sumOfNearestNeighbors, candidateQueueSizes)
            }
        }
    }
}
