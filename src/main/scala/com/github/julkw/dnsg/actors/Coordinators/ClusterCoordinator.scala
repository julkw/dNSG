package com.github.julkw.dnsg.actors.Coordinators

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.Coordinators.GraphRedistributionCoordinator.{AllSharedReplication, NoReplication, OnlyParentsReplication}
import com.github.julkw.dnsg.actors.Coordinators.NodeCoordinator.{AllDone, NodeCoordinationEvent, StartDistributingData, StartSearchOnGraph}
import com.github.julkw.dnsg.actors.DataHolder.{GetAverageValue, LoadDataEvent, ReadTestQueries, SaveGraphToFile}
import com.github.julkw.dnsg.actors.NodeLocatorHolder.{AllNodeLocatorHolders, NodeLocationEvent, SendNodeLocatorToNodeCoordinator, ShareNodeLocator}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.{FindNearestNeighbors, FindNearestNeighborsStartingFrom, SearchOnGraphEvent}
import com.github.julkw.dnsg.actors.createNSG.NSGMerger.MergeNSGEvent
import com.github.julkw.dnsg.actors.nndescent.KnngWorker._
import com.github.julkw.dnsg.util._

object ClusterCoordinator {

  sealed trait CoordinationEvent extends dNSGSerializable

  // setup
  final case class NodeIntroduction(nodeCoordinator: ActorRef[NodeCoordinationEvent],
                                    nodeDataHolder: ActorRef[LoadDataEvent],
                                    nodeLocatorHolder: ActorRef[NodeLocationEvent]) extends CoordinationEvent

  final case class DataSize(dataSize: Int, filename: String, dataHolder: ActorRef[LoadDataEvent]) extends CoordinationEvent

  final case object FinishedSearchOnGraphNodeLocator extends CoordinationEvent

  final case class SearchOnGraphNodeLocator(nodeLocator: NodeLocator[SearchOnGraphEvent]) extends CoordinationEvent

  final case object FinishedKnngNodeLocator extends CoordinationEvent

  final case class FinishedApproximateGraph(sender: ActorRef[BuildKNNGEvent]) extends CoordinationEvent

  final case class FinishedNNDescent(worker: ActorRef[BuildKNNGEvent]) extends CoordinationEvent

  final case class CorrectFinishedNNDescent(worker: ActorRef[BuildKNNGEvent]) extends CoordinationEvent

  final case class ConfirmFinishedNNDescent(worker: ActorRef[BuildKNNGEvent]) extends CoordinationEvent

  final case object UpdatedGraph extends CoordinationEvent

  // connecting the graph
  final case object ConnectionAchieved extends CoordinationEvent

  final case object ConnectorsCleanedUp extends CoordinationEvent

  // building the NSG
  final case class AverageValue(average: Seq[Float]) extends CoordinationEvent

  final case class GetMoreQueries(sender: ActorRef[SearchOnGraphEvent]) extends CoordinationEvent

  final case class KNearestNeighbors(query: Seq[Float], neighbors: Seq[Int]) extends CoordinationEvent

  final case class KNearestNeighborsWithDist(query: Seq[Float], neighbors: Seq[(Int, Double)]) extends CoordinationEvent

  final case class InitialNSGDone(nsgMergers: ActorRef[MergeNSGEvent]) extends CoordinationEvent

  final case class NSGonSOG(responsibilityMidPoint: Seq[Float], sender: ActorRef[SearchOnGraphEvent]) extends CoordinationEvent

  // writing the graph to file
  final case object GraphWrittenToFile extends CoordinationEvent

  // testing the graph
  final case class TestQueries(queries: Seq[(Seq[Float], Seq[Int])]) extends CoordinationEvent

  def apply(): Behavior[CoordinationEvent] = Behaviors.setup { ctx =>

    val settings = Settings(ctx.system.settings.config)
    settings.printSettings(ctx)

    Behaviors.setup(
      ctx => new ClusterCoordinator(ctx, settings).setUp(Set.empty, Set.empty, Set.empty)
    )
  }
}

class ClusterCoordinator(ctx: ActorContext[ClusterCoordinator.CoordinationEvent],
                         settings: Settings) extends Distance with LocalityCheck {
  import ClusterCoordinator._

  def setUp(nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
            dataHolders: Set[ActorRef[LoadDataEvent]],
            nodeLocatorHolders: Set[ActorRef[NodeLocationEvent]]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case NodeIntroduction(nodeCoordinator, nodeDataHolder, nodeLocatorHolder) =>
        val updatedNodeCoordinators = nodeCoordinators + nodeCoordinator
        val updatedDataHolders = dataHolders + nodeDataHolder
        if (settings.nodesExpected == updatedNodeCoordinators.size) {
          updatedNodeCoordinators.foreach(nc => nc ! StartDistributingData(updatedDataHolders))
          ctx.log.info("All expected nodes found, start distributing data.")
        }
        setUp(updatedNodeCoordinators, updatedDataHolders, nodeLocatorHolders + nodeLocatorHolder)

      case DataSize(dataSize, filename, dataHolder) =>
        ctx.log.info("Read {} vectors from {}", dataSize, filename)
        nodeLocatorHolders.foreach(nodeLocatorHolder => nodeLocatorHolder ! AllNodeLocatorHolders(nodeLocatorHolders, dataSize))
        waitOnSearchInitialDistribution(nodeCoordinators, dataHolder, nodeLocatorHolders, nodeLocatorHolders.size)
  }

  def waitOnSearchInitialDistribution(nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                                      dataHolder: ActorRef[LoadDataEvent],
                                      nodeLocatorHolders: Set[ActorRef[NodeLocationEvent]],
                                      waitingOnNodeLocators: Int): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case FinishedSearchOnGraphNodeLocator =>
        if (waitingOnNodeLocators == 1) {
          ctx.log.info("Nodes now distributed to SearchOnGraph Actors for initial random graph")
          nodeLocatorHolders.foreach(nodeLocatorHolder => nodeLocatorHolder ! ShareNodeLocator(isLocal(nodeLocatorHolder)))
        }
        waitOnSearchInitialDistribution(nodeCoordinators, dataHolder, nodeLocatorHolders, waitingOnNodeLocators - 1)

      case SearchOnGraphNodeLocator(nodeLocator) =>
        waitForKnngDataDistribution(nodeLocator, nodeCoordinators, dataHolder, nodeLocatorHolders, nodeLocatorHolders.size)

      case FinishedKnngNodeLocator =>
        ctx.self ! FinishedKnngNodeLocator
        waitOnSearchInitialDistribution(nodeCoordinators, dataHolder, nodeLocatorHolders, waitingOnNodeLocators)
    }

  def waitForKnngDataDistribution(nodeLocator: NodeLocator[SearchOnGraphEvent],
                                  nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                                  dataHolder: ActorRef[LoadDataEvent],
                                  nodeLocatorHolders: Set[ActorRef[NodeLocationEvent]],
                                  waitingOnNodeLocators: Int): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case FinishedKnngNodeLocator =>
        if (waitingOnNodeLocators == 1) {
          nodeLocatorHolders.foreach { nodeLocatorHolder =>
            nodeLocatorHolder ! ShareNodeLocator(isLocal(nodeLocatorHolder))
          }
          buildApproximateKnng(nodeLocator, Set.empty, nodeCoordinators, dataHolder, nodeLocatorHolders)
        } else {
          waitForKnngDataDistribution(nodeLocator, nodeCoordinators, dataHolder, nodeLocatorHolders, waitingOnNodeLocators - 1)
        }

      case FinishedApproximateGraph(worker) =>
        ctx.self ! FinishedApproximateGraph(worker)
        waitForKnngDataDistribution(nodeLocator, nodeCoordinators, dataHolder, nodeLocatorHolders, waitingOnNodeLocators)
    }

  def buildApproximateKnng(nodeLocator: NodeLocator[SearchOnGraphEvent],
                           knngWorkers: Set[ActorRef[BuildKNNGEvent]],
                           nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                           dataHolder: ActorRef[LoadDataEvent],
                           nodeLocatorHolders: Set[ActorRef[NodeLocationEvent]]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case FinishedApproximateGraph(sender) =>
        val updatedWorkers = knngWorkers + sender
        if (updatedWorkers.size == nodeLocator.allActors.size) {
          ctx.log.info("Approximate graph has been build. Start NNDescent")
          updatedWorkers.foreach(worker => worker ! StartNNDescent)
          waitForNnDescent(nodeLocator, Set.empty, nodeCoordinators, dataHolder, nodeLocatorHolders)
        } else {
          buildApproximateKnng(nodeLocator, updatedWorkers, nodeCoordinators, dataHolder, nodeLocatorHolders)
        }
    }

  def waitForNnDescent(nodeLocator: NodeLocator[SearchOnGraphEvent],
                       doneWorkers: Set[ActorRef[BuildKNNGEvent]],
                       nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                       dataHolder: ActorRef[LoadDataEvent],
                       nodeLocatorHolders: Set[ActorRef[NodeLocationEvent]]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case FinishedNNDescent(worker) =>
        val updatedWorkers = doneWorkers + worker
        if (updatedWorkers.size == nodeLocator.allActors.size) {
          updatedWorkers.foreach(knngWorker => knngWorker ! GetNNDescentFinishedConfirmation)
          confirmNNDescent(nodeLocator, updatedWorkers, Set.empty, nodeCoordinators, dataHolder, nodeLocatorHolders)
        } else {
          waitForNnDescent(nodeLocator, updatedWorkers, nodeCoordinators, dataHolder, nodeLocatorHolders)
        }

      case CorrectFinishedNNDescent(worker) =>
        waitForNnDescent(nodeLocator, doneWorkers - worker, nodeCoordinators, dataHolder, nodeLocatorHolders)

      case ConfirmFinishedNNDescent(worker) =>
        // I'll ask again once all workers are done so this message can be ignored
        waitForNnDescent(nodeLocator, doneWorkers, nodeCoordinators, dataHolder, nodeLocatorHolders)
    }

  def confirmNNDescent(nodeLocator: NodeLocator[SearchOnGraphEvent],
                       knngWorkers: Set[ActorRef[BuildKNNGEvent]],
                       confirmedWorkers: Set[ActorRef[BuildKNNGEvent]],
                       nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                       dataHolder: ActorRef[LoadDataEvent],
                       nodeLocatorHolders: Set[ActorRef[NodeLocationEvent]]): Behavior[CoordinationEvent] = Behaviors.receiveMessagePartial {
    case ConfirmFinishedNNDescent(worker) =>
      val updatedConfirmedWorkers = confirmedWorkers + worker
      if (updatedConfirmedWorkers.size == knngWorkers.size) {
        ctx.log.info("Done with NNDescent")
        //nodeCoordinators.foreach(nodeCoordinator => nodeCoordinator ! StartSearchOnGraph)
        knngWorkers.foreach( knngWorker => knngWorker ! MoveGraph)
        waitOnSearchOnGraphUpdated(nodeLocator, knngWorkers, nodeCoordinators, dataHolder, nodeLocatorHolders, knngWorkers.size)
      } else {
        confirmNNDescent(nodeLocator, knngWorkers, updatedConfirmedWorkers, nodeCoordinators, dataHolder, nodeLocatorHolders)
      }

    case FinishedNNDescent(worker) =>
      // ignore
      confirmNNDescent(nodeLocator, knngWorkers, confirmedWorkers, nodeCoordinators, dataHolder, nodeLocatorHolders)

    case CorrectFinishedNNDescent(worker) =>
      ctx.log.info("Actor not done during confirmation")
      // apparently not all are done, go back to waiting again
      waitForNnDescent(nodeLocator, knngWorkers - worker, nodeCoordinators, dataHolder, nodeLocatorHolders)
  }

  def waitOnSearchOnGraphUpdated(nodeLocator: NodeLocator[SearchOnGraphEvent],
                                 oldWorkers: Set[ActorRef[BuildKNNGEvent]],
                                 nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                                 dataHolder: ActorRef[LoadDataEvent],
                                 nodeLocatorHolders: Set[ActorRef[NodeLocationEvent]],
                                 waitingOnGraphUpdates: Int): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case UpdatedGraph =>
        if (waitingOnGraphUpdates == 1) {
          ctx.log.info("All SearchOnGraph actors now updated to new AKNNG")
          oldWorkers.foreach(worker => worker ! AllKnngWorkersDone)
          val aknngFilename = settings.aknngFilePath
          if (aknngFilename.nonEmpty) {
            dataHolder ! SaveGraphToFile(aknngFilename, nodeLocator.allActors, nodeLocator.graphSize, None, ctx.self)
            waitForAKNNGToBeWrittenToFile(nodeLocator, nodeCoordinators, dataHolder, nodeLocatorHolders)
          } else {
            dataHolder ! GetAverageValue(ctx.self)
            findNavigatingNode(nodeLocator, nodeCoordinators, dataHolder, nodeLocatorHolders)
          }
        } else {
          waitOnSearchOnGraphUpdated(nodeLocator, oldWorkers, nodeCoordinators, dataHolder, nodeLocatorHolders, waitingOnGraphUpdates - 1)
        }
    }

  def waitForAKNNGToBeWrittenToFile(nodeLocator: NodeLocator[SearchOnGraphEvent],
                                    nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                                    dataHolder: ActorRef[LoadDataEvent],
                                    nodeLocatorHolders: Set[ActorRef[NodeLocationEvent]]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case GraphWrittenToFile =>
        ctx.log.info("AKNNG written to file for potential later testing")
        dataHolder ! GetAverageValue(ctx.self)
        findNavigatingNode(nodeLocator, nodeCoordinators, dataHolder, nodeLocatorHolders)
    }

  def findNavigatingNode(nodeLocator: NodeLocator[SearchOnGraphEvent],
                         nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                         dataHolder: ActorRef[LoadDataEvent],
                         nodeLocatorHolders: Set[ActorRef[NodeLocationEvent]]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case AverageValue(value) =>
        ctx.log.info("Received average value, now looking for Navigating Node")
        // find navigating Node, start from random
        nodeLocator.allActors.head ! FindNearestNeighbors(Seq(value), settings.candidateQueueSizeKnng, ctx.self, false, false)
        findNavigatingNode(nodeLocator, nodeCoordinators, dataHolder, nodeLocatorHolders)

      case KNearestNeighbors(query, neighbors) =>
        // Right now the only query being asked for is the NavigationNode, so that has been found
        val navigatingNode = neighbors.head
        ctx.log.info("The navigating node has the index: {}", navigatingNode)
        if (settings.dataRedistribution != "noRedistribution") {
          val replicationModel =  settings.dataReplication match {
            case "onlyParentsReplication" =>
              OnlyParentsReplication
            case "allSharedReplication" =>
              AllSharedReplication
            case "noReplication" =>
              NoReplication
            case _ =>
              NoReplication
          }
          ctx.spawn(GraphRedistributionCoordinator(neighbors.head, replicationModel, nodeLocator, nodeLocatorHolders, settings.maxMessageSize), name="GraphRedistributionCoordinator")
          waitOnRedistribution(neighbors.head, nodeCoordinators, dataHolder, nodeLocatorHolders)
        } else {
          startNSG(navigatingNode, nodeLocator, nodeCoordinators, dataHolder, nodeLocatorHolders)
        }
    }

  def waitOnRedistribution(navigatingNode: Int,
                           nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                           dataHolder: ActorRef[LoadDataEvent],
                           nodeLocatorHolders: Set[ActorRef[NodeLocationEvent]]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case SearchOnGraphNodeLocator(nodeLocator) =>
        startNSG(navigatingNode, nodeLocator, nodeCoordinators, dataHolder, nodeLocatorHolders)
    }

  def startNSG(navigatingNode: Int,
               nodeLocator: NodeLocator[SearchOnGraphEvent],
               nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
               dataHolder: ActorRef[LoadDataEvent],
               nodeLocatorHolders: Set[ActorRef[NodeLocationEvent]]): Behavior[CoordinationEvent] = {
    ctx.log.info("Start with NSG")
    nodeLocatorHolders.foreach(nodeLocatorHolder => nodeLocatorHolder ! SendNodeLocatorToNodeCoordinator(navigatingNode))
    waitOnNSG(Set.empty, navigatingNode, nodeLocator, nodeCoordinators, dataHolder, nodeLocatorHolders)
  }

  def waitOnNSG(finishedNsgMergers: Set[ActorRef[MergeNSGEvent]],
                navigatingNodeIndex: Int,
                nodeLocator: NodeLocator[SearchOnGraphEvent],
                nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                dataHolder: ActorRef[LoadDataEvent],
                nodeLocatorHolders: Set[ActorRef[NodeLocationEvent]]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial{
      case InitialNSGDone(nsgMerger) =>
        val updatedMergers = finishedNsgMergers + nsgMerger
        if (updatedMergers.size == settings.nodesExpected) {
          ctx.log.info("Initial NSG seems to be done")
          nodeCoordinators.foreach(nodeCoordinator => nodeCoordinator ! StartSearchOnGraph)
          moveNSGToSog(navigatingNodeIndex, Seq.empty, nodeLocator, nodeCoordinators, dataHolder, nodeLocatorHolders)
        } else {
          waitOnNSG(updatedMergers, navigatingNodeIndex, nodeLocator, nodeCoordinators, dataHolder, nodeLocatorHolders)
        }
    }

  def moveNSGToSog(navigatingNodeIndex: Int,
                   actorMidPoints: Seq[(ActorRef[SearchOnGraphEvent], Seq[Float])],
                   nodeLocator: NodeLocator[SearchOnGraphEvent],
                   nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                   dataHolder: ActorRef[LoadDataEvent],
                   nodeLocatorHolders: Set[ActorRef[NodeLocationEvent]]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case NSGonSOG(responsibilityMidPoint, sender) =>
        val updatedActorMidPoints = actorMidPoints :+ ((sender, responsibilityMidPoint))
        if (updatedActorMidPoints.length == nodeLocator.allActors.size) {
          ctx.log.info("NSG moved to SearchOnGraphActors. Now connecting graph")
          ctx.spawn(GraphConnectorCoordinator(navigatingNodeIndex, nodeLocator, nodeLocatorHolders, ctx.self), name="GraphConnectorCoordinator")
          val queryNodeLocator = QueryNodeLocator(updatedActorMidPoints, nodeLocator.graphSize)
          waitForConnectedGraphs(navigatingNodeIndex, queryNodeLocator, nodeCoordinators, dataHolder)
        } else {
          moveNSGToSog(navigatingNodeIndex, updatedActorMidPoints, nodeLocator, nodeCoordinators, dataHolder, nodeLocatorHolders)
        }
    }

    def waitForConnectedGraphs(navigatingNodeIndex: Int,
                               nodeLocator: QueryNodeLocator[SearchOnGraphEvent],
                               nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                               dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
      Behaviors.receiveMessagePartial {
        case ConnectionAchieved =>
          ctx.log.info("All Search on Graph actors updated to connected graph")
          val nsgFilename = settings.nsgFilePath
          if (nsgFilename.nonEmpty) {
            dataHolder ! SaveGraphToFile(nsgFilename, nodeLocator.allActors, nodeLocator.graphSize, Some(navigatingNodeIndex), ctx.self)
            waitForNSGToBeWrittenToFile(navigatingNodeIndex, nodeLocator, nodeCoordinators, dataHolder)
          } else {
            startTesting(navigatingNodeIndex, nodeLocator, nodeCoordinators, dataHolder)
          }
      }

    def waitForNSGToBeWrittenToFile(navigatingNodeIndex: Int,
                                    nodeLocator: QueryNodeLocator[SearchOnGraphEvent],
                                    nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                                    dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
      Behaviors.receiveMessagePartial {
        case GraphWrittenToFile =>
          ctx.log.info("NSG written to file for potential later testing")
          startTesting(navigatingNodeIndex, nodeLocator, nodeCoordinators, dataHolder)
      }

  // TODO move to a TestCoordinator
    def startTesting(navigatingNodeIndex: Int,
                     nodeLocator: QueryNodeLocator[SearchOnGraphEvent],
                     nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                     dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] = {
      if (settings.queryFilePath.nonEmpty && settings.queryResultFilePath.nonEmpty) {
        dataHolder ! ReadTestQueries(settings.queryFilePath, settings.queryResultFilePath, ctx.self)
        waitForQueries(navigatingNodeIndex, nodeLocator, nodeCoordinators)
      } else {
        shutdown(nodeCoordinators)
      }
    }

    def waitForQueries(navigatingNodeIndex: Int,
                       nodeLocator: QueryNodeLocator[SearchOnGraphEvent],
                       nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]]): Behavior[CoordinationEvent] = Behaviors.receiveMessagePartial {
      case TestQueries(testQueries) =>
        val neighborsExpectedPerQuery = testQueries.head._2.length
        ctx.log.info("Testing {} queries. Perfect answer would be {} correct nearest neighbors found.", testQueries.size, testQueries.size * neighborsExpectedPerQuery)
        val maxQueriesToAskFor = settings.maxMessageSize / testQueries.head._1.length
        val newToSend = testQueries.map(_._1).groupBy( query => nodeLocator.findResponsibleActor(query)).transform { (actor, queries) =>
          val queriesToAskForNow = queries.slice(0, maxQueriesToAskFor)
          val queriesToAskForLater = queries.slice(maxQueriesToAskFor, queries.length)
          // TODO do as parameter for candidate queue size instead? Was that how they effected precision?
          actor ! FindNearestNeighborsStartingFrom(queriesToAskForNow, navigatingNodeIndex, neighborsExpectedPerQuery, ctx.self, queriesToAskForLater.nonEmpty)
          queriesToAskForLater
        }
        testNSG(navigatingNodeIndex,
          nodeLocator,
          nodeCoordinators,
          testQueries.toMap,
          newToSend,
          maxQueriesToAskFor,
          sumOfExactNeighborFound = 0,
          sumOfNeighborsFound = 0,
          testQueries.size * neighborsExpectedPerQuery)
    }

    def testNSG(navigatingNodeIndex: Int,
                nodeLocator: QueryNodeLocator[SearchOnGraphEvent],
                nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                queries: Map[Seq[Float], Seq[Int]],
                toSend: Map[ActorRef[SearchOnGraphEvent], Seq[Seq[Float]]],
                maxQueriesToAskFor: Int,
                sumOfExactNeighborFound: Int,
                sumOfNeighborsFound: Int,
                sumOfNearestNeighbors: Int): Behavior[CoordinationEvent] =
      Behaviors.receiveMessagePartial{
        case GetMoreQueries(sender) =>
          val toSendTo = toSend(sender)
          val toSendNow = toSendTo.slice(0, maxQueriesToAskFor)
          val toSendLater = toSendTo.slice(maxQueriesToAskFor, toSend.size)
          sender !  FindNearestNeighborsStartingFrom(toSendNow, navigatingNodeIndex, settings.candidateQueueSizeNSG, ctx.self, toSendLater.nonEmpty)
          val updatedToSend = toSend + (sender -> toSendLater)
          testNSG(navigatingNodeIndex, nodeLocator, nodeCoordinators, queries, updatedToSend, maxQueriesToAskFor, sumOfExactNeighborFound, sumOfNeighborsFound, sumOfNearestNeighbors)

        case KNearestNeighbors(query, neighbors) =>
          val correctNeighborIndices = queries(query)
          val newSum = sumOfNeighborsFound + correctNeighborIndices.intersect(neighbors).length
          val firstNearestNeighborFound = if (neighbors.head == correctNeighborIndices.head) { 1 } else { 0 }
          //ctx.log.info("Still waiting on {} queries: {}", queries.size - 1)
          if (queries.size == 1) {
            ctx.log.info("Overall 1st nearest neighbors found: {}", sumOfExactNeighborFound + firstNearestNeighborFound)
            ctx.log.info("Overall correct neighbors found: {}", newSum)
            ctx.log.info("Precision: {}", newSum.toFloat / sumOfNearestNeighbors.toFloat)
            shutdown(nodeCoordinators)
          } else {
            testNSG(navigatingNodeIndex, nodeLocator, nodeCoordinators, queries - query, toSend, maxQueriesToAskFor, sumOfExactNeighborFound + firstNearestNeighborFound, newSum, sumOfNearestNeighbors)
          }
      }

  def shutdown(nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]]): Behavior[CoordinationEvent] = {
    nodeCoordinators.foreach { nodeCoordinator =>
      nodeCoordinator ! AllDone
    }
    Behaviors.stopped
  }
}
