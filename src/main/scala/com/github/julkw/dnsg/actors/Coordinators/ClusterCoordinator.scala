package com.github.julkw.dnsg.actors.Coordinators

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.Coordinators.GraphRedistributionCoordinator.{AllSharedReplication, NoReplication, OnlyParentsReplication}
import com.github.julkw.dnsg.actors.Coordinators.NodeCoordinator.{AllDone, NodeCoordinationEvent, StartBuildingNSG, StartDistributingData, StartSearchOnGraph}
import com.github.julkw.dnsg.actors.DataHolder.{GetAverageValue, GetGraphFromFile, LoadDataEvent, ReadTestQueries, SaveGraphToFile}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.{FindNearestNeighbors, FindNearestNeighborsStartingFrom, GraphDistribution, RedistributeGraph, SearchOnGraphEvent}
import com.github.julkw.dnsg.actors.createNSG.NSGMerger.MergeNSGEvent
import com.github.julkw.dnsg.actors.nndescent.KnngWorker._
import com.github.julkw.dnsg.util._

object ClusterCoordinator {

  sealed trait CoordinationEvent extends dNSGSerializable

  // setup
  final case class NodeCoordinatorIntroduction(nodeCoordinator: ActorRef[NodeCoordinationEvent]) extends CoordinationEvent

  final case class DataSize(dataSize: Int, dataHolder: ActorRef[LoadDataEvent]) extends CoordinationEvent

  // building the approximate nearest neighbor graph
  final case class KnngDistributionInfo(responsibility: Seq[Int], sender: ActorRef[BuildGraphEvent]) extends CoordinationEvent

  final case class SearchOnGraphDistributionInfo(responsibility: Seq[Int], sender: ActorRef[SearchOnGraphEvent]) extends CoordinationEvent

  final case object FinishedApproximateGraph extends CoordinationEvent

  final case class FinishedNNDescent(worker: ActorRef[BuildGraphEvent]) extends CoordinationEvent

  final case class CorrectFinishedNNDescent(worker: ActorRef[BuildGraphEvent]) extends CoordinationEvent

  final case class ConfirmFinishedNNDescent(worker: ActorRef[BuildGraphEvent]) extends CoordinationEvent

  final case class RedistributionFinished(nodeLocator: NodeLocator[SearchOnGraphEvent]) extends CoordinationEvent

  // connecting the graph
  final case object ConnectionAchieved extends CoordinationEvent

  final case object ConnectorsCleanedUp extends CoordinationEvent

  // building the NSG
  final case class AverageValue(average: Seq[Float]) extends CoordinationEvent

  final case class KNearestNeighbors(query: Seq[Float], neighbors: Seq[Int]) extends CoordinationEvent

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
      ctx => new ClusterCoordinator(ctx, settings).setUp(Set.empty)
    )
  }
}

class ClusterCoordinator(ctx: ActorContext[ClusterCoordinator.CoordinationEvent],
                         settings: Settings) extends Distance {
  import ClusterCoordinator._

  def setUp(nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]]): Behavior[CoordinationEvent] = Behaviors.receiveMessagePartial {
    case NodeCoordinatorIntroduction(nodeCoordinator) =>
      val updatedNodeCoordinators = nodeCoordinators + nodeCoordinator
      if (settings.nodesExpected == updatedNodeCoordinators.size) {
        updatedNodeCoordinators.foreach(nc => nc ! StartDistributingData)
        ctx.log.info("All expected nodes found, start distributing data.")
      }
      setUp(updatedNodeCoordinators)

    case DataSize(dataSize, dataHolder) =>
      distributeDataForKnng(NodeLocatorBuilder(dataSize), nodeCoordinators, dataHolder)

    case KnngDistributionInfo(responsibility, worker) =>
      ctx.self ! KnngDistributionInfo(responsibility, worker)
      setUp(nodeCoordinators)
  }

  def distributeDataForKnng(nodeLocatorBuilder: NodeLocatorBuilder[BuildGraphEvent],
                            nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                            dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
        case KnngDistributionInfo(responsibility, worker) =>
          nodeLocatorBuilder.addLocation(responsibility, worker) match {
            case Some(nodeLocator) =>
              ctx.log.info("Data distribution done, start building approximate graph")
              nodeLocator.allActors.foreach{ worker =>
                worker ! BuildApproximateGraph(nodeLocator)
              }
              buildApproximateKnng(nodeLocator, workersDone = 0, nodeCoordinators, dataHolder)
            case None =>
              // not yet collected all distributionInfo
              distributeDataForKnng(nodeLocatorBuilder, nodeCoordinators, dataHolder)
          }
    }

  def buildApproximateKnng(nodeLocator: NodeLocator[BuildGraphEvent],
                           workersDone: Int,
                           nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                           dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case FinishedApproximateGraph =>
        val knngWorkers = nodeLocator.allActors
        val updatedWorkersDone = workersDone + 1
        if (updatedWorkersDone == knngWorkers.size) {
          ctx.log.info("Approximate graph has been build. Start NNDescent")
          knngWorkers.foreach(worker => worker ! StartNNDescent)
          waitForNnDescent(nodeLocator, Set.empty, nodeCoordinators, dataHolder)
        } else {
          buildApproximateKnng(nodeLocator, updatedWorkersDone, nodeCoordinators, dataHolder)
        }
    }

  def waitForNnDescent(nodeLocator: NodeLocator[BuildGraphEvent],
                       doneWorkers: Set[ActorRef[BuildGraphEvent]],
                       nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                       dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case FinishedNNDescent(worker) =>
        val updatedWorkersDone = doneWorkers + worker
        if (updatedWorkersDone.size == nodeLocator.allActors.size) {
          nodeLocator.allActors.foreach(knngWorker => knngWorker ! GetNNDescentFinishedConfirmation)
          confirmNNDescent(nodeLocator, Set.empty, nodeCoordinators, dataHolder)
        } else {
          waitForNnDescent(nodeLocator, updatedWorkersDone, nodeCoordinators, dataHolder)
        }

      case CorrectFinishedNNDescent(worker) =>
        waitForNnDescent(nodeLocator, doneWorkers - worker, nodeCoordinators, dataHolder)

      case ConfirmFinishedNNDescent(worker) =>
        // I'll ask again once all workers are done so this message can be ignored
        waitForNnDescent(nodeLocator, doneWorkers, nodeCoordinators, dataHolder)
    }

  def confirmNNDescent(nodeLocator: NodeLocator[BuildGraphEvent],
                       confirmedWorkers: Set[ActorRef[BuildGraphEvent]],
                       nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                       dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] = Behaviors.receiveMessagePartial {
    case ConfirmFinishedNNDescent(worker) =>
      val updatedConfirmedWorkers = confirmedWorkers + worker
      if (updatedConfirmedWorkers.size == nodeLocator.allActors.size) {
        ctx.log.info("Done with NNDescent")
        nodeCoordinators.foreach(nodeCoordinator => nodeCoordinator ! StartSearchOnGraph)
        waitOnSearchOnGraphDistributionInfo(nodeLocator.allActors, NodeLocatorBuilder(nodeLocator.graphSize), nodeCoordinators, dataHolder)
      } else {
        confirmNNDescent(nodeLocator, updatedConfirmedWorkers, nodeCoordinators, dataHolder)
      }

    case CorrectFinishedNNDescent(worker) =>
      ctx.log.info("Actor not done during confirmation")
      // apparently not all are done, go back to waiting again
      waitForNnDescent(nodeLocator, nodeLocator.allActors - worker, nodeCoordinators, dataHolder)
  }

  def waitOnSearchOnGraphDistributionInfo(oldWorkers: Set[ActorRef[BuildGraphEvent]],
                                          nodeLocatorBuilder: NodeLocatorBuilder[SearchOnGraphEvent],
                                          nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                                          dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case SearchOnGraphDistributionInfo(indices, actorRef) =>
        nodeLocatorBuilder.addLocation(indices, actorRef) match {
          case Some (nodeLocator) =>
            ctx.log.info("All graphs now with SearchOnGraph actors")
            oldWorkers.foreach(worker => worker ! AllKnngWorkersDone)
            nodeLocator.allActors.foreach(sogActor => sogActor ! GraphDistribution(nodeLocator))
            val aknngFilename = settings.aknngFilePath
            if (aknngFilename.nonEmpty) {
              dataHolder ! SaveGraphToFile(aknngFilename, nodeLocator.allActors, nodeLocator.graphSize, None, ctx.self)
              waitForAKNNGToBeWrittenToFile(nodeLocator, nodeCoordinators, dataHolder)
            } else {
              dataHolder ! GetAverageValue(ctx.self)
              findNavigatingNode(nodeLocator, nodeCoordinators, dataHolder)
            }
          case None =>
            waitOnSearchOnGraphDistributionInfo(oldWorkers, nodeLocatorBuilder, nodeCoordinators, dataHolder)
        }
    }

  def waitForAKNNGToBeWrittenToFile(nodeLocator: NodeLocator[SearchOnGraphEvent],
                                    nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                                    dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case GraphWrittenToFile =>
        ctx.log.info("AKNNG written to file for potential later testing")
        dataHolder ! GetAverageValue(ctx.self)
        findNavigatingNode(nodeLocator, nodeCoordinators, dataHolder)
    }

  def findNavigatingNode(nodeLocator: NodeLocator[SearchOnGraphEvent],
                         nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                         dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case AverageValue(value) =>
        ctx.log.info("Received average value, now looking for Navigating Node")
        // find navigating Node, start from random
        nodeLocator.allActors.head ! FindNearestNeighbors(value, settings.k, ctx.self)
        findNavigatingNode(nodeLocator, nodeCoordinators, dataHolder)

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
          ctx.spawn(GraphRedistributionCoordinator(neighbors.head, replicationModel, nodeLocator, dataHolder, ctx.self), name="GraphRedistributionCoordinator")
          waitOnRedistribution(neighbors.head, nodeCoordinators, dataHolder)
        } else {
          startNSG(navigatingNode, nodeLocator, nodeCoordinators, dataHolder)
        }
    }

  def waitOnRedistribution(navigatingNode: Int,
                           nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                           dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case RedistributionFinished(nodeLocator) =>
        startNSG(navigatingNode, nodeLocator, nodeCoordinators, dataHolder)
    }

  def startNSG(navigatingNode: Int,
               nodeLocator: NodeLocator[SearchOnGraphEvent],
               nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
               dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] = {
    ctx.log.info("Start with NSG")
    nodeCoordinators.foreach(nodeCoordinator => nodeCoordinator ! StartBuildingNSG(navigatingNode, nodeLocator))
    waitOnNSG(Set.empty, navigatingNode, nodeLocator, nodeCoordinators, dataHolder)
  }

  def waitOnNSG(finishedNsgMergers: Set[ActorRef[MergeNSGEvent]],
                navigatingNodeIndex: Int,
                nodeLocator: NodeLocator[SearchOnGraphEvent],
                nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial{
      case InitialNSGDone(nsgMerger) =>
        val updatedMergers = finishedNsgMergers + nsgMerger
        if (updatedMergers.size == settings.nodesExpected) {
          ctx.log.info("Initial NSG seems to be done")
          nodeCoordinators.foreach(nodeCoordinator => nodeCoordinator ! StartSearchOnGraph)
          moveNSGToSog(navigatingNodeIndex, Seq.empty, nodeLocator, nodeCoordinators, dataHolder)
        } else {
          waitOnNSG(updatedMergers, navigatingNodeIndex, nodeLocator, nodeCoordinators, dataHolder)
        }
    }

  def moveNSGToSog(navigatingNodeIndex: Int,
                   actorMidPoints: Seq[(ActorRef[SearchOnGraphEvent], Seq[Float])],
                   nodeLocator: NodeLocator[SearchOnGraphEvent],
                   nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                   dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case NSGonSOG(responsibilityMidPoint, sender) =>
        val updatedActorMidPoints = actorMidPoints :+ (sender, responsibilityMidPoint)
        if (updatedActorMidPoints.length == nodeLocator.allActors.size) {
          ctx.log.info("NSG moved to SearchOnGraphActors. Now connecting graph")
          ctx.spawn(GraphConnectorCoordinator(navigatingNodeIndex, nodeLocator, ctx.self), name="GraphConnectorCoordinator")
          val queryNodeLocator = QueryNodeLocator(updatedActorMidPoints, nodeLocator.graphSize)
          waitForConnectedGraphs(navigatingNodeIndex, queryNodeLocator, nodeCoordinators, dataHolder)
        } else {
          moveNSGToSog(navigatingNodeIndex, updatedActorMidPoints, nodeLocator, nodeCoordinators, dataHolder)
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

    def startTesting(navigatingNodeIndex: Int,
                     nodeLocator: QueryNodeLocator[SearchOnGraphEvent],
                     nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                     dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] = {
      if (settings.queryFilePath.nonEmpty) {
        dataHolder ! ReadTestQueries(settings.queryFilePath, ctx.self)
        testNSG(navigatingNodeIndex, nodeLocator, nodeCoordinators, Map.empty, sumOfNeighborsFound = 0)
      } else {
        shutdown(nodeCoordinators)
      }
    }

    def testNSG(navigatingNodeIndex: Int,
                nodeLocator: QueryNodeLocator[SearchOnGraphEvent],
                nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                queries: Map[Seq[Float], Seq[Int]],
                sumOfNeighborsFound: Int): Behavior[CoordinationEvent] =
      Behaviors.receiveMessagePartial{
        case TestQueries(testQueries) =>
          ctx.log.info("Testing {} queries. Perfect answer would be {} correct nearest neighbors found.", testQueries.size, testQueries.size * settings.k)
          testQueries.foreach(query => nodeLocator.findResponsibleActor(query._1) !
              FindNearestNeighborsStartingFrom(query._1, navigatingNodeIndex, settings.k, ctx.self))
          testNSG(navigatingNodeIndex, nodeLocator, nodeCoordinators, testQueries.toMap, sumOfNeighborsFound)

        case KNearestNeighbors(query, neighbors) =>
          val correctNeighborIndices = queries(query)
          val newSum = sumOfNeighborsFound + correctNeighborIndices.intersect(neighbors).length
          if (queries.size == 1) {
            ctx.log.info("Overall correct neighbors found: {}", newSum)
            shutdown(nodeCoordinators)
          } else {
            testNSG(navigatingNodeIndex, nodeLocator, nodeCoordinators, queries - query, newSum)
          }
      }

  def shutdown(nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]]): Behavior[CoordinationEvent] = {
    nodeCoordinators.foreach { nodeCoordinator =>
      nodeCoordinator ! AllDone
    }
    Behaviors.stopped
  }
}
