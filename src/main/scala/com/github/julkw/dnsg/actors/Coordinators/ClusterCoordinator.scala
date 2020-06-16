package com.github.julkw.dnsg.actors.Coordinators

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.Coordinators.GraphRedistributionCoordinator.{AllSharedReplication, NoReplication}
import com.github.julkw.dnsg.actors.Coordinators.NodeCoordinator.{AllDone, NodeCoordinationEvent, StartBuildingNSG, StartDistributingData, StartSearchOnGraph}
import com.github.julkw.dnsg.actors.DataHolder.{GetAverageValue, LoadDataEvent, ReadTestQueries}
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

  final case class RedistributionFinished(nodeLocator: NodeLocator[SearchOnGraphEvent]) extends CoordinationEvent

  // connecting the graph
  final case object ConnectionAchieved extends CoordinationEvent

  final case object ConnectorsCleanedUp extends CoordinationEvent

  // building the NSG

  final case class AverageValue(average: Seq[Float]) extends CoordinationEvent
  // TODO move to SOGActor
  final case class KNearestNeighbors(query: Seq[Float], neighbors: Seq[Int]) extends CoordinationEvent

  final case class InitialNSGDone(nsgMergers: ActorRef[MergeNSGEvent]) extends CoordinationEvent

  final case object NSGonSOG extends CoordinationEvent

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
              buildApproximateKnng(nodeLocator.allActors, 0, nodeLocator.graphSize, nodeCoordinators, dataHolder)
            case None =>
              // not yet collected all distributionInfo
              distributeDataForKnng(nodeLocatorBuilder, nodeCoordinators, dataHolder)
          }
    }

  def buildApproximateKnng(knngWorkers: Set[ActorRef[BuildGraphEvent]],
                           workersDone: Int,
                           graphSize: Int,
                           nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                           dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case FinishedApproximateGraph =>
        val updatedWorkersDone = workersDone + 1
        if (updatedWorkersDone == knngWorkers.size) {
          ctx.log.info("Approximate graph has been build. Start NNDescent")
          knngWorkers.foreach(worker => worker ! StartNNDescent)
          waitForNnDescent(knngWorkers, Set.empty, graphSize, nodeCoordinators, dataHolder)
        } else {
          buildApproximateKnng(knngWorkers, updatedWorkersDone, graphSize, nodeCoordinators, dataHolder)
        }
    }

  def waitForNnDescent(knngWorkers: Set[ActorRef[BuildGraphEvent]],
                       doneWorkers: Set[ActorRef[BuildGraphEvent]],
                       graphSize: Int,
                       nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                       dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case FinishedNNDescent(worker) =>
        val updatedWorkersDone = doneWorkers + worker
        if (updatedWorkersDone.size == knngWorkers.size) {
          ctx.log.info("NNDescent seems to be done")
          nodeCoordinators.foreach(nodeCoordinator => nodeCoordinator ! StartSearchOnGraph)
          waitOnSearchOnGraphDistributionInfo(NodeLocatorBuilder(graphSize), nodeCoordinators, dataHolder)
        } else {
          waitForNnDescent(knngWorkers, updatedWorkersDone, graphSize, nodeCoordinators, dataHolder)
        }

      case CorrectFinishedNNDescent(worker) =>
        waitForNnDescent(knngWorkers, doneWorkers - worker, graphSize, nodeCoordinators, dataHolder)
    }

  def waitOnSearchOnGraphDistributionInfo(nodeLocatorBuilder: NodeLocatorBuilder[SearchOnGraphEvent],
                                          nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                                          dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case SearchOnGraphDistributionInfo(indices, actorRef) =>
        nodeLocatorBuilder.addLocation(indices, actorRef) match {
          case Some (nodeLocator) =>
            ctx.log.info("All graphs now with SearchOnGraph actors")
            dataHolder ! GetAverageValue(ctx.self)
            nodeLocator.allActors.foreach(sogActor => sogActor ! GraphDistribution(nodeLocator))
            findNavigatingNode(nodeLocator, nodeCoordinators, dataHolder)
          case None =>
            waitOnSearchOnGraphDistributionInfo(nodeLocatorBuilder, nodeCoordinators, dataHolder)
        }
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
        // TODO make dataRedistribution optional
        // TODO get replication model from settings
        ctx.spawn(GraphRedistributionCoordinator(neighbors.head, AllSharedReplication, nodeLocator, dataHolder, ctx.self), name="GraphRedistributionCoordinator")
        waitOnRedistribution(neighbors.head, nodeCoordinators, dataHolder)
    }

  def waitOnRedistribution(navigatingNode: Int,
                           nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                           dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case RedistributionFinished(nodeLocator) =>
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
          moveNSGToSog(nodeLocator.allActors.size, navigatingNodeIndex, nodeLocator, nodeCoordinators, dataHolder)
        } else {
          waitOnNSG(updatedMergers, navigatingNodeIndex, nodeLocator, nodeCoordinators, dataHolder)
        }
    }

  def moveNSGToSog(waitingOnGraphHolders: Int,
                   navigatingNodeIndex: Int,
                   nodeLocator: NodeLocator[SearchOnGraphEvent],
                   nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                   dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case NSGonSOG =>
        if (waitingOnGraphHolders == 1) {
          ctx.log.info("NSG moved to SearchOnGraphActors. Now connecting graph")
          ctx.spawn(GraphConnectorCoordinator(navigatingNodeIndex, nodeLocator, ctx.self), name="GraphConnectorCoordinator")
          waitForConnectedGraphs(navigatingNodeIndex, nodeLocator, nodeCoordinators, dataHolder)
        } else {
          moveNSGToSog(waitingOnGraphHolders - 1, navigatingNodeIndex, nodeLocator, nodeCoordinators, dataHolder)
        }
    }

    def waitForConnectedGraphs(navigatingNodeIndex: Int,
                               nodeLocator: NodeLocator[SearchOnGraphEvent],
                               nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                               dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
      Behaviors.receiveMessagePartial {
        case ConnectionAchieved =>
          ctx.log.info("All Search on Graph actors updated to connected graph, can start search now")
          val settings = Settings(ctx.system.settings.config)
          dataHolder ! ReadTestQueries(settings.queryFilePath, ctx.self)
          testNSG(navigatingNodeIndex, nodeLocator, nodeCoordinators, Map.empty, 0)
      }

    def testNSG(navigatingNodeIndex: Int,
                nodeLocator: NodeLocator[SearchOnGraphEvent],
                nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                queries: Map[Seq[Float], Seq[Int]],
                sumOfNeighborsFound: Int): Behavior[CoordinationEvent] =
      Behaviors.receiveMessagePartial{
        case TestQueries(testQueries) =>
          // TODO instead of using the NNActor, find a way to choose them using the query
          testQueries.foreach(query => nodeLocator.findResponsibleActor(navigatingNodeIndex) !
              FindNearestNeighborsStartingFrom(query._1, navigatingNodeIndex, settings.k, ctx.self))
          testNSG(navigatingNodeIndex, nodeLocator, nodeCoordinators, testQueries.toMap, sumOfNeighborsFound)

        case KNearestNeighbors(query, neighbors) =>
          val correctNeighborIndices = queries(query)
          val newSum = sumOfNeighborsFound + correctNeighborIndices.intersect(neighbors).length
          if (queries.size == 1) {
            ctx.log.info("Overall correct neighbors found: {}", newSum)
            nodeCoordinators.foreach { nodeCoordinator =>
              nodeCoordinator ! AllDone
            }
            Behaviors.stopped
          } else {
            testNSG(navigatingNodeIndex, nodeLocator, nodeCoordinators, queries - query, newSum)
          }
      }
}
