package com.github.julkw.dnsg.actors

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.DataHolder.{GetAverageValue, LoadDataEvent, ReadTestQueries}
import com.github.julkw.dnsg.actors.NodeCoordinator.{AllDone, NodeCoordinationEvent, StartBuildingNSG, StartDistributingData, StartSearchOnGraph}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.{AddToGraph, FindNearestNeighbors, FindNearestNeighborsStartingFrom, FindUnconnectedNode, GraphConnected, GraphDistribution, SearchOnGraphEvent, SendResponsibleIndicesTo, UpdateConnectivity}
import com.github.julkw.dnsg.actors.createNSG.NSGMerger.{MergeNSGEvent, NSGToSearchOnGraph}
import com.github.julkw.dnsg.actors.nndescent.KnngWorker._
import com.github.julkw.dnsg.util.{Distance, NodeLocator, NodeLocatorBuilder, Settings, dNSGSerializable}

object ClusterCoordinator {

  sealed trait CoordinationEvent extends dNSGSerializable

  // setup
  final case class NodeCoordinatorIntroduction(nodeCoordinator: ActorRef[NodeCoordinationEvent]) extends CoordinationEvent

  final case class DataSize(dataSize: Int, dataHolder: ActorRef[LoadDataEvent]) extends CoordinationEvent

  // building the approximate nearest neighbor graph
  final case class KnngDistributionInfo(responsibility: Seq[Int], sender: ActorRef[BuildGraphEvent]) extends CoordinationEvent

  final case class SearchOnGraphDistributionInfo(responsibility: Seq[Int], sender: ActorRef[SearchOnGraphEvent]) extends CoordinationEvent

  final case object FinishedApproximateGraph extends CoordinationEvent

  final case object FinishedNNDescent extends CoordinationEvent

  final case object CorrectFinishedNNDescent extends CoordinationEvent

  final case class WrappedSearchOnGraphEvent(event: SearchOnGraphActor.SearchOnGraphEvent) extends CoordinationEvent

  // building the NSG

  final case class AverageValue(average: Seq[Float]) extends CoordinationEvent

  final case class KNearestNeighbors(query: Seq[Float], neighbors: Seq[Int]) extends CoordinationEvent

  final case class InitialNSGDone(nsgMergers: ActorRef[MergeNSGEvent]) extends CoordinationEvent

  final case object UpdatedToNSG extends CoordinationEvent

  final case object FinishedUpdatingConnectivity extends CoordinationEvent

  final case class UnconnectedNode(nodeIndex: Int, nodeData: Seq[Float]) extends CoordinationEvent

  final case object AllConnected extends CoordinationEvent

  final case object BackToSearch extends CoordinationEvent

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
      }
      setUp(updatedNodeCoordinators)
    case DataSize(dataSize, dataHolder) =>
      distributeDataForKnng(NodeLocatorBuilder(dataSize), Set.empty, nodeCoordinators, dataHolder)
  }

  def distributeDataForKnng(nodeLocatorBuilder: NodeLocatorBuilder[BuildGraphEvent],
                            knngWorkers: Set[ActorRef[BuildGraphEvent]],
                            nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                            dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
        case KnngDistributionInfo(responsibility, worker) =>
          val updatedKnngWorkers = knngWorkers + worker
          nodeLocatorBuilder.addLocations(responsibility, worker) match {
            case Some(nodeLocator) =>
              ctx.log.info("Data distribution done, start building approximate graph")
              updatedKnngWorkers.foreach{ worker =>
                worker ! BuildApproximateGraph(nodeLocator, updatedKnngWorkers)
              }
              buildApproximateKnng(updatedKnngWorkers, 0, nodeLocator.graphSize, nodeCoordinators, dataHolder)
            case None =>
              // not yet collected all distributionInfo
              distributeDataForKnng(nodeLocatorBuilder, updatedKnngWorkers, nodeCoordinators, dataHolder)
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
          waitForNnDescent(knngWorkers, 0, graphSize, nodeCoordinators, dataHolder)
        } else {
          buildApproximateKnng(knngWorkers, updatedWorkersDone, graphSize, nodeCoordinators, dataHolder)
        }
    }

  def waitForNnDescent(knngWorkers: Set[ActorRef[BuildGraphEvent]],
                       workersDone: Int,
                       graphSize: Int,
                       nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                       dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case FinishedNNDescent =>
        val updatedWorkersDone = workersDone + 1
        if (updatedWorkersDone == knngWorkers.size) {
          ctx.log.info("NNDescent seems to be done")
          nodeCoordinators.foreach(nodeCoordinator => nodeCoordinator ! StartSearchOnGraph)
          waitOnSearchOnGraphDistributionInfo(NodeLocatorBuilder(graphSize), Set.empty, nodeCoordinators, dataHolder)
        } else {
          waitForNnDescent(knngWorkers, updatedWorkersDone, graphSize, nodeCoordinators, dataHolder)
        }

      case CorrectFinishedNNDescent =>
        waitForNnDescent(knngWorkers, workersDone - 1, graphSize, nodeCoordinators, dataHolder)
    }

  def waitOnSearchOnGraphDistributionInfo(nodeLocatorBuilder: NodeLocatorBuilder[SearchOnGraphEvent],
                                          graphHolders: Set[ActorRef[SearchOnGraphEvent]],
                                          nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                                          dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case SearchOnGraphDistributionInfo(indices, actorRef) =>
        val updatedSogActors = graphHolders + actorRef
        nodeLocatorBuilder.addLocations(indices, actorRef) match {
          case Some (nodeLocator) =>
            ctx.log.info("All graphs now with SearchOnGraph actors")
            dataHolder ! GetAverageValue(ctx.self)
            // TODO wait till they have received dist info?
            updatedSogActors.foreach(sogActor => sogActor ! GraphDistribution(nodeLocator))
            searchOnKnng(nodeLocator, updatedSogActors, nodeCoordinators, dataHolder)
          case None =>
            waitOnSearchOnGraphDistributionInfo(nodeLocatorBuilder, updatedSogActors, nodeCoordinators, dataHolder)
        }
    }

  def searchOnKnng(nodeLocator: NodeLocator[SearchOnGraphEvent],
                   graphHolders: Set[ActorRef[SearchOnGraphEvent]],
                   nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                   dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case AverageValue(value) =>
        ctx.log.info("Received average value, now looking for Navigating Node")
        // find navigating Node, start from random
        graphHolders.head ! FindNearestNeighbors(value, settings.k, ctx.self)
        searchOnKnng(nodeLocator, graphHolders, nodeCoordinators, dataHolder)

      case KNearestNeighbors(query, neighbors) =>
        // Right now the only query being asked for is the NavigationNode, so that has been found
        val navigatingNode = neighbors.head
        ctx.log.info("The navigating node has the index: {}", navigatingNode)
        // TODO do some kind of data redistribution with the knowledge of the navigating node, updating the nodeLocator in the process
        nodeCoordinators.foreach(nodeCoordinator => nodeCoordinator ! StartBuildingNSG(navigatingNode, nodeLocator))
        waitOnNSG(Set.empty, 0, navigatingNode, nodeLocator, graphHolders, nodeCoordinators, dataHolder)
    }

  def waitOnNSG(finishedNsgMergers: Set[ActorRef[MergeNSGEvent]],
                movedToSog: Int,
                navigatingNodeIndex: Int,
                nodeLocator: NodeLocator[SearchOnGraphEvent],
                graphHolders: Set[ActorRef[SearchOnGraphEvent]],
                nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial{
      case InitialNSGDone(nsgMerger) =>
        ctx.log.info("One NSGMerger is done")
        val updatedMergers = finishedNsgMergers + nsgMerger
        if (updatedMergers.size == settings.nodesExpected) {
          ctx.log.info("Initial NSG seems to be done")
          finishedNsgMergers.foreach { merger => merger ! NSGToSearchOnGraph}
          nodeCoordinators.foreach(nodeCoordinator => nodeCoordinator ! StartSearchOnGraph)
        }
        waitOnNSG(updatedMergers, movedToSog, navigatingNodeIndex, nodeLocator, graphHolders, nodeCoordinators, dataHolder)

      case UpdatedToNSG =>
        if (movedToSog + 1 == graphHolders.size) {
          // nsg is fully back to search on graph actors
          nodeLocator.findResponsibleActor(navigatingNodeIndex) ! UpdateConnectivity(navigatingNodeIndex)
          connectNSG(navigatingNodeIndex, nodeLocator, graphHolders, nodeCoordinators, dataHolder, -1, Seq.empty)
        } else {
          waitOnNSG(finishedNsgMergers, movedToSog + 1, navigatingNodeIndex, nodeLocator, graphHolders, nodeCoordinators, dataHolder)
        }
    }

  def connectNSG(navigatingNodeIndex: Int,
                 nodeLocator: NodeLocator[SearchOnGraphEvent],
                 graphHolders: Set[ActorRef[SearchOnGraphEvent]],
                 nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                 dataHolder: ActorRef[LoadDataEvent],
                 latestUnconnectedNodeIndex: Int,
                 latestUnconnectedNodedData: Seq[Float]) : Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial{
      case FinishedUpdatingConnectivity =>
        // find out if there is still an unconnected node and connect it
        val startingNode = graphHolders.head
        startingNode ! FindUnconnectedNode(ctx.self, graphHolders - startingNode)
        connectNSG(navigatingNodeIndex, nodeLocator, graphHolders, nodeCoordinators, dataHolder, latestUnconnectedNodeIndex, latestUnconnectedNodedData)

      case UnconnectedNode(nodeIndex, nodeData) =>
        ctx.log.info("found an unconnected node")
        nodeLocator.findResponsibleActor(nodeIndex) !
          FindNearestNeighborsStartingFrom(nodeData, navigatingNodeIndex, settings.k, ctx.self)
        connectNSG(navigatingNodeIndex, nodeLocator, graphHolders, nodeCoordinators, dataHolder, nodeIndex, nodeData)

      case KNearestNeighbors(query, neighbors) =>
        // Right now the only query being asked for is to connect unconnected nodes
        assert(query == latestUnconnectedNodedData)
        neighbors.foreach(reverseNeighbor =>
          nodeLocator.findResponsibleActor(reverseNeighbor) !
            AddToGraph(reverseNeighbor, latestUnconnectedNodeIndex))
        nodeLocator.findResponsibleActor(latestUnconnectedNodeIndex) ! UpdateConnectivity(latestUnconnectedNodeIndex)
        connectNSG(navigatingNodeIndex, nodeLocator, graphHolders, nodeCoordinators, dataHolder, -1, Seq.empty)

      case AllConnected =>
        ctx.log.info("NSG now fully connected")
        graphHolders.foreach(graphHolder => graphHolder ! GraphConnected(ctx.self))
        waitForBackToSearch(navigatingNodeIndex, nodeLocator, graphHolders, nodeCoordinators, dataHolder, 0)
    }

    def waitForBackToSearch(navigatingNodeIndex: Int,
                            nodeLocator: NodeLocator[SearchOnGraphEvent],
                            graphHolders: Set[ActorRef[SearchOnGraphEvent]],
                            nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                            dataHolder: ActorRef[LoadDataEvent],
                            responses: Int): Behavior[CoordinationEvent] =
      Behaviors.receiveMessagePartial {
        case BackToSearch =>
          if (responses + 1 == graphHolders.size) {
            ctx.log.info("All Search on Graph actors back to search, can start testing now")
            val settings = Settings(ctx.system.settings.config)
            dataHolder ! ReadTestQueries(settings.queryFilePath, ctx.self)
            testNSG(navigatingNodeIndex, nodeLocator, graphHolders, nodeCoordinators, Map.empty, 0)
          } else {
            waitForBackToSearch(navigatingNodeIndex, nodeLocator, graphHolders, nodeCoordinators, dataHolder, responses + 1)
          }
      }

    def testNSG(navigatingNodeIndex: Int,
                nodeLocator: NodeLocator[SearchOnGraphEvent],
                graphHolders: Set[ActorRef[SearchOnGraphEvent]],
                nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                queries: Map[Seq[Float], Seq[Int]],
                sumOfNeighborsFound: Int): Behavior[CoordinationEvent] =
      Behaviors.receiveMessagePartial{
        case TestQueries(testQueries) =>
          // TODO instead of using the NNActor, find a way to choose them using the query
          testQueries.foreach(query => nodeLocator.findResponsibleActor(navigatingNodeIndex) !
              FindNearestNeighborsStartingFrom(query._1, navigatingNodeIndex, settings.k, ctx.self))
          testNSG(navigatingNodeIndex, nodeLocator, graphHolders, nodeCoordinators, testQueries.toMap, sumOfNeighborsFound)

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
            testNSG(navigatingNodeIndex, nodeLocator, graphHolders, nodeCoordinators, queries - query, newSum)
          }
      }
}
