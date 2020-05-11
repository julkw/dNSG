package com.github.julkw.dnsg.actors

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.DataHolder.{GetAverageValue, LoadDataEvent, ReadTestQueries}
import com.github.julkw.dnsg.actors.NodeCoordinator.{NodeCoordinationEvent, StartBuildingNSG, StartDistributingData, StartSearchOnGraph}
import com.github.julkw.dnsg.actors.SearchOnGraph.{AddToGraph, FindNearestNeighbors, FindNearestNeighborsStartingFrom, FindUnconnectedNode, GraphDistribution, KNearestNeighbors, SearchOnGraphEvent, SendResponsibleIndicesTo, UpdateConnectivity}
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

  final case class WrappedSearchOnGraphEvent(event: SearchOnGraph.SearchOnGraphEvent) extends CoordinationEvent

  // building the NSG

  final case class AverageValue(average: Seq[Float]) extends CoordinationEvent

  final case class InitialNSGDone(nsgMergers: ActorRef[MergeNSGEvent]) extends CoordinationEvent

  final case object UpdatedToNSG extends CoordinationEvent

  final case object FinishedUpdatingConnectivity extends CoordinationEvent

  final case class UnconnectedNode(nodeIndex: Int, nodeData: Seq[Float]) extends CoordinationEvent

  final case object AllConnected extends CoordinationEvent

  // testing the graph
  final case class TestQueries(queries: Seq[(Seq[Float], Seq[Int])]) extends CoordinationEvent

  def apply(): Behavior[CoordinationEvent] = Behaviors.setup { ctx =>

    val searchOnGraphEventAdapter: ActorRef[SearchOnGraph.SearchOnGraphEvent] =
      ctx.messageAdapter { event => WrappedSearchOnGraphEvent(event)}

    val settings = Settings(ctx.system.settings.config)
    settings.printSettings(ctx)

    Behaviors.setup(
      ctx => new ClusterCoordinator(searchOnGraphEventAdapter, ctx, settings).setUp(Set.empty)
    )
  }
}

class ClusterCoordinator(searchOnGraphEventAdapter: ActorRef[SearchOnGraph.SearchOnGraphEvent],
                         ctx: ActorContext[ClusterCoordinator.CoordinationEvent],
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
        // find navigating Node, start from random
        graphHolders.head ! FindNearestNeighbors(value, searchOnGraphEventAdapter)
        searchOnKnng(nodeLocator, graphHolders, nodeCoordinators, dataHolder)

      case wrappedSearchOnGraphEvent: WrappedSearchOnGraphEvent =>
        wrappedSearchOnGraphEvent.event match {
          case KNearestNeighbors(query, neighbors) =>
            // Right now the only query being asked for is the NavigationNode, so that has been found
            val navigatingNode = neighbors.head
            ctx.log.info("The navigating node has the index: {}", navigatingNode)
            // TODO do some kind of data redistribution with the knowledge of the navigating node, updating the nodeLocator in the process
            nodeCoordinators.foreach(nodeCoordinator => nodeCoordinator ! StartBuildingNSG(navigatingNode, nodeLocator))
            waitOnNSG(Set.empty, 0, navigatingNode, nodeLocator, graphHolders, nodeCoordinators, dataHolder)
        }
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
        val updatedMergers = finishedNsgMergers + nsgMerger
        if (updatedMergers.size == settings.nodesExpected) {
          ctx.log.info("Initial NSG seems to be done")
          finishedNsgMergers.foreach { merger => merger ! NSGToSearchOnGraph}
          nodeCoordinators.foreach(nodeCoordinator => nodeCoordinator ! StartSearchOnGraph)
        }
        waitOnNSG(finishedNsgMergers, movedToSog, navigatingNodeIndex, nodeLocator, graphHolders, nodeCoordinators, dataHolder)

      case UpdatedToNSG =>
        if (movedToSog + 1 == settings.nodesExpected) {
          // nsg is fully back to search on graph actors
          nodeLocator.findResponsibleActor(navigatingNodeIndex) ! UpdateConnectivity(navigatingNodeIndex)
          connectNSG(navigatingNodeIndex, nodeLocator, graphHolders, dataHolder, -1, Seq.empty)
        } else {
          waitOnNSG(finishedNsgMergers, movedToSog + 1, navigatingNodeIndex, nodeLocator, graphHolders, nodeCoordinators, dataHolder)
        }
    }

  def connectNSG(navigatingNodeIndex: Int,
                 nodeLocator: NodeLocator[SearchOnGraphEvent],
                 graphHolders: Set[ActorRef[SearchOnGraphEvent]],
                 dataHolder: ActorRef[LoadDataEvent],
                 latestUnconnectedNodeIndex: Int,
                 latestUnconnectedNodedData: Seq[Float]) : Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial{
      case FinishedUpdatingConnectivity =>
        // find out if there is still an unconnected node and connect it
        val startingNode = graphHolders.head
        startingNode ! FindUnconnectedNode(ctx.self, graphHolders - startingNode)
        connectNSG(navigatingNodeIndex, nodeLocator, graphHolders, dataHolder, latestUnconnectedNodeIndex, latestUnconnectedNodedData)

      case UnconnectedNode(nodeIndex, nodeData) =>
        ctx.log.info("found an unconnected node")
        nodeLocator.findResponsibleActor(nodeIndex) !
          FindNearestNeighborsStartingFrom(nodeData, navigatingNodeIndex, searchOnGraphEventAdapter)
        connectNSG(navigatingNodeIndex, nodeLocator, graphHolders, dataHolder, nodeIndex, nodeData)

      case wrappedSearchOnGraphEvent: WrappedSearchOnGraphEvent =>
        wrappedSearchOnGraphEvent.event match {
          case KNearestNeighbors(query, neighbors) =>
            // Right now the only query being asked for is to connect unconnected nodes
            assert(query == latestUnconnectedNodedData)
            neighbors.foreach(reverseNeighbor =>
              nodeLocator.findResponsibleActor(reverseNeighbor) !
                AddToGraph(reverseNeighbor, latestUnconnectedNodeIndex))
            nodeLocator.findResponsibleActor(latestUnconnectedNodeIndex) ! UpdateConnectivity(latestUnconnectedNodeIndex)
            connectNSG(navigatingNodeIndex, nodeLocator, graphHolders, dataHolder, -1, Seq.empty)
        }

      case AllConnected =>
        ctx.log.info("NSG build seems to be done")
        val settings = Settings(ctx.system.settings.config)
        dataHolder ! ReadTestQueries(settings.queryFilePath, ctx.self)
        testNSG(navigatingNodeIndex, nodeLocator, graphHolders, Map.empty, 0)
    }

    def testNSG(navigatingNodeIndex: Int,
                nodeLocator: NodeLocator[SearchOnGraphEvent],
                graphHolders: Set[ActorRef[SearchOnGraphEvent]],
                queries: Map[Seq[Float], Seq[Int]],
                sumOfNeighborsFound: Int): Behavior[CoordinationEvent] =
      Behaviors.receiveMessagePartial{
        case TestQueries(testQueries) =>
          // TODO instead of using the NNActor, find a way to choose them using the query
          testQueries.foreach(query => nodeLocator.findResponsibleActor(navigatingNodeIndex) !
              FindNearestNeighborsStartingFrom(query._1, navigatingNodeIndex, searchOnGraphEventAdapter))
          testNSG(navigatingNodeIndex, nodeLocator, graphHolders, testQueries.toMap, sumOfNeighborsFound)

        case wrappedSearchOnGraphEvent: WrappedSearchOnGraphEvent =>
          wrappedSearchOnGraphEvent.event match {
            case KNearestNeighbors(query, neighbors) =>
              val correctNeighborIndices = queries(query)
              val newSum = sumOfNeighborsFound + correctNeighborIndices.intersect(neighbors).length
              if (queries.size == 1) {
                ctx.log.info("Overall correct neighbors found: {}", newSum)
                ctx.system.terminate()
                Behaviors.stopped
              } else {
                testNSG(navigatingNodeIndex, nodeLocator, graphHolders, queries - query, newSum)
              }

          }
      }

}
