package com.github.julkw.dnsg.actors.Coordinators

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.Coordinators.GraphConnectorCoordinator.{ConnectionCoordinationEvent, DoneWithConnecting, StartGraphRedistribution}
import com.github.julkw.dnsg.actors.Coordinators.NodeCoordinator.{AllDone, NodeCoordinationEvent, StartBuildingNSG, StartDistributingData, StartSearchOnGraph}
import com.github.julkw.dnsg.actors.DataHolder.{GetAverageValue, LoadDataEvent, ReadTestQueries, StartRedistributingData}
import com.github.julkw.dnsg.actors.GraphRedistributer.{AllSharedReplication, DistributeData, NoReplication, RedistributionEvent}
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

  final case object FinishedNNDescent extends CoordinationEvent

  final case object CorrectFinishedNNDescent extends CoordinationEvent

  // redistributing the graph
  final case class RedistributerDistributionInfo(responsibility: Seq[Int], sender: ActorRef[RedistributionEvent]) extends CoordinationEvent

  final case class RedistributionNodeAssignments(nodeAssignments: Map[Int, Set[ActorRef[SearchOnGraphEvent]]]) extends CoordinationEvent

  final case object DoneWithRedistribution extends CoordinationEvent

  // connecting the graph
  final case object ConnectionAchieved extends CoordinationEvent

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
      // TODO this is not very nice
      ctx.log.info("Got distribution info too early, forwarding to self")
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
              ctx.log.info("Got distribution info from a worker")
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
          waitOnSearchOnGraphDistributionInfo(NodeLocatorBuilder(graphSize), nodeCoordinators, dataHolder)
        } else {
          waitForNnDescent(knngWorkers, updatedWorkersDone, graphSize, nodeCoordinators, dataHolder)
        }

      case CorrectFinishedNNDescent =>
        waitForNnDescent(knngWorkers, workersDone - 1, graphSize, nodeCoordinators, dataHolder)
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
            // TODO wait till they have received dist info?
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
        ctx.log.info("Now connecting graph for redistribution")
        val connectorCoordinator =  ctx.spawn(GraphConnectorCoordinator(neighbors.head, nodeLocator, ctx.self), name="GraphConnectorCoordinator")
        connectGraphForRedistribution(neighbors.head, connectorCoordinator, nodeLocator, nodeCoordinators, dataHolder)
        // TODO make dataRedistribution optional
        // nodeCoordinators.foreach(nodeCoordinator => nodeCoordinator ! StartBuildingNSG(navigatingNode, nodeLocator))
        // waitOnNSG(Set.empty, navigatingNode, nodeLocator, graphHolders, nodeCoordinators, dataHolder)
    }

  def connectGraphForRedistribution(navigatingNodeIndex: Int,
                                    connectorCoordinator: ActorRef[ConnectionCoordinationEvent],
                                    nodeLocator: NodeLocator[SearchOnGraphEvent],
                                    nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                                    dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case ConnectionAchieved =>
        ctx.log.info("All Search on Graph actors updated to connected graph, can start redistribution now")
        connectorCoordinator ! StartGraphRedistribution
        waitOnRedistributionDistributionInfo(navigatingNodeIndex, connectorCoordinator, NodeLocatorBuilder(nodeLocator.graphSize), nodeLocator, nodeCoordinators, dataHolder)
    }

  def waitOnRedistributionDistributionInfo(navigatingNodeIndex: Int,
                                           connectorCoordinator: ActorRef[ConnectionCoordinationEvent],
                                           redistributionLocations: NodeLocatorBuilder[ActorRef[RedistributionEvent]],
                                           sogNodeLocator: NodeLocator[SearchOnGraphEvent],
                                           nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                                           dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case RedistributerDistributionInfo(responsibilities, redistributer) =>
        redistributionLocations.addLocation(responsibilities, redistributer) match {
          case Some(redistributionLocator) =>
            // TODO take replicationMode from settings
            ctx.log.info("All redistributers started")
            redistributionLocator.allActors.foreach(graphRedistributer => graphRedistributer ! DistributeData(sogNodeLocator.allActors, AllSharedReplication, redistributionLocator))
            waitOnRedistributionAssignments(navigatingNodeIndex, connectorCoordinator, NodeLocatorBuilder(sogNodeLocator.graphSize), sogNodeLocator, nodeCoordinators, dataHolder)
          case None =>
            waitOnRedistributionDistributionInfo(navigatingNodeIndex, connectorCoordinator, redistributionLocations, sogNodeLocator, nodeCoordinators, dataHolder)
        }
    }

  def waitOnRedistributionAssignments(navigatingNodeIndex: Int,
                                      connectorCoordinator: ActorRef[ConnectionCoordinationEvent],
                                      redistributionAssignments: NodeLocatorBuilder[Set[ActorRef[SearchOnGraphEvent]]],
                                      sogNodeLocator: NodeLocator[SearchOnGraphEvent],
                                      nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                                      dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case RedistributionNodeAssignments(assignments) =>
        redistributionAssignments.addFromMap(assignments) match {
          case Some(redistributionAssignments) =>
            ctx.log.info("Calculated redistribution, now telling actors to do it")
            sogNodeLocator.allActors.foreach(graphHolder => graphHolder ! RedistributeGraph(redistributionAssignments))
            dataHolder ! StartRedistributingData(redistributionAssignments)
            // TODO have graphRedistributer return slightly different datastructure that shows assignWithChildren vs assignWithParents?
            val simplifiedLocator = NodeLocator(redistributionAssignments.locationData.map(_.head), sogNodeLocator.allActors)
            waitForRedistribution(navigatingNodeIndex, connectorCoordinator, 0, simplifiedLocator, nodeCoordinators, dataHolder)
          case None =>
            waitOnRedistributionAssignments(navigatingNodeIndex, connectorCoordinator, redistributionAssignments, sogNodeLocator, nodeCoordinators, dataHolder)
        }
    }

  def waitForRedistribution(navigatingNode: Int,
                            connectorCoordinator: ActorRef[ConnectionCoordinationEvent],
                            finishedWorkers: Int,
                            nodeLocator: NodeLocator[SearchOnGraphEvent],
                            nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                            dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case DoneWithRedistribution =>
        ctx.log.info("{} graphHolders done with redistribution", finishedWorkers + 1)
        if (finishedWorkers + 1 == nodeLocator.allActors.size) {
          ctx.log.info("Done with data redistribution, starting with NSG")
          connectorCoordinator ! DoneWithConnecting
          nodeCoordinators.foreach(nodeCoordinator => nodeCoordinator ! StartBuildingNSG(navigatingNode, nodeLocator))
          waitOnNSG(Set.empty, navigatingNode, nodeLocator, nodeCoordinators, dataHolder)
        } else {
          waitForRedistribution(navigatingNode, connectorCoordinator, finishedWorkers + 1, nodeLocator, nodeCoordinators, dataHolder)
        }
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
