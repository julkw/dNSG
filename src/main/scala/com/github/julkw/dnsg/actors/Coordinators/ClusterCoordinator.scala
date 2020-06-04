package com.github.julkw.dnsg.actors.Coordinators

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.Coordinators.NodeCoordinator.{AllDone, NodeCoordinationEvent, StartBuildingNSG, StartDistributingData, StartSearchOnGraph, StartSearchOnGraphAndConnect}
import com.github.julkw.dnsg.actors.DataHolder.{GetAverageValue, LoadDataEvent, ReadTestQueries, StartRedistributingData}
import com.github.julkw.dnsg.actors.GraphConnector.{AddToGraph, ConnectGraphEvent, ConnectorDistributionInfo, FindUnconnectedNode, GraphConnected, UpdateConnectivity}
import com.github.julkw.dnsg.actors.GraphRedistributer.{DistributeData, NoReplication, RedistributionEvent}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.{FindNearestNeighbors, FindNearestNeighborsStartingFrom, GraphDistribution, RedistributeGraph, SearchOnGraphEvent, StartGraphRedistribution}
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

  final case class KNearestNeighbors(query: Seq[Float], neighbors: Seq[Int]) extends CoordinationEvent

  final case class InitialNSGDone(nsgMergers: ActorRef[MergeNSGEvent]) extends CoordinationEvent

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
      distributeDataForKnng(NodeLocatorBuilder(dataSize), Set.empty, nodeCoordinators, dataHolder)

    case KnngDistributionInfo(responsibility, worker) =>
      // TODO this is not very nice
      ctx.log.info("Got distribution info too early, forwarding to self")
      ctx.self ! KnngDistributionInfo(responsibility, worker)
      setUp(nodeCoordinators)
  }

  def distributeDataForKnng(nodeLocatorBuilder: NodeLocatorBuilder[ActorRef[BuildGraphEvent]],
                            knngWorkers: Set[ActorRef[BuildGraphEvent]],
                            nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                            dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
        case KnngDistributionInfo(responsibility, worker) =>
          val updatedKnngWorkers = knngWorkers + worker
          nodeLocatorBuilder.addLocation(responsibility, worker) match {
            case Some(nodeLocator) =>
              ctx.log.info("Data distribution done, start building approximate graph")
              updatedKnngWorkers.foreach{ worker =>
                worker ! BuildApproximateGraph(nodeLocator, updatedKnngWorkers)
              }
              buildApproximateKnng(updatedKnngWorkers, 0, nodeLocator.graphSize, nodeCoordinators, dataHolder)
            case None =>
              ctx.log.info("Got distribution info from {} workers, still waiting for rest", updatedKnngWorkers.size)
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

  def waitOnSearchOnGraphDistributionInfo(nodeLocatorBuilder: NodeLocatorBuilder[ActorRef[SearchOnGraphEvent]],
                                          graphHolders: Set[ActorRef[SearchOnGraphEvent]],
                                          nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                                          dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case SearchOnGraphDistributionInfo(indices, actorRef) =>
        val updatedSogActors = graphHolders + actorRef
        nodeLocatorBuilder.addLocation(indices, actorRef) match {
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

  def searchOnKnng(nodeLocator: NodeLocator[ActorRef[SearchOnGraphEvent]],
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
        /*
        graphHolders.foreach(graphHolder => graphHolder ! StartGraphRedistribution)
        ctx.log.info("Start graph redistribution")
        waitOnRedistributionDistributionInfo(neighbors.head, Set.empty, NodeLocatorBuilder(nodeLocator.graphSize), nodeLocator, graphHolders, nodeCoordinators, dataHolder)
         */
        nodeCoordinators.foreach(nodeCoordinator => nodeCoordinator ! StartBuildingNSG(navigatingNode, nodeLocator))
        waitOnNSG(Set.empty, navigatingNode, nodeLocator, graphHolders, nodeCoordinators, dataHolder)
    }

  def waitOnRedistributionDistributionInfo(navigatingNodeIndex: Int,
                                           redistributers: Set[ActorRef[RedistributionEvent]],
                                           redistributionLocations: NodeLocatorBuilder[ActorRef[RedistributionEvent]],
                                           sogNodeLocator: NodeLocator[ActorRef[SearchOnGraphEvent]],
                                           graphHolders: Set[ActorRef[SearchOnGraphEvent]],
                                           nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                                           dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case RedistributerDistributionInfo(responsibilities, redistributer) =>
        val updatedRedistributers = redistributers + redistributer
        redistributionLocations.addLocation(responsibilities, redistributer) match {
          case Some(redistributionLocator) =>
            // TODO take replicationMode from settings
            ctx.log.info("All redistributers started")
            updatedRedistributers.foreach(graphRedistributer => graphRedistributer ! DistributeData(navigatingNodeIndex, graphHolders, NoReplication, redistributionLocator))
            waitOnRedistributionAssignments(navigatingNodeIndex, NodeLocatorBuilder(sogNodeLocator.graphSize), sogNodeLocator, graphHolders, nodeCoordinators, dataHolder)
          case None =>
            waitOnRedistributionDistributionInfo(navigatingNodeIndex, updatedRedistributers, redistributionLocations, sogNodeLocator, graphHolders, nodeCoordinators, dataHolder)
        }
    }

  def waitOnRedistributionAssignments(navigatingNodeIndex: Int,
                                      redistributionAssignments: NodeLocatorBuilder[Set[ActorRef[SearchOnGraphEvent]]],
                                      sogNodeLocator: NodeLocator[ActorRef[SearchOnGraphEvent]],
                                      graphHolders: Set[ActorRef[SearchOnGraphEvent]],
                                      nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                                      dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case RedistributionNodeAssignments(assignments) =>
        redistributionAssignments.addFromMap(assignments) match {
          case Some(redistributionAssignments) =>
            // TODO send to SearchOnGraph Actors and dataHolders and then wait for them to finish moving data around
            ctx.log.info("Calculated redistribution, now telling actors to do it")
            graphHolders.foreach(graphHolder => graphHolder ! RedistributeGraph(redistributionAssignments))
            dataHolder ! StartRedistributingData(redistributionAssignments)
            val simplifiedLocator = NodeLocator(redistributionAssignments.locationData.map(_.head))
            waitForRedistribution(navigatingNodeIndex, 0, simplifiedLocator, graphHolders, nodeCoordinators, dataHolder)
          case None =>
            waitOnRedistributionAssignments(navigatingNodeIndex, redistributionAssignments, sogNodeLocator, graphHolders, nodeCoordinators, dataHolder)
        }
    }

  // TODO don't build new locator here, we already have one. Just wait for a DoneWithRedistribution message
  def waitForRedistribution(navigatingNode: Int,
                            finishedWorkers: Int,
                            nodeLocator: NodeLocator[ActorRef[SearchOnGraphEvent]],
                            graphHolders: Set[ActorRef[SearchOnGraphEvent]],
                            nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                            dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case DoneWithRedistribution =>
        if (finishedWorkers + 1 == graphHolders.size) {
          ctx.log.info("Done with data redistribution, starting with NSG")
          nodeCoordinators.foreach(nodeCoordinator => nodeCoordinator ! StartBuildingNSG(navigatingNode, nodeLocator))
          waitOnNSG(Set.empty, navigatingNode, nodeLocator, graphHolders, nodeCoordinators, dataHolder)
        }
        waitForRedistribution(navigatingNode, finishedWorkers + 1, nodeLocator, graphHolders, nodeCoordinators, dataHolder)
    }

  def waitOnNSG(finishedNsgMergers: Set[ActorRef[MergeNSGEvent]],
                navigatingNodeIndex: Int,
                nodeLocator: NodeLocator[ActorRef[SearchOnGraphEvent]],
                graphHolders: Set[ActorRef[SearchOnGraphEvent]],
                nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial{
      case InitialNSGDone(nsgMerger) =>
        ctx.log.info("One NSGMerger is done")
        val updatedMergers = finishedNsgMergers + nsgMerger
        if (updatedMergers.size == settings.nodesExpected) {
          ctx.log.info("Initial NSG seems to be done")
          val connectorCoordinator = ctx.spawn(GraphConnectorCoordinator(navigatingNodeIndex, nodeLocator, ctx.self), name="GraphConnectorCoordinator")
          nodeCoordinators.foreach(nodeCoordinator => nodeCoordinator ! StartSearchOnGraphAndConnect(connectorCoordinator))
          waitForConnectedGraphs(navigatingNodeIndex, nodeLocator, graphHolders, nodeCoordinators, dataHolder)
        } else {
          waitOnNSG(updatedMergers, navigatingNodeIndex, nodeLocator, graphHolders, nodeCoordinators, dataHolder)
        }
    }

    def waitForConnectedGraphs(navigatingNodeIndex: Int,
                               nodeLocator: NodeLocator[ActorRef[SearchOnGraphEvent]],
                               graphHolders: Set[ActorRef[SearchOnGraphEvent]],
                               nodeCoordinators: Set[ActorRef[NodeCoordinationEvent]],
                               dataHolder: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
      Behaviors.receiveMessagePartial {
        case ConnectionAchieved =>
          ctx.log.info("All Search on Graph actors updated to connected graph, can start search now")
          val settings = Settings(ctx.system.settings.config)
          dataHolder ! ReadTestQueries(settings.queryFilePath, ctx.self)
          testNSG(navigatingNodeIndex, nodeLocator, graphHolders, nodeCoordinators, Map.empty, 0)
      }

    def testNSG(navigatingNodeIndex: Int,
                nodeLocator: NodeLocator[ActorRef[SearchOnGraphEvent]],
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
