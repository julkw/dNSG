package com.github.julkw.dnsg.actors.Coordinators

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.{ConnectionAchieved, CoordinationEvent, DoneWithRedistribution, KNearestNeighbors, RedistributerDistributionInfo, RedistributionNodeAssignments}
import com.github.julkw.dnsg.actors.Coordinators.GraphConnectorCoordinator.ConnectionCoordinationEvent
import com.github.julkw.dnsg.actors.Coordinators.NodeCoordinator.{NodeCoordinationEvent, StartBuildingNSG}
import com.github.julkw.dnsg.actors.DataHolder.{LoadDataEvent, StartRedistributingData}
import com.github.julkw.dnsg.actors.GraphConnector.{AddEdgeAndContinue, BuildTreeFrom, ConnectGraphEvent, ConnectorDistributionInfo, FindUnconnectedNode, GraphConnected, StartGraphRedistributers}
import com.github.julkw.dnsg.actors.GraphRedistributer.{AllSharedReplication, DistributeData, RedistributionEvent}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.{AddToGraph, ConnectGraph, FindNearestNeighborsStartingFrom, RedistributeGraph, SearchOnGraphEvent}
import com.github.julkw.dnsg.util.{NodeLocator, NodeLocatorBuilder, dNSGSerializable}

object GraphRedistributionCoordinator {

  trait RedistributionCoordinationEvent extends dNSGSerializable


  def apply(navigatingNodeIndex: Int,
            graphNodeLocator: NodeLocator[SearchOnGraphEvent],
            clusterCoordinator: ActorRef[CoordinationEvent]): Behavior[RedistributionCoordinationEvent] =
    Behaviors.setup { ctx =>
      new GraphConnectorCoordinator(navigatingNodeIndex, graphNodeLocator, clusterCoordinator, ctx).setup()
    }

}

class GraphRedistributionCoordinator(navigatingNodeIndex: Int,
                                    graphNodeLocator: NodeLocator[SearchOnGraphEvent],
                                    clusterCoordinator: ActorRef[CoordinationEvent],
                                    ctx: ActorContext[GraphConnectorCoordinator.ConnectionCoordinationEvent]) {
  import GraphRedistributionCoordinator._

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
}
