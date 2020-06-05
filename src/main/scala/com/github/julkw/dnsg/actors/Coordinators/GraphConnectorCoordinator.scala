package com.github.julkw.dnsg.actors.Coordinators

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.{ConnectionAchieved, CoordinationEvent, KNearestNeighbors}
import com.github.julkw.dnsg.actors.Coordinators.NodeCoordinator.NodeCoordinationEvent
import com.github.julkw.dnsg.actors.GraphConnector.{AddToGraph, ConnectGraphEvent, ConnectorDistributionInfo, FindUnconnectedNode, GraphConnected, UpdateConnectivity}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.{ConnectGraph, FindNearestNeighborsStartingFrom, SearchOnGraphEvent}
import com.github.julkw.dnsg.util.{NodeLocator, NodeLocatorBuilder, dNSGSerializable}

object GraphConnectorCoordinator {

  trait ConnectionCoordinationEvent extends dNSGSerializable

  final case class GraphConnectorDistributionInfo(responsibility: Seq[Int], sender: ActorRef[ConnectGraphEvent]) extends ConnectionCoordinationEvent

  final case object FinishedUpdatingConnectivity extends ConnectionCoordinationEvent

  final case class UnconnectedNode(nodeIndex: Int, nodeData: Seq[Float]) extends ConnectionCoordinationEvent

  final case object AllConnected extends ConnectionCoordinationEvent

  final case object UpdatedGraph extends ConnectionCoordinationEvent

  final case class WrappedCoordinationEvent(event: CoordinationEvent) extends ConnectionCoordinationEvent

  def apply(navigatingNodeIndex: Int,
            graphNodeLocator: NodeLocator[ActorRef[SearchOnGraphEvent]],
            clusterCoordinator: ActorRef[CoordinationEvent]): Behavior[ConnectionCoordinationEvent] =
    Behaviors.setup { ctx =>
      val coordinationEventAdapter: ActorRef[CoordinationEvent] =
        ctx.messageAdapter { event => WrappedCoordinationEvent(event)}

      new GraphConnectorCoordinator(navigatingNodeIndex, graphNodeLocator, clusterCoordinator, coordinationEventAdapter, ctx).setup()
    }

}

class GraphConnectorCoordinator(navigatingNodeIndex: Int,
                                graphNodeLocator: NodeLocator[ActorRef[SearchOnGraphEvent]],
                                clusterCoordinator: ActorRef[CoordinationEvent],
                                coordinationEventAdapter: ActorRef[CoordinationEvent],
                                ctx: ActorContext[GraphConnectorCoordinator.ConnectionCoordinationEvent]) {
  import GraphConnectorCoordinator._

  def setup(): Behavior[ConnectionCoordinationEvent] = {
    graphNodeLocator.allActors().foreach( graphHolder => graphHolder ! ConnectGraph(ctx.self))
    // TODO tell all graphHolders to start connecting the graph
    waitForGraphConnectors(Set.empty, NodeLocatorBuilder(graphNodeLocator.graphSize))
  }
  def waitForGraphConnectors(graphConnectors: Set[ActorRef[ConnectGraphEvent]],
                             graphConnectorLocator: NodeLocatorBuilder[ActorRef[ConnectGraphEvent]]): Behavior[ConnectionCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case GraphConnectorDistributionInfo(responsibility, graphConnector) =>
        val updatedConnectors = graphConnectors + graphConnector
        val gcLocator = graphConnectorLocator.addLocation(responsibility, graphConnector)
        gcLocator match {
          case Some(newLocator) =>
            updatedConnectors.foreach(connector => connector ! ConnectorDistributionInfo(newLocator))
            newLocator.findResponsibleActor(navigatingNodeIndex) ! UpdateConnectivity(navigatingNodeIndex)
            connectNSG(newLocator, updatedConnectors, -1)
          case None =>
            waitForGraphConnectors(updatedConnectors, graphConnectorLocator)
        }
    }

  def connectNSG(connectorLocator: NodeLocator[ActorRef[ConnectGraphEvent]],
                 graphConnectors: Set[ActorRef[ConnectGraphEvent]],
                 latestUnconnectedNodeIndex: Int) : Behavior[ConnectionCoordinationEvent] =
    Behaviors.receiveMessagePartial{
      case FinishedUpdatingConnectivity =>
        // find out if there is still an unconnected node and connect it
        val startingNode = graphConnectors.head
        startingNode ! FindUnconnectedNode(ctx.self, graphConnectors)
        connectNSG(connectorLocator, graphConnectors, latestUnconnectedNodeIndex)

      case UnconnectedNode(nodeIndex, nodeData) =>
        ctx.log.info("found an unconnected node")
        graphNodeLocator.findResponsibleActor(nodeIndex) !
          FindNearestNeighborsStartingFrom(nodeData, navigatingNodeIndex, 1, coordinationEventAdapter)
        connectNSG(connectorLocator, graphConnectors, nodeIndex)

      case WrappedCoordinationEvent(event) =>
        event match {
          case KNearestNeighbors(query, neighbors) =>
            // Right now the only query being asked for is to connect unconnected nodes
            connectorLocator.findResponsibleActor(neighbors.head) ! AddToGraph(neighbors.head, latestUnconnectedNodeIndex)
            connectorLocator.findResponsibleActor(latestUnconnectedNodeIndex) ! UpdateConnectivity(latestUnconnectedNodeIndex)
            connectNSG(connectorLocator, graphConnectors, -1)
        }

      case AllConnected =>
        ctx.log.info("NSG now fully connected")
        graphConnectors.foreach(graphConnector => graphConnector ! GraphConnected)
        waitForUpdatedGraphs(graphNodeLocator.allActors().size)
    }

  def waitForUpdatedGraphs(remainingGraphsToUpdate: Int): Behavior[ConnectionCoordinationEvent] = Behaviors.receiveMessagePartial {
    case UpdatedGraph =>
      if (remainingGraphsToUpdate == 1) {
        clusterCoordinator ! ConnectionAchieved
        Behaviors.stopped
      } else {
        waitForUpdatedGraphs(remainingGraphsToUpdate - 1)
      }

  }
}
