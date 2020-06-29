package com.github.julkw.dnsg.actors.Coordinators

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.{ConnectionAchieved, ConnectorsCleanedUp, CoordinationEvent, KNearestNeighbors}
import com.github.julkw.dnsg.actors.Coordinators.GraphRedistributionCoordinator.RedistributionCoordinationEvent
import com.github.julkw.dnsg.actors.GraphConnector.{AddEdgeAndContinue, BuildTreeFrom, ConnectGraphEvent, FindUnconnectedNode, GraphConnected, StartGraphRedistributers}
import com.github.julkw.dnsg.actors.NodeLocatorHolder.{BuildConnectorNodeLocator, NodeLocationEvent, ShareNodeLocator}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.{AddToGraph, ConnectGraph, FindNearestNeighborsStartingFrom, SearchOnGraphEvent}
import com.github.julkw.dnsg.util.{LocalityCheck, NodeLocator, Settings, dNSGSerializable}

object GraphConnectorCoordinator {

  trait ConnectionCoordinationEvent extends dNSGSerializable

  final case object FinishedGraphConnectorNodeLocator extends ConnectionCoordinationEvent

  final case class GraphConnectorNodeLocator(nodeLocator: NodeLocator[ConnectGraphEvent]) extends ConnectionCoordinationEvent

  final case object FinishedUpdatingConnectivity extends ConnectionCoordinationEvent

  final case class UnconnectedNode(nodeIndex: Int, nodeData: Seq[Float]) extends ConnectionCoordinationEvent

  final case object ReceivedNewEdge extends ConnectionCoordinationEvent

  final case object AllConnected extends ConnectionCoordinationEvent

  final case class StartGraphRedistribution(redistributionCoordinator: ActorRef[RedistributionCoordinationEvent]) extends ConnectionCoordinationEvent

  final case object CleanUpConnectors extends ConnectionCoordinationEvent

  final case object ConnectorShutdown extends ConnectionCoordinationEvent

  final case class WrappedCoordinationEvent(event: CoordinationEvent) extends ConnectionCoordinationEvent

  def apply(navigatingNodeIndex: Int,
            graphNodeLocator: NodeLocator[SearchOnGraphEvent],
            nodeLocatorHolders: Set[ActorRef[NodeLocationEvent]],
            supervisor: ActorRef[CoordinationEvent]): Behavior[ConnectionCoordinationEvent] =
    Behaviors.setup { ctx =>
      val coordinationEventAdapter: ActorRef[CoordinationEvent] =
        ctx.messageAdapter { event => WrappedCoordinationEvent(event)}

      val candidateQueueSize = Settings(ctx.system.settings.config).candidateQueueSizeKnng
      new GraphConnectorCoordinator(navigatingNodeIndex, candidateQueueSize, graphNodeLocator, nodeLocatorHolders, supervisor, coordinationEventAdapter, ctx).setup()
    }

}

class GraphConnectorCoordinator(navigatingNodeIndex: Int,
                                candidateQueueSize: Int,
                                graphNodeLocator: NodeLocator[SearchOnGraphEvent],
                                nodeLocatorHolders: Set[ActorRef[NodeLocationEvent]],
                                supervisor: ActorRef[CoordinationEvent],
                                coordinationEventAdapter: ActorRef[CoordinationEvent],
                                ctx: ActorContext[GraphConnectorCoordinator.ConnectionCoordinationEvent]) extends LocalityCheck {
  import GraphConnectorCoordinator._

  def setup(): Behavior[ConnectionCoordinationEvent] = {
    nodeLocatorHolders.foreach(nodeLocatorHolder => nodeLocatorHolder ! BuildConnectorNodeLocator(ctx.self))
    graphNodeLocator.allActors.foreach( graphHolder => graphHolder ! ConnectGraph(ctx.self))
    waitForGraphConnectorNodeLocator(nodeLocatorHolders.size)
  }

  def waitForGraphConnectorNodeLocator(waitingOnNodeLocatorHolders: Int): Behavior[ConnectionCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case FinishedGraphConnectorNodeLocator =>
        if (waitingOnNodeLocatorHolders == 1) {
          nodeLocatorHolders.foreach(nodeLocatorHolder => nodeLocatorHolder ! ShareNodeLocator(isLocal(nodeLocatorHolder)))
        }
        waitForGraphConnectorNodeLocator(waitingOnNodeLocatorHolders - 1)

      case GraphConnectorNodeLocator(nodeLocator) =>
        nodeLocator.findResponsibleActor(navigatingNodeIndex) ! BuildTreeFrom(navigatingNodeIndex)
        connectGraph(nodeLocator, nodeLocator.allActors, waitOnNodeAck = 0, latestUnconnectedNodeIndex = -1, allConnected = false)
    }

  def connectGraph(connectorLocator: NodeLocator[ConnectGraphEvent],
                   graphConnectors: Set[ActorRef[ConnectGraphEvent]],
                   waitOnNodeAck: Int,
                   latestUnconnectedNodeIndex: Int,
                   allConnected: Boolean) : Behavior[ConnectionCoordinationEvent] =
    Behaviors.receiveMessagePartial{
      case FinishedUpdatingConnectivity =>
        // find out if there is still an unconnected node and connect it
        val startingNode = graphConnectors.head
        startingNode ! FindUnconnectedNode(ctx.self, graphConnectors)
        connectGraph(connectorLocator, graphConnectors, waitOnNodeAck, latestUnconnectedNodeIndex, allConnected)

      case UnconnectedNode(nodeIndex, nodeData) =>
        graphNodeLocator.findResponsibleActor(nodeIndex) !
          FindNearestNeighborsStartingFrom(nodeData, navigatingNodeIndex, candidateQueueSize, coordinationEventAdapter)
        connectGraph(connectorLocator, graphConnectors, waitOnNodeAck, nodeIndex, allConnected)

      case WrappedCoordinationEvent(event) =>
        event match {
          case KNearestNeighbors(query, neighbors) =>
            // the only query being asked for is to connect unconnected nodes
            assert(latestUnconnectedNodeIndex >= 0)
            assert(neighbors.head != latestUnconnectedNodeIndex)
            ctx.log.info("Add edge to graph to ensure connectivity: {} to {}", neighbors.head, latestUnconnectedNodeIndex)
            graphNodeLocator.findResponsibleActor(neighbors.head) ! AddToGraph(neighbors.head, latestUnconnectedNodeIndex, ctx.self)
            connectorLocator.findResponsibleActor(neighbors.head) ! AddEdgeAndContinue(neighbors.head, latestUnconnectedNodeIndex)
            connectGraph(connectorLocator, graphConnectors, waitOnNodeAck + 1, latestUnconnectedNodeIndex = -1, allConnected)
        }

      case ReceivedNewEdge =>
        if (allConnected && waitOnNodeAck == 1) {
          supervisor ! ConnectionAchieved
        }
        connectGraph(connectorLocator, graphConnectors, waitOnNodeAck - 1, latestUnconnectedNodeIndex, allConnected)

      case AllConnected =>
        ctx.log.info("Graph now fully connected")
        if (waitOnNodeAck == 0) {
          supervisor ! ConnectionAchieved
        }
        connectGraph(connectorLocator, graphConnectors, waitOnNodeAck, latestUnconnectedNodeIndex, allConnected = true)

      case StartGraphRedistribution(redistributionCoordinator) =>
        graphConnectors.foreach(graphConnector => graphConnector ! StartGraphRedistributers(redistributionCoordinator, graphNodeLocator))
        connectGraph(connectorLocator, graphConnectors, waitOnNodeAck, latestUnconnectedNodeIndex, allConnected)

      case CleanUpConnectors =>
        graphConnectors.foreach(graphConnector => graphConnector ! GraphConnected)
        cleanup(graphConnectors.size)
    }

  def cleanup(waitOnConnectors: Int): Behavior[ConnectionCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case ConnectorShutdown =>
        if (waitOnConnectors == 1) {
          ctx.log.info("Connectors now shutdown")
          supervisor ! ConnectorsCleanedUp
          Behaviors.stopped
        } else {
          cleanup(waitOnConnectors - 1)
        }
    }
}
