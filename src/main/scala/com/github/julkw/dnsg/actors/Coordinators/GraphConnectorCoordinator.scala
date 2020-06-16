package com.github.julkw.dnsg.actors.Coordinators

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.{ConnectionAchieved, ConnectorsCleanedUp, CoordinationEvent, KNearestNeighbors}
import com.github.julkw.dnsg.actors.Coordinators.GraphRedistributionCoordinator.RedistributionCoordinationEvent
import com.github.julkw.dnsg.actors.GraphConnector.{AddEdgeAndContinue, BuildTreeFrom, ConnectGraphEvent, ConnectorDistributionInfo, FindUnconnectedNode, GraphConnected, StartGraphRedistributers}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.{AddToGraph, ConnectGraph, FindNearestNeighborsStartingFrom, SearchOnGraphEvent}
import com.github.julkw.dnsg.util.{NodeLocator, NodeLocatorBuilder, dNSGSerializable}

object GraphConnectorCoordinator {

  trait ConnectionCoordinationEvent extends dNSGSerializable

  final case class GraphConnectorDistributionInfo(responsibility: Seq[Int], sender: ActorRef[ConnectGraphEvent]) extends ConnectionCoordinationEvent

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
            supervisor: ActorRef[CoordinationEvent]): Behavior[ConnectionCoordinationEvent] =
    Behaviors.setup { ctx =>
      val coordinationEventAdapter: ActorRef[CoordinationEvent] =
        ctx.messageAdapter { event => WrappedCoordinationEvent(event)}

      new GraphConnectorCoordinator(navigatingNodeIndex, graphNodeLocator, supervisor, coordinationEventAdapter, ctx).setup()
    }

}

class GraphConnectorCoordinator(navigatingNodeIndex: Int,
                                graphNodeLocator: NodeLocator[SearchOnGraphEvent],
                                supervisor: ActorRef[CoordinationEvent],
                                coordinationEventAdapter: ActorRef[CoordinationEvent],
                                ctx: ActorContext[GraphConnectorCoordinator.ConnectionCoordinationEvent]) {
  import GraphConnectorCoordinator._

  def setup(): Behavior[ConnectionCoordinationEvent] = {
    graphNodeLocator.allActors.foreach( graphHolder => graphHolder ! ConnectGraph(ctx.self))
    waitForGraphConnectors(Set.empty, NodeLocatorBuilder(graphNodeLocator.graphSize))
  }

  def waitForGraphConnectors(graphConnectors: Set[ActorRef[ConnectGraphEvent]],
                             graphConnectorLocator: NodeLocatorBuilder[ConnectGraphEvent]): Behavior[ConnectionCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case GraphConnectorDistributionInfo(responsibility, graphConnector) =>
        ctx.log.info("Got DistributionInfo from a graphConnector for {} nodes", responsibility)
        val updatedConnectors = graphConnectors + graphConnector
        val gcLocator = graphConnectorLocator.addLocation(responsibility, graphConnector)
        gcLocator match {
          case Some(newLocator) =>
            updatedConnectors.foreach(connector => connector ! ConnectorDistributionInfo(newLocator))
            newLocator.findResponsibleActor(navigatingNodeIndex) ! BuildTreeFrom(navigatingNodeIndex)
            connectGraph(newLocator, updatedConnectors,0, -1, false)
          case None =>
            waitForGraphConnectors(updatedConnectors, graphConnectorLocator)
        }
    }

  def connectGraph(connectorLocator: NodeLocator[ConnectGraphEvent],
                   graphConnectors: Set[ActorRef[ConnectGraphEvent]],
                   waitOnNodeAck: Int,
                   latestUnconnectedNodeIndex: Int,
                   allConnected: Boolean) : Behavior[ConnectionCoordinationEvent] =
    Behaviors.receiveMessagePartial{
      case FinishedUpdatingConnectivity =>
        ctx.log.info("Updated Connectivity, look for unconnected node")
        // find out if there is still an unconnected node and connect it
        val startingNode = graphConnectors.head
        startingNode ! FindUnconnectedNode(ctx.self, graphConnectors)
        connectGraph(connectorLocator, graphConnectors, waitOnNodeAck, latestUnconnectedNodeIndex, allConnected)

      case UnconnectedNode(nodeIndex, nodeData) =>
        ctx.log.info("Unconnected node found: {}", nodeIndex)
        graphNodeLocator.findResponsibleActor(nodeIndex) !
          FindNearestNeighborsStartingFrom(nodeData, navigatingNodeIndex, 1, coordinationEventAdapter)
        connectGraph(connectorLocator, graphConnectors, waitOnNodeAck, nodeIndex, allConnected)

      case WrappedCoordinationEvent(event) =>
        event match {
          case KNearestNeighbors(query, neighbors) =>
            // the only query being asked for is to connect unconnected nodes
            assert(latestUnconnectedNodeIndex >= 0)
            assert(neighbors.head != latestUnconnectedNodeIndex)
            graphNodeLocator.findResponsibleActor(neighbors.head) ! AddToGraph(neighbors.head, latestUnconnectedNodeIndex, ctx.self)
            connectorLocator.findResponsibleActor(neighbors.head) ! AddEdgeAndContinue(neighbors.head, latestUnconnectedNodeIndex)
            connectGraph(connectorLocator, graphConnectors, waitOnNodeAck + 1, -1, allConnected)
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
        connectGraph(connectorLocator, graphConnectors, waitOnNodeAck, latestUnconnectedNodeIndex, true)

      case StartGraphRedistribution(redistributionCoordinator) =>
        graphConnectors.foreach(graphConnector => graphConnector ! StartGraphRedistributers(redistributionCoordinator))
        connectGraph(connectorLocator, graphConnectors, waitOnNodeAck, latestUnconnectedNodeIndex, allConnected)

      case CleanUpConnectors =>
        ctx.log.info("Tell all connectors to shut themselves down")
        graphConnectors.foreach(graphConnector => graphConnector ! GraphConnected)
        cleanup(graphConnectors.size)
    }

  def cleanup(waitOnConnectors: Int): Behavior[ConnectionCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case ConnectorShutdown =>
        if (waitOnConnectors == 1) {
          supervisor ! ConnectorsCleanedUp
          Behaviors.stopped
        } else {
          cleanup(waitOnConnectors - 1)
        }
    }
}
