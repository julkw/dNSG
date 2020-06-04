package com.github.julkw.dnsg.actors

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.ClusterCoordinator.{AllConnected, CoordinationEvent, FinishedUpdatingConnectivity, GraphConnectorDistributionInfo, UnconnectedNode}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.{SearchOnGraphEvent, UpdateGraph}
import com.github.julkw.dnsg.util.Data.CacheData
import com.github.julkw.dnsg.util.{NodeLocator, dNSGSerializable}

import scala.collection.mutable

object GraphConnector {
  sealed trait ConnectGraphEvent extends dNSGSerializable

  final case class GraphToConnect(graph: Map[Int, Seq[Int]]) extends ConnectGraphEvent

  final case class ConnectorDistributionInfo(nodeLocator: NodeLocator[ActorRef[ConnectGraphEvent]]) extends ConnectGraphEvent

  final case class UpdateConnectivity(root: Int) extends ConnectGraphEvent

  final case class IsConnected(connectedNode: Int, parent: Int) extends ConnectGraphEvent

  final case class DoneConnectingChildren(nodeAwaitingAnswer: Int) extends ConnectGraphEvent

  final case class FindUnconnectedNode(sendTo: ActorRef[CoordinationEvent], notAskedYet: Set[ActorRef[ConnectGraphEvent]]) extends ConnectGraphEvent

  final case object GraphConnected extends ConnectGraphEvent

  final case class AddToGraph(startNode: Int, endNode: Int) extends ConnectGraphEvent

  final case object UpdatedGraphReceived extends ConnectGraphEvent

  // data structures for more readable code
  protected case class MessageCounter(var waitingForMessages: Int, parentNode: Int)

  protected case class ConnectivityInfo(connectedNodes: mutable.Set[Int], messageTracker: mutable.Map[Int, MessageCounter])

  def apply(data: CacheData[Float],
            clusterCoordinator: ActorRef[CoordinationEvent],
            searchOnGraphActor: ActorRef[SearchOnGraphEvent]): Behavior[ConnectGraphEvent] = Behaviors.setup(ctx =>
    new GraphConnector(data, clusterCoordinator, searchOnGraphActor, ctx).setup()
  )

}

class GraphConnector(data: CacheData[Float],
                     clusterCoordinator: ActorRef[CoordinationEvent],
                     searchOnGraphActor: ActorRef[SearchOnGraphEvent],
                     ctx: ActorContext[GraphConnector.ConnectGraphEvent]) {
  import GraphConnector._

  def setup(): Behavior[ConnectGraphEvent] = Behaviors.receiveMessagePartial {
    case GraphToConnect(graph) =>
      clusterCoordinator ! GraphConnectorDistributionInfo(graph.keys.toSeq, ctx.self)
      waitForDistInfo(graph)
  }

  def waitForDistInfo(graph: Map[Int, Seq[Int]]): Behavior[ConnectGraphEvent] = Behaviors.receiveMessagePartial {
    case ConnectorDistributionInfo(nodeLocator) =>
      establishConnectivity(graph, nodeLocator, ConnectivityInfo(mutable.Set.empty, mutable.Map.empty))

    case UpdateConnectivity(root) =>
      // TODO this is an ugly way to deal with the case that this message arrives here before the DistributionInfo
      ctx.self ! UpdateConnectivity(root)
      Behaviors.same
  }

  def establishConnectivity(graph: Map[Int, Seq[Int]],
                            nodeLocator: NodeLocator[ActorRef[ConnectGraphEvent]],
                            connectivityInfo: ConnectivityInfo): Behavior[ConnectGraphEvent] =
    Behaviors.receiveMessagePartial {
      case UpdateConnectivity(root) =>
        ctx.log.info("Updating connectivity")
        connectivityInfo.connectedNodes.add(root)
        updateNeighborConnectedness(root, root, connectivityInfo, graph, clusterCoordinator, nodeLocator)
        establishConnectivity(graph, nodeLocator, connectivityInfo)

      case IsConnected(connectedNode, parent) =>
        // in case the parent is placed on another node this might not be known here
        connectivityInfo.connectedNodes.add(parent)
        if (connectivityInfo.connectedNodes.contains(connectedNode)) {
          nodeLocator.findResponsibleActor(parent) ! DoneConnectingChildren(parent)
        } else {
          connectivityInfo.connectedNodes.add(connectedNode)
          updateNeighborConnectedness(connectedNode, parent, connectivityInfo, graph, clusterCoordinator, nodeLocator)
        }
        establishConnectivity(graph, nodeLocator, connectivityInfo)

      case DoneConnectingChildren(nodeAwaitingAnswer) =>
        val messageCounter = connectivityInfo.messageTracker(nodeAwaitingAnswer)
        messageCounter.waitingForMessages -= 1
        if (messageCounter.waitingForMessages == 0) {
          if (messageCounter.parentNode == nodeAwaitingAnswer) {
            clusterCoordinator ! FinishedUpdatingConnectivity
            ctx.log.info("Done with updating connectivity")
          } else {
            nodeLocator.findResponsibleActor(messageCounter.parentNode) !
              DoneConnectingChildren(messageCounter.parentNode)
            connectivityInfo.messageTracker -= nodeAwaitingAnswer
          }
        }
        establishConnectivity(graph, nodeLocator, connectivityInfo)

      case FindUnconnectedNode(sendTo, notAskedYet) =>
        val unconnectedNodes = graph.keys.toSet -- connectivityInfo.connectedNodes
        if (unconnectedNodes.isEmpty) {
          // no unconnected nodes in this actor, ask others
          val yetToAsk = notAskedYet - ctx.self
          if (yetToAsk.nonEmpty) {
            yetToAsk.head ! FindUnconnectedNode(sendTo, yetToAsk)
          } else {
            // there are no unconnected nodes
            sendTo ! AllConnected
          }
        } else {
          // send one of the unconnected nodes
          sendTo ! UnconnectedNode(unconnectedNodes.head, data.get(unconnectedNodes.head))
        }
        establishConnectivity(graph, nodeLocator, connectivityInfo)

      case AddToGraph(startNode, endNode) =>
        ctx.log.info("Add edge to graph to ensure connectivity")
        val updatedNeighbors = graph(startNode) :+ endNode
        establishConnectivity(graph + (startNode -> updatedNeighbors), nodeLocator, connectivityInfo)

      case GraphConnected =>
        searchOnGraphActor ! UpdateGraph(graph, ctx.self)
        waitForShutdown()
    }

  def waitForShutdown(): Behavior[ConnectGraphEvent] = Behaviors.receiveMessagePartial {
    case UpdatedGraphReceived =>
      Behaviors.stopped
  }

  def updateNeighborConnectedness(node: Int,
                                  parent: Int,
                                  connectivityInfo: ConnectivityInfo,
                                  graph: Map[Int, Seq[Int]],
                                  sendResultTo: ActorRef[CoordinationEvent],
                                  nodeLocator: NodeLocator[ActorRef[ConnectGraphEvent]]): Unit = {
    var sendMessages = 0
    // tell all neighbors they are connected
    graph(node).foreach { neighborIndex =>
      if (!connectivityInfo.connectedNodes.contains(neighborIndex)) {
        nodeLocator.findResponsibleActor(neighborIndex) ! IsConnected(neighborIndex, node)
        sendMessages += 1
      }
    }
    if (sendMessages > 0) {
      connectivityInfo.messageTracker += (node -> MessageCounter(sendMessages, parent))
    } else if (node != parent) {
      nodeLocator.findResponsibleActor(parent) ! DoneConnectingChildren(parent)
    } else { // no neighbors updated and this is the root
      sendResultTo ! FinishedUpdatingConnectivity
      ctx.log.info("None of the previously unconnected nodes are connected to the root")
    }
  }

}
