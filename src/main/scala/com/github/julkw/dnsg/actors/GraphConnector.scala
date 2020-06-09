package com.github.julkw.dnsg.actors

import scala.concurrent.duration._
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.CoordinationEvent
import com.github.julkw.dnsg.actors.Coordinators.GraphConnectorCoordinator.{AllConnected, ConnectionCoordinationEvent, FinishedUpdatingConnectivity, GraphConnectorDistributionInfo, UnconnectedNode}
import com.github.julkw.dnsg.util.Data.LocalData
import com.github.julkw.dnsg.util.{NodeLocator, dNSGSerializable}

import scala.collection.mutable

object GraphConnector {
  sealed trait ConnectGraphEvent extends dNSGSerializable

  final case class ConnectorDistributionInfo(nodeLocator: NodeLocator[ActorRef[ConnectGraphEvent]]) extends ConnectGraphEvent

  final case class BuildTreeFrom(root: Int) extends ConnectGraphEvent

  final case class AddChild(connectedNode: Int, parent: Int) extends ConnectGraphEvent

  final case class NotYourChild(connectedNode: Int, parent: Int) extends ConnectGraphEvent

  final case class DoneConnectingChildren(connectedNode: Int, nodeAwaitingAnswer: Int) extends ConnectGraphEvent

  final case class AddEdgeAndContinue(from: Int, to: Int) extends ConnectGraphEvent

  final case class FindUnconnectedNode(sendTo: ActorRef[ConnectionCoordinationEvent], notAskedYet: Set[ActorRef[ConnectGraphEvent]]) extends ConnectGraphEvent

  final case object GraphConnected extends ConnectGraphEvent

  final case class StartGraphRedistributers(clusterCoordinator: ActorRef[CoordinationEvent]) extends ConnectGraphEvent

  // ensuring message delivery
  final case class ResendAddChild(connectedNode: Int, parent: Int) extends ConnectGraphEvent

  //final case class NowConnectingChildren(connectedNode: Int, parent: Int) extends ConnectGraphEvent

  final case class IsConnectedKey(connectedNode: Int, parent: Int)

  val timeoutAfter = 3.second

  // data structures for more readable code
  case class CTreeNode(parent: Int, children: mutable.Set[Int], awaitingAnswer: mutable.Set[Int])

  def apply(data: LocalData[Float],
            graph: Map[Int, Seq[Int]],
            supervisor: ActorRef[ConnectionCoordinationEvent]): Behavior[ConnectGraphEvent] = Behaviors.setup(ctx =>
    Behaviors.withTimers(timers =>
      new GraphConnector(data, graph, supervisor, timers, ctx).setup()
    )
  )
}

class GraphConnector(data: LocalData[Float],
                     graph: Map[Int, Seq[Int]],
                     supervisor: ActorRef[ConnectionCoordinationEvent],
                     timers: TimerScheduler[GraphConnector.ConnectGraphEvent],
                     ctx: ActorContext[GraphConnector.ConnectGraphEvent]) {
  import GraphConnector._

  def setup(): Behavior[ConnectGraphEvent] = {
    supervisor ! GraphConnectorDistributionInfo(graph.keys.toSeq, ctx.self)
    waitForDistInfo()
  }

  def waitForDistInfo(): Behavior[ConnectGraphEvent] = Behaviors.receiveMessagePartial {
    case ConnectorDistributionInfo(nodeLocator) =>
      buildTree(nodeLocator, -1, Map.empty)

    case BuildTreeFrom(root) =>
      ctx.self ! BuildTreeFrom(root)
      waitForDistInfo()

    case AddChild(connectedNode, parent) =>
      ctx.self ! AddChild(connectedNode, parent)
      waitForDistInfo()
  }

  def buildTree(nodeLocator: NodeLocator[ActorRef[ConnectGraphEvent]],
                root: Int,
                tree: Map[Int, CTreeNode]): Behavior[ConnectGraphEvent] =
    Behaviors.receiveMessagePartial {
      case BuildTreeFrom(root) =>
        ctx.log.info("Updating connectivity")
        // treat the newly connected node as out new root node
        val rootNode = updateNeighborConnectedness(root, root, root, tree, supervisor, nodeLocator)
        buildTree(nodeLocator, root, tree + (root -> rootNode))

      case AddEdgeAndContinue(from, to) =>
        ctx.log.info("Updating connectivity after adding a new edge")
        timers.startSingleTimer(IsConnectedKey(to, from), ResendAddChild(to, from), timeoutAfter)
        nodeLocator.findResponsibleActor(to) ! AddChild(to, from)
        tree(from).awaitingAnswer.add(to)
        buildTree(nodeLocator, from, tree)

      case AddChild(connectedNode, parent) =>
        if (tree.contains(connectedNode)) {
          val node = tree(connectedNode)
          if (node.parent == parent) {
            if(node.awaitingAnswer.isEmpty) {
              nodeLocator.findResponsibleActor(parent) ! DoneConnectingChildren(connectedNode, parent)
            } // else still working on it, don't respond
          } else {
            nodeLocator.findResponsibleActor(parent) ! NotYourChild(connectedNode, parent)
          }
          buildTree(nodeLocator, root, tree)
        } else {
          //nodeLocator.findResponsibleActor(parent) ! NowConnectingChildren(connectedNode, parent)
          val newNode = updateNeighborConnectedness(connectedNode, parent, root, tree, supervisor, nodeLocator)
          buildTree(nodeLocator, root, tree + (connectedNode -> newNode))
        }

      case NotYourChild(connectedNode, parent) =>
        timers.cancel(IsConnectedKey(connectedNode, parent))
        val node = tree(parent)
        node.awaitingAnswer -= connectedNode
        if (node.awaitingAnswer.isEmpty) {
          nodeLocator.findResponsibleActor(node.parent) ! DoneConnectingChildren(parent, node.parent)
        }
        buildTree(nodeLocator, root, tree)

      case ResendAddChild(connectedNode, parent) =>
        ctx.log.info("Apparently a connection message got lost. resend.")
        timers.startSingleTimer(IsConnectedKey(connectedNode, parent), ResendAddChild(connectedNode, parent), timeoutAfter)
        nodeLocator.findResponsibleActor(connectedNode) ! AddChild(connectedNode, parent)
        buildTree(nodeLocator, root, tree)

      case DoneConnectingChildren(connectedNode, parent) =>
        timers.cancel(IsConnectedKey(connectedNode, parent))
        val node = tree(parent)
        node.awaitingAnswer -= connectedNode
        node.children += connectedNode
        if (node.awaitingAnswer.isEmpty) {
          if (parent == root) {
            supervisor ! FinishedUpdatingConnectivity
          } else {
            nodeLocator.findResponsibleActor(node.parent) !
              DoneConnectingChildren(parent, node.parent)
          }
        }
        buildTree(nodeLocator, root, tree)

      case FindUnconnectedNode(sendTo, notAskedYet) =>
        val unconnectedNodes = graph.keys.toSet -- tree.keys
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
        buildTree(nodeLocator, root, tree)

      case GraphConnected =>
        Behaviors.stopped

      case StartGraphRedistributers(clusterCoordinator) =>
        ctx.spawn(GraphRedistributer(tree, clusterCoordinator), name="GraphRedistributer")
        buildTree(nodeLocator, root, tree)
    }

  def updateNeighborConnectedness(node: Int,
                                  parent: Int,
                                  root: Int,
                                  tree: Map[Int, CTreeNode],
                                  sendResultTo: ActorRef[ConnectionCoordinationEvent],
                                  nodeLocator: NodeLocator[ActorRef[ConnectGraphEvent]]): CTreeNode = {
    // tell all neighbors they are connected
    val nodeInfo = CTreeNode(parent, mutable.Set.empty, mutable.Set.empty)
    graph(node).foreach { neighborIndex =>
      if (!tree.contains(neighborIndex)) {
        timers.startSingleTimer(IsConnectedKey(neighborIndex, node), ResendAddChild(neighborIndex, node), timeoutAfter)
        nodeLocator.findResponsibleActor(neighborIndex) ! AddChild(neighborIndex, node)
        nodeInfo.awaitingAnswer.add(neighborIndex)
      }
    }
    if (nodeInfo.awaitingAnswer.isEmpty) {
      if (node == root) {
        sendResultTo ! FinishedUpdatingConnectivity
      } else {
        nodeLocator.findResponsibleActor(parent) ! DoneConnectingChildren(node, parent)
      }
    }
    nodeInfo
  }

}
