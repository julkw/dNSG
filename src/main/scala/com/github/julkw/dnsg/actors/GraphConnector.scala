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

  // work pulling
  final case class GetConnectivityInfo(sender: ActorRef[ConnectGraphEvent]) extends ConnectGraphEvent

  final case class AddChildren(connectedNodes: Seq[TreeEdge], sender: ActorRef[ConnectGraphEvent]) extends ConnectGraphEvent

  final case class NotYourChildren(connectedNodes: Seq[TreeEdge], sender: ActorRef[ConnectGraphEvent]) extends ConnectGraphEvent

  final case class DoneConnectingChildren(connectedNodes: Seq[TreeEdge], sender: ActorRef[ConnectGraphEvent]) extends ConnectGraphEvent


  final case class AddEdgeAndContinue(from: Int, to: Int) extends ConnectGraphEvent

  final case class FindUnconnectedNode(sendTo: ActorRef[ConnectionCoordinationEvent], notAskedYet: Set[ActorRef[ConnectGraphEvent]]) extends ConnectGraphEvent

  final case object GraphConnected extends ConnectGraphEvent

  final case class StartGraphRedistributers(clusterCoordinator: ActorRef[CoordinationEvent]) extends ConnectGraphEvent

  // ensuring message delivery
  final case class ResendAddChild(connectedNode: Int, parent: Int) extends ConnectGraphEvent

  final case class IsConnectedKey(connectedNode: Int, parent: Int)

  val timeoutAfter = 3.second

  // data structures for more readable code
  case class CTreeNode(parent: Int, children: mutable.Set[Int], awaitingAnswer: mutable.Set[Int])

  case class TreeEdge(child: Int, parent: Int)

  case class ConnectivityInformation(var addChildren: Seq[TreeEdge], var notChildren: Seq[TreeEdge], var doneChildren: Seq[TreeEdge])

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
      val allConnectors = nodeLocator.allActors()
      allConnectors.foreach(connector => connector ! GetConnectivityInfo(ctx.self))
      val toSend = allConnectors.map(connector => connector -> ConnectivityInformation(Seq.empty, Seq.empty, Seq.empty)).toMap
      buildTree(nodeLocator, -1, Map.empty, toSend)

    case GetConnectivityInfo(sender) =>
      ctx.self ! GetConnectivityInfo(sender)
      waitForDistInfo()

    case BuildTreeFrom(root) =>
      ctx.self ! BuildTreeFrom(root)
      waitForDistInfo()
  }

  def buildTree(nodeLocator: NodeLocator[ActorRef[ConnectGraphEvent]],
                root: Int,
                tree: Map[Int, CTreeNode],
                toSend: Map[ActorRef[ConnectGraphEvent], ConnectivityInformation]): Behavior[ConnectGraphEvent] =
    Behaviors.receiveMessagePartial {
      case BuildTreeFrom(root) =>
        ctx.log.info("Updating connectivity")
        // treat the newly connected node as out new root node
        val rootNode = updateNeighborConnectedness(TreeEdge(root, root), root, tree, supervisor, nodeLocator, toSend)
        buildTree(nodeLocator, root, tree + (root -> rootNode), toSend)

      case AddEdgeAndContinue(from, to) =>
        ctx.log.info("Updating connectivity after adding a new edge")
        timers.startSingleTimer(IsConnectedKey(to, from), ResendAddChild(to, from), timeoutAfter)
        nodeLocator.findResponsibleActor(to) ! AddChildren(Seq(TreeEdge(to, from)), ctx.self)
        tree(from).awaitingAnswer.add(to)
        buildTree(nodeLocator, from, tree, toSend)

      case GetConnectivityInfo(sender) =>
        val connectivityInfo = toSend(sender)
        sender ! AddChildren(connectivityInfo.addChildren, ctx.self)
        sender ! NotYourChildren(connectivityInfo.notChildren, ctx.self)
        sender ! DoneConnectingChildren(connectivityInfo.doneChildren, ctx.self)
        buildTree(nodeLocator, root, tree, toSend + (sender -> ConnectivityInformation(Seq.empty, Seq.empty, Seq.empty)))

      case AddChildren(connectedNodes, sender) =>
        val (knownNodes, unknownNodes) = connectedNodes.partition(treeEdge => tree.contains(treeEdge.child))
        knownNodes.foreach { treeEdge =>
          val node = tree(treeEdge.child)
          if (node.parent == treeEdge.parent) {
            if(node.awaitingAnswer.isEmpty) {
              toSend(nodeLocator.findResponsibleActor(treeEdge.parent)).doneChildren :+= treeEdge
            } // else still working on it, don't respond
          } else {
            toSend(nodeLocator.findResponsibleActor(treeEdge.parent)).notChildren :+= treeEdge
          }
        }
        val newNodes = unknownNodes.map { treeEdge =>
          val newNode = updateNeighborConnectedness(treeEdge, root, tree, supervisor, nodeLocator, toSend)
          treeEdge.child -> newNode
        }
        buildTree(nodeLocator, root, tree ++ newNodes, toSend)

      case NotYourChildren(connectedNodes, sender) =>
        connectedNodes.foreach { treeEdge =>
          val node = tree(treeEdge.parent)
          node.awaitingAnswer -= treeEdge.child
          if (node.awaitingAnswer.isEmpty) {
            toSend(nodeLocator.findResponsibleActor(node.parent)).doneChildren :+= TreeEdge(treeEdge.parent, node.parent)
          }
        }
        buildTree(nodeLocator, root, tree, toSend)

      case DoneConnectingChildren(connectedNodes, sender) =>
        connectedNodes.foreach { treeEdge =>
          val node = tree(treeEdge.parent)
          node.awaitingAnswer -= treeEdge.child
          node.children += treeEdge.child
          if (node.awaitingAnswer.isEmpty) {
            if (treeEdge.parent == root) {
              supervisor ! FinishedUpdatingConnectivity
            } else {
              toSend(nodeLocator.findResponsibleActor(node.parent)).doneChildren :+= TreeEdge(treeEdge.parent, node.parent)
            }
          }
        }
        // TODO Use timer if this was empty? Put all three messages into one?
        sender ! GetConnectivityInfo(ctx.self)
        buildTree(nodeLocator, root, tree, toSend)
/*
      case ResendAddChild(connectedNode, parent) =>
        ctx.log.info("Apparently a connection message got lost. resend.")
        timers.startSingleTimer(IsConnectedKey(connectedNode, parent), ResendAddChild(connectedNode, parent), timeoutAfter)
        nodeLocator.findResponsibleActor(connectedNode) ! AddChild(connectedNode, parent)
        buildTree(nodeLocator, root, tree,toSend)
 */
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
        buildTree(nodeLocator, root, tree, toSend)

        // TODO with work pulling this needs to be in another state or I get an infinte loop of asking for nothing
      case GraphConnected =>
        Behaviors.stopped

      case StartGraphRedistributers(clusterCoordinator) =>
        ctx.spawn(GraphRedistributer(tree, clusterCoordinator), name="GraphRedistributer")
        buildTree(nodeLocator, root, tree, toSend)
    }

  def updateNeighborConnectedness(newEdge: TreeEdge,
                                  root: Int,
                                  tree: Map[Int, CTreeNode],
                                  sendResultTo: ActorRef[ConnectionCoordinationEvent],
                                  nodeLocator: NodeLocator[ActorRef[ConnectGraphEvent]],
                                  toSend: Map[ActorRef[ConnectGraphEvent], ConnectivityInformation]): CTreeNode = {
    // tell all neighbors they are connected
    val nodeInfo = CTreeNode(newEdge.parent, mutable.Set.empty, mutable.Set.empty)
    graph(newEdge.child).foreach { neighborIndex =>
      if (!tree.contains(neighborIndex)) {
        //timers.startSingleTimer(IsConnectedKey(neighborIndex, newEdge.child), ResendAddChild(neighborIndex, newEdge.child), timeoutAfter)
        toSend(nodeLocator.findResponsibleActor(neighborIndex)).addChildren :+= TreeEdge(neighborIndex, newEdge.child)
        nodeInfo.awaitingAnswer.add(neighborIndex)
      }
    }
    if (nodeInfo.awaitingAnswer.isEmpty) {
      if (newEdge.child == root) {
        sendResultTo ! FinishedUpdatingConnectivity
      } else {
        toSend(nodeLocator.findResponsibleActor(newEdge.parent)).addChildren :+= newEdge
      }
    }
    nodeInfo
  }

}
