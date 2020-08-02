package com.github.julkw.dnsg.actors

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.Coordinators.GraphConnectorCoordinator.{AllConnected, ConnectionCoordinationEvent, ConnectorShutdown, FinishedUpdatingConnectivity, UnconnectedNode}
import com.github.julkw.dnsg.actors.Coordinators.GraphRedistributionCoordinator.RedistributionCoordinationEvent
import com.github.julkw.dnsg.actors.NodeLocatorHolder.{GraphConnectorGotGraphFrom, NodeLocationEvent}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.SearchOnGraphEvent
import com.github.julkw.dnsg.util.Data.LocalData
import com.github.julkw.dnsg.util.{NodeLocator, Settings, dNSGSerializable}

import scala.collection.mutable

object GraphConnector {
  sealed trait ConnectGraphEvent extends dNSGSerializable

  final case class ConnectorDistributionInfo(nodeLocator: NodeLocator[ConnectGraphEvent]) extends ConnectGraphEvent

  final case class BuildTreeFrom(root: Int) extends ConnectGraphEvent

  // work pulling
  final case class GetConnectivityInfo(sender: ActorRef[ConnectGraphEvent]) extends ConnectGraphEvent

  final case class ConnectivityInfo(connectivityInfo: ConnectivityInformation, sender: ActorRef[ConnectGraphEvent]) extends ConnectGraphEvent

  final case class AddEdgeAndContinue(from: Int, to: Int) extends ConnectGraphEvent

  final case class FindUnconnectedNode(sendTo: ActorRef[ConnectionCoordinationEvent], notAskedYet: Set[ActorRef[ConnectGraphEvent]]) extends ConnectGraphEvent

  final case object GraphConnected extends ConnectGraphEvent

  final case class StartGraphRedistributers(redistributionCoordinator: ActorRef[RedistributionCoordinationEvent], graphNodeLocator: NodeLocator[SearchOnGraphEvent]) extends ConnectGraphEvent

  // data structures for more readable code
  case class CTreeNode(parent: Int, children: mutable.Set[Int], awaitingAnswer: mutable.Set[Int])

  case class TreeEdge(child: Int, parent: Int)

  case class ConnectivityInformation(var addChildren: Seq[TreeEdge], var notChildren: Seq[TreeEdge], var doneChildren: Seq[TreeEdge]) {
    def nothingToSend(): Boolean = {addChildren.isEmpty && notChildren.isEmpty && doneChildren.isEmpty}
    def somethingToSend(): Boolean = {!nothingToSend()}
  }

  case class SendInformation(connectivityInformation: ConnectivityInformation, var sendImmediately: Boolean)

  def apply(data: LocalData[Float],
            graph: Map[Int, Seq[Int]],
            responsibility: Seq[Int],
            supervisor: ActorRef[ConnectionCoordinationEvent],
            nodeLocatorHolder: ActorRef[NodeLocationEvent],
            parent: ActorRef[SearchOnGraphEvent]): Behavior[ConnectGraphEvent] = Behaviors.setup { ctx =>
    // three seqs with each element containing two Ints -> limit each seq's size by maxMessageSize/6
    val messageSize = Settings(ctx.system.settings.config).maxMessageSize / 6
    Behaviors.withTimers(timers =>
      new GraphConnector(data, graph, responsibility, messageSize, supervisor, nodeLocatorHolder, parent, ctx).setup()
    )
  }
}

class GraphConnector(data: LocalData[Float],
                     graph: Map[Int, Seq[Int]],
                     responsibility: Seq[Int],
                     messageSize: Int,
                     supervisor: ActorRef[ConnectionCoordinationEvent],
                     nodeLocatorHolder: ActorRef[NodeLocationEvent],
                     parent: ActorRef[SearchOnGraphEvent],
                     ctx: ActorContext[GraphConnector.ConnectGraphEvent]) {
  import GraphConnector._

  def setup(): Behavior[ConnectGraphEvent] = {
    nodeLocatorHolder ! GraphConnectorGotGraphFrom(parent, ctx.self)
    waitForDistInfo()
  }

  def waitForDistInfo(): Behavior[ConnectGraphEvent] = Behaviors.receiveMessagePartial {
    case ConnectorDistributionInfo(nodeLocator) =>
      val allConnectors = nodeLocator.allActors
      allConnectors.foreach(connector => connector ! GetConnectivityInfo(ctx.self))
      val toSend = allConnectors.map(connector =>
        connector -> SendInformation(ConnectivityInformation(Seq.empty, Seq.empty, Seq.empty), sendImmediately = false)
      ).toMap
      val alreadyConnected = Array.fill(nodeLocator.graphSize){false}
      buildTree(nodeLocator, root = -1, Map.empty, alreadyConnected, toSend)

    case GetConnectivityInfo(sender) =>
      ctx.self ! GetConnectivityInfo(sender)
      waitForDistInfo()

    case BuildTreeFrom(root) =>
      ctx.self ! BuildTreeFrom(root)
      waitForDistInfo()
  }

  def buildTree(nodeLocator: NodeLocator[ConnectGraphEvent],
                root: Int,
                tree: Map[Int, CTreeNode],
                alreadyConnected: Array[Boolean],
                toSend: Map[ActorRef[ConnectGraphEvent], SendInformation]): Behavior[ConnectGraphEvent] =
    Behaviors.receiveMessagePartial {
      case BuildTreeFrom(root) =>
        // treat the newly connected node as out new root node
        val rootNode = updateNeighborConnectedness(TreeEdge(root, root), root, alreadyConnected, supervisor, nodeLocator, toSend)
        val updatedToSend = sendChangesImmediately(toSend)
        buildTree(nodeLocator, root, tree + (root -> rootNode), alreadyConnected, updatedToSend)

      case AddEdgeAndContinue(from, to) =>
        val actorToTell = nodeLocator.findResponsibleActor(to)
        tree(from).awaitingAnswer.add(to)
        alreadyConnected(to) = true
        toSend(actorToTell).connectivityInformation.addChildren :+= TreeEdge(to, from)
        val updatedToSend = sendChangesImmediately(toSend)
        buildTree(nodeLocator, from, tree, alreadyConnected, updatedToSend)

      case GetConnectivityInfo(sender) =>
        // the sender only asks for new information if they have received the last one
        val connectivityInfo = toSend(sender).connectivityInformation
        if (connectivityInfo.nothingToSend()) {
          // send as soon as there is something to send
          toSend(sender).sendImmediately = true
          buildTree(nodeLocator, root, tree, alreadyConnected, toSend)
        } else {
          val toSendInfo = sendConnectivityInfo(sender, toSend(sender).connectivityInformation)
          buildTree(nodeLocator, root, tree, alreadyConnected, toSend + (sender -> toSendInfo))
        }

      case ConnectivityInfo(connectivityInfo, sender) =>
        sender ! GetConnectivityInfo(ctx.self)
        val newNodes = addChildren(connectivityInfo.addChildren, tree, alreadyConnected, root, nodeLocator, toSend)
        notMyChildren(connectivityInfo.notChildren, tree, root, nodeLocator, toSend)
        doneChildren(connectivityInfo.doneChildren, tree, root, nodeLocator, toSend)
        // send updates to those who have asked but gotten nothing before
        val updatedToSend = sendChangesImmediately(toSend)
        buildTree(nodeLocator, root, tree ++ newNodes, alreadyConnected, updatedToSend)

      case FindUnconnectedNode(sendTo, notAskedYet) =>
        val unconnectedNodes = responsibility.diff(tree.keys.toSeq)
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
        buildTree(nodeLocator, root, tree, alreadyConnected, toSend)

      case GraphConnected =>
        supervisor ! ConnectorShutdown
        Behaviors.stopped

      case StartGraphRedistributers(redistributionCoordinator, graphNodeLocator) =>
        ctx.spawn(GraphRedistributer(data, tree, graphNodeLocator, redistributionCoordinator, nodeLocatorHolder, ctx.self), name="GraphRedistributer")
        buildTree(nodeLocator, root, tree, alreadyConnected, toSend)
    }

  def addChildren(connectedNodes: Seq[TreeEdge],
                  tree: Map[Int, CTreeNode],
                  alreadyConnected: Array[Boolean],
                  root: Int,
                  nodeLocator: NodeLocator[ConnectGraphEvent],
                  toSend: Map[ActorRef[ConnectGraphEvent], SendInformation]): Seq[(Int, CTreeNode)] = {
    val (knownNodes, unknownNodes) = connectedNodes.partition(treeEdge => tree.contains(treeEdge.child))
    knownNodes.foreach { treeEdge =>
      toSend(nodeLocator.findResponsibleActor(treeEdge.parent)).connectivityInformation.notChildren :+= treeEdge
    }
    val newNodes = unknownNodes.map { treeEdge =>
      val newNode = updateNeighborConnectedness(treeEdge, root, alreadyConnected, supervisor, nodeLocator, toSend)
      treeEdge.child -> newNode
    }
    newNodes
  }

  def notMyChildren(connectedNodes: Seq[TreeEdge],
                    tree: Map[Int, CTreeNode],
                    root: Int,
                    nodeLocator: NodeLocator[ConnectGraphEvent],
                    toSend: Map[ActorRef[ConnectGraphEvent], SendInformation]): Unit = {
    connectedNodes.foreach { treeEdge =>
      val node = tree(treeEdge.parent)
      node.awaitingAnswer -= treeEdge.child
      checkIfDone(node, treeEdge.parent, root, nodeLocator, toSend)
    }
  }

  def doneChildren(connectedNodes: Seq[TreeEdge],
                   tree: Map[Int, CTreeNode],
                   root: Int,
                   nodeLocator: NodeLocator[ConnectGraphEvent],
                   toSend: Map[ActorRef[ConnectGraphEvent], SendInformation]): Unit = {
    connectedNodes.foreach { treeEdge =>
      val node = tree(treeEdge.parent)
      node.awaitingAnswer -= treeEdge.child
      node.children += treeEdge.child
      checkIfDone(node, treeEdge.parent, root, nodeLocator, toSend)
    }
  }

  def checkIfDone(node: CTreeNode,
                  nodeIndex: Int,
                  root: Int,
                  nodeLocator: NodeLocator[ConnectGraphEvent],
                  toSend: Map[ActorRef[ConnectGraphEvent], SendInformation]): Unit = {
    if (node.awaitingAnswer.isEmpty) {
      if (nodeIndex == root) {
        supervisor ! FinishedUpdatingConnectivity
      } else {
        toSend(nodeLocator.findResponsibleActor(node.parent)).connectivityInformation.doneChildren :+= TreeEdge(nodeIndex, node.parent)
      }
    }
  }

  def addEdge(treeEdge: TreeEdge,
              parentNode: CTreeNode,
              alreadyConnected: Array[Boolean],
              nodeLocator: NodeLocator[ConnectGraphEvent],
              toSend: Map[ActorRef[ConnectGraphEvent], SendInformation]): Unit = {
    val newChild = treeEdge.child
    toSend(nodeLocator.findResponsibleActor(newChild)).connectivityInformation.addChildren :+= treeEdge
    alreadyConnected(newChild) = true
    parentNode.awaitingAnswer.add(newChild)
  }

  def sendConnectivityInfo(sendTo: ActorRef[ConnectGraphEvent], connectivityInfo: ConnectivityInformation): SendInformation = {
    val conInfoToSend = ConnectivityInformation(
      connectivityInfo.addChildren.slice(0, messageSize),
      connectivityInfo.notChildren.slice(0, messageSize),
      connectivityInfo.doneChildren.slice(0, messageSize)
    )
    val conInfoRest = ConnectivityInformation(
      connectivityInfo.addChildren.slice(messageSize, connectivityInfo.addChildren.length),
      connectivityInfo.notChildren.slice(messageSize, connectivityInfo.notChildren.length),
      connectivityInfo.doneChildren.slice(messageSize, connectivityInfo.doneChildren.length)
    )
    sendTo ! ConnectivityInfo(conInfoToSend, ctx.self)
    SendInformation(conInfoRest, sendImmediately = false)
  }

  def sendChangesImmediately(toSend: Map[ActorRef[ConnectGraphEvent], SendInformation]): Map[ActorRef[ConnectGraphEvent], SendInformation] = {
    toSend.transform { (connector, toSendInfo) =>
      if (toSendInfo.sendImmediately && toSendInfo.connectivityInformation.somethingToSend()) {
        sendConnectivityInfo(connector, toSendInfo.connectivityInformation)
      } else {
        toSendInfo
      }
    }
  }

  def updateNeighborConnectedness(newEdge: TreeEdge,
                                  root: Int,
                                  alreadyConnected: Array[Boolean],
                                  sendResultTo: ActorRef[ConnectionCoordinationEvent],
                                  nodeLocator: NodeLocator[ConnectGraphEvent],
                                  toSend: Map[ActorRef[ConnectGraphEvent], SendInformation]): CTreeNode = {
    // tell all neighbors they are connected
    val nodeInfo = CTreeNode(newEdge.parent, mutable.Set.empty, mutable.Set.empty)
    alreadyConnected(newEdge.child) = true
    alreadyConnected(newEdge.parent) = true
    graph(newEdge.child).foreach { neighborIndex =>
      if (!alreadyConnected(neighborIndex)) {
        toSend(nodeLocator.findResponsibleActor(neighborIndex)).connectivityInformation.addChildren :+= TreeEdge(neighborIndex, newEdge.child)
        alreadyConnected(neighborIndex) = true
        nodeInfo.awaitingAnswer.add(neighborIndex)
      }
    }
    if (nodeInfo.awaitingAnswer.isEmpty) {
      if (newEdge.child == root) {
        sendResultTo ! FinishedUpdatingConnectivity
      } else {
        toSend(nodeLocator.findResponsibleActor(newEdge.parent)).connectivityInformation.doneChildren :+= newEdge
      }
    }
    nodeInfo
  }
}
