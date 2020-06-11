package com.github.julkw.dnsg.actors

import scala.concurrent.duration._
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.CoordinationEvent
import com.github.julkw.dnsg.actors.Coordinators.GraphConnectorCoordinator.{AllConnected, ConnectionCoordinationEvent, FinishedUpdatingConnectivity, GraphConnectorDistributionInfo, UnconnectedNode}
import com.github.julkw.dnsg.util.Data.LocalData
import com.github.julkw.dnsg.util.{NodeLocator, Settings, dNSGSerializable}

import scala.collection.mutable

object GraphConnector {
  sealed trait ConnectGraphEvent extends dNSGSerializable

  final case class ConnectorDistributionInfo(nodeLocator: NodeLocator[ActorRef[ConnectGraphEvent]]) extends ConnectGraphEvent

  final case class BuildTreeFrom(root: Int) extends ConnectGraphEvent

  // work pulling
  final case class GetConnectivityInfo(sender: ActorRef[ConnectGraphEvent]) extends ConnectGraphEvent

  final case class ConnectivityInfo(connectivityInfo: ConnectivityInformation, sender: ActorRef[ConnectGraphEvent]) extends ConnectGraphEvent

  final case class AddEdgeAndContinue(from: Int, to: Int) extends ConnectGraphEvent

  final case class FindUnconnectedNode(sendTo: ActorRef[ConnectionCoordinationEvent], notAskedYet: Set[ActorRef[ConnectGraphEvent]]) extends ConnectGraphEvent

  final case object GraphConnected extends ConnectGraphEvent

  final case class StartGraphRedistributers(clusterCoordinator: ActorRef[CoordinationEvent]) extends ConnectGraphEvent

  // ensure message deliver
  protected case class ConnectivityInfoTimerKey(receiver: ActorRef[ConnectGraphEvent])

  protected case class ResendConnectivityInfo(infoToResend: ConnectivityInformation, sendTo: ActorRef[ConnectGraphEvent]) extends ConnectGraphEvent

  val timeout = 3.second

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
            supervisor: ActorRef[ConnectionCoordinationEvent]): Behavior[ConnectGraphEvent] = Behaviors.setup { ctx =>
    val messageSize = Settings(ctx.system.settings.config).connectMessageSize
    Behaviors.withTimers(timers =>
      new GraphConnector(data, graph, messageSize, timers, supervisor, ctx).setup()
    )
  }
}

class GraphConnector(data: LocalData[Float],
                     graph: Map[Int, Seq[Int]],
                     messageSize: Int,
                     timers: TimerScheduler[GraphConnector.ConnectGraphEvent],
                     supervisor: ActorRef[ConnectionCoordinationEvent],
                     ctx: ActorContext[GraphConnector.ConnectGraphEvent]) {
  import GraphConnector._

  def setup(): Behavior[ConnectGraphEvent] = {
    supervisor ! GraphConnectorDistributionInfo(graph.keys.toSeq, ctx.self)
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
      buildTree(nodeLocator, -1, Map.empty, alreadyConnected, toSend)

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
                alreadyConnected: Array[Boolean],
                toSend: Map[ActorRef[ConnectGraphEvent], SendInformation]): Behavior[ConnectGraphEvent] =
    Behaviors.receiveMessagePartial {
      case BuildTreeFrom(root) =>
        ctx.log.info("Updating connectivity")
        // treat the newly connected node as out new root node
        val rootNode = updateNeighborConnectedness(TreeEdge(root, root), root, alreadyConnected, supervisor, nodeLocator, toSend)
        buildTree(nodeLocator, root, tree + (root -> rootNode), alreadyConnected, toSend)

      case AddEdgeAndContinue(from, to) =>
        ctx.log.info("Updating connectivity after adding a new edge: {} to {}", from, to)
        val actorToTell = nodeLocator.findResponsibleActor(to)
        tree(from).awaitingAnswer.add(to)
        alreadyConnected(to) = true
        toSend(actorToTell).connectivityInformation.addChildren :+= TreeEdge(to, from)
        if (toSend(actorToTell).sendImmediately) {
          val toSendInfo = sendConnectivityInfo(actorToTell, toSend(actorToTell).connectivityInformation)
          buildTree(nodeLocator, root, tree, alreadyConnected, toSend + (actorToTell -> toSendInfo))
        } else {
          buildTree(nodeLocator, from, tree, alreadyConnected, toSend)
        }

      case GetConnectivityInfo(sender) =>
        // the sender only asks for new information if they have received the last one
        timers.cancel(ConnectivityInfoTimerKey(sender))
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
        val updatedToSend = toSend.transform { (connector, toSendInfo) =>
          if (toSendInfo.sendImmediately && toSendInfo.connectivityInformation.somethingToSend()) {
            sendConnectivityInfo(connector, toSendInfo.connectivityInformation)
          } else {
            toSendInfo
          }
        }
        buildTree(nodeLocator, root, tree ++ newNodes, alreadyConnected, updatedToSend)

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
        buildTree(nodeLocator, root, tree, alreadyConnected, toSend)

      case GraphConnected =>
        Behaviors.stopped

      case StartGraphRedistributers(clusterCoordinator) =>
        ctx.spawn(GraphRedistributer(tree, clusterCoordinator), name="GraphRedistributer")
        buildTree(nodeLocator, root, tree, alreadyConnected, toSend)
    }

  def addChildren(connectedNodes: Seq[TreeEdge],
                  tree: Map[Int, CTreeNode],
                  alreadyConnected: Array[Boolean],
                  root: Int,
                  nodeLocator: NodeLocator[ActorRef[ConnectGraphEvent]],
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
                    nodeLocator: NodeLocator[ActorRef[ConnectGraphEvent]],
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
                   nodeLocator: NodeLocator[ActorRef[ConnectGraphEvent]],
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
                  nodeLocator: NodeLocator[ActorRef[ConnectGraphEvent]],
                  toSend: Map[ActorRef[ConnectGraphEvent], SendInformation]): Unit = {
    //ctx.log.info("{} still waiting on {}", nodeIndex, node.awaitingAnswer)
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
              nodeLocator: NodeLocator[ActorRef[ConnectGraphEvent]],
              toSend: Map[ActorRef[ConnectGraphEvent], SendInformation]): Unit = {
    val newChild = treeEdge.child
    toSend(nodeLocator.findResponsibleActor(newChild)).connectivityInformation.addChildren :+= treeEdge
    alreadyConnected(newChild) = true
    parentNode.awaitingAnswer.add(newChild)
  }

  def sendConnectivityInfo(sendTo: ActorRef[ConnectGraphEvent], connectivityInfo: ConnectivityInformation): SendInformation = {
    // TODO potentially add timer to ensure the message gets received (cancel timer on next GetConnectivityInfo from sender)
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
    timers.startSingleTimer(ConnectivityInfoTimerKey(sendTo), ResendConnectivityInfo(conInfoToSend, sendTo), timeout)
    SendInformation(conInfoRest, false)
  }

  def updateNeighborConnectedness(newEdge: TreeEdge,
                                  root: Int,
                                  alreadyConnected: Array[Boolean],
                                  sendResultTo: ActorRef[ConnectionCoordinationEvent],
                                  nodeLocator: NodeLocator[ActorRef[ConnectGraphEvent]],
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
