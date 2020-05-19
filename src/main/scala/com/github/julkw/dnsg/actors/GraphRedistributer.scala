package com.github.julkw.dnsg.actors

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.ClusterCoordinator.{CoordinationEvent, RedistributerDistributionInfo}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.SearchOnGraphEvent
import com.github.julkw.dnsg.util.Data.LocalData
import com.github.julkw.dnsg.util.{NodeLocator, dNSGSerializable}

object GraphRedistributer {

  sealed trait RedistributionEvent extends dNSGSerializable

  final case class DistributeData(root: Int, dataSize: Int, workers: Set[ActorRef[SearchOnGraphEvent]], dataReplication: DataReplicationModel, nodeLocator: NodeLocator[RedistributionEvent]) extends RedistributionEvent

  // tree building
  final case class AddToTree(g_node: Int, parent: Int) extends RedistributionEvent

  final case class IsChildOf(g_node: Int, childIndex: Int, childDescendants: Int) extends RedistributionEvent

  final case class NotYourChild(g_node: Int) extends RedistributionEvent

  // Search for nodes to assign
  final case class FindNodesInRange(g_node: Int, minNodes: Int, maxNodes: Int, removeFromDescendants: Int, waitingList: Seq[WaitingListEntry], workersLeft: Set[ActorRef[SearchOnGraphEvent]], nodesLeft: Int) extends RedistributionEvent

  // assigning nodes to workers
  final case class AssignWithChildren(g_node: Int, worker: ActorRef[SearchOnGraphEvent]) extends RedistributionEvent

  final case class AssignWithParents(g_node: Int, worker: ActorRef[SearchOnGraphEvent]) extends RedistributionEvent

  final case object SendResults extends RedistributionEvent

  // data structures for more readable code
  protected case class TreeNodeInfo(parent: Int, children: Array[Boolean], var descendants: Int, var waitingForResponses: Int)

  protected case class WaitingListEntry(g_node: Int, descendants: Int)

  trait DataReplicationModel

  case object NoReplication extends DataReplicationModel

  case object OnlyParentsReplication extends DataReplicationModel

  case object AllSharedReplication extends DataReplicationModel

  def apply(graph: Map[Int, Seq[Int]], data: LocalData[Float], clusterCoordinator: ActorRef[CoordinationEvent]): Behavior[RedistributionEvent] = Behaviors.setup(ctx =>
    new GraphRedistributer(graph, data, clusterCoordinator, ctx).setup()
  )
}

class GraphRedistributer(graph: Map[Int, Seq[Int]], data: LocalData[Float], clusterCoordinator: ActorRef[CoordinationEvent], ctx: ActorContext[GraphRedistributer.RedistributionEvent]) {
  import GraphRedistributer._

  def setup(): Behavior[RedistributionEvent] = {
    clusterCoordinator ! RedistributerDistributionInfo(graph.keys.toSeq, ctx.self)
    waitForStartSignal()
  }

  def waitForStartSignal(): Behavior[RedistributionEvent] = Behaviors.receiveMessagePartial {
    case DistributeData(root, dataSize, workers, dataReplication, nodeLocator) =>
      if (graph.contains(root)) {
        val rootInfo = createTreeNode(root, root, graph(root), nodeLocator)
        buildTreeForDistribution(Map(root -> rootInfo), dataSize, workers, dataReplication, nodeLocator)
      } else {
        buildTreeForDistribution(Map.empty, dataSize, workers, dataReplication, nodeLocator)
      }

    case AddToTree(g_node, parent) =>
      // TODO find nicer way to deal with this message arriving before the DistributeData one
      ctx.self ! AddToTree(g_node, parent)
      waitForStartSignal()
  }

  def buildTreeForDistribution(tree: Map[Int, TreeNodeInfo],
                               dataSize: Int,
                               workers: Set[ActorRef[SearchOnGraphEvent]],
                               replicationStrategy: DataReplicationModel,
                               nodeLocator: NodeLocator[RedistributionEvent]): Behavior[RedistributionEvent] =
    Behaviors.receiveMessagePartial {
      case AddToTree(g_node, parent) =>
        if (tree.contains(g_node)) {
          nodeLocator.findResponsibleActor(parent) ! NotYourChild(parent)
          buildTreeForDistribution(tree, dataSize, workers, replicationStrategy, nodeLocator)
        } else {
          val nodeInfo = createTreeNode(g_node, parent, graph(g_node), nodeLocator)
          buildTreeForDistribution(tree + (g_node -> nodeInfo), dataSize, workers, replicationStrategy, nodeLocator)
        }

      case IsChildOf(g_node, childIndex, childDescendants) =>
        val neighbors = graph(g_node)
        val nodeInfoChildIndex = neighbors.indexOf(childIndex)
        val treeNode = tree(g_node)
        treeNode.children(nodeInfoChildIndex) = true
        treeNode.descendants += childDescendants
        val treeDone = updateResponses(g_node, tree(g_node), nodeLocator)
        if (treeDone) {
          ctx.self ! FindNodesInRange(g_node, 0, 0, 0, Seq.empty, workers, dataSize)
          // TODO fill map from the beginning
          distributeUsingTree(tree, Map.empty, replicationStrategy, workers, nodeLocator)
        } else {
          buildTreeForDistribution(tree, dataSize, workers, replicationStrategy, nodeLocator)
        }

      case NotYourChild(g_node) =>
        val treeDone = updateResponses(g_node, tree(g_node), nodeLocator)
        if (treeDone) {
          ctx.self ! FindNodesInRange(g_node, 0, 0, 0, Seq.empty, workers, dataSize)
          distributeUsingTree(tree, Map.empty, replicationStrategy, workers, nodeLocator)
        } else {
          buildTreeForDistribution(tree, dataSize, workers, replicationStrategy, nodeLocator)
        }
    }

  def distributeUsingTree(tree: Map[Int, TreeNodeInfo],
                          distributionInfo: Map[Int, Set[ActorRef[SearchOnGraphEvent]]],
                          replicationStrategy: DataReplicationModel,
                          workers: Set[ActorRef[SearchOnGraphEvent]],
                          nodeLocator: NodeLocator[RedistributionEvent]): Behavior[RedistributionEvent] =
    Behaviors.receiveMessagePartial {
      case FindNodesInRange(g_node, minNodes, maxNodes, removeFromDescendants, waitingList, workersLeft, nodesLeft) =>
        val alreadyCollected = waitingList.map(_.descendants).sum + waitingList.length
        val treeNode = tree(g_node)
        treeNode.descendants -= removeFromDescendants
        val withMe = alreadyCollected + treeNode.descendants + 1
        if (withMe < minNodes) {
          val myEntry = WaitingListEntry(g_node, treeNode.descendants)
          nodeLocator.findResponsibleActor(treeNode.parent) ! FindNodesInRange(treeNode.parent, minNodes, maxNodes, treeNode.descendants + 1, waitingList :+ myEntry, workersLeft, nodesLeft)
        } else if (withMe <= maxNodes) {
          val myEntry = WaitingListEntry(g_node, treeNode.descendants)
          val worker = workersLeft.head
          // TODO check if done
          assignNodesToWorker(waitingList :+ myEntry, worker, replicationStrategy, nodeLocator)
          startSearchForNextWorkersNodes(treeNode.parent, nodesLeft - withMe, treeNode.descendants + 1, workersLeft - worker, nodeLocator)
        } else {
          val childIndex = treeNode.children.indexWhere(isChild => isChild)
          val child = graph(g_node)(childIndex)
          // remove the child because the next time the search reaches me it will have been assigned something or be in the waiting list
          treeNode.children(childIndex) = false
          nodeLocator.findResponsibleActor(child) ! FindNodesInRange(child, minNodes, maxNodes, 0, waitingList, workersLeft, nodesLeft)
        }
        distributeUsingTree(tree, distributionInfo, replicationStrategy, workers, nodeLocator)

      case AssignWithChildren(g_node, worker) =>
        val treeNode = tree(g_node)
        val children = graph(g_node).zip(treeNode.children).filter(_._2).map(_._1)
        children.foreach(child => nodeLocator.findResponsibleActor(child) ! AssignWithChildren(child, worker))
        // in assign with children each node should only be called once
        distributeUsingTree(tree, distributionInfo + (g_node -> Set(worker)), replicationStrategy, workers, nodeLocator)

      case AssignWithParents(g_node, worker) =>
        val parent = tree(g_node).parent
        if (parent != g_node) {
          nodeLocator.findResponsibleActor(parent) ! AssignWithParents(parent, worker)
        }
        if (distributionInfo.contains(g_node)) {
          val updatedAssignees = distributionInfo(g_node) + worker
          distributeUsingTree(tree, distributionInfo + (g_node -> updatedAssignees), replicationStrategy, workers, nodeLocator)
        } else {
          distributeUsingTree(tree, distributionInfo + (g_node -> Set(worker)), replicationStrategy, workers, nodeLocator)
        }

      case SendResults =>
        // TODO depending on replicationStrategy change and send results to clusterCoordinator
        Behaviors.stopped
    }

  def createTreeNode(nodeIndex: Int, parentIndex: Int, neighbors: Seq[Int], nodeLocator: NodeLocator[RedistributionEvent]): TreeNodeInfo = {
    neighbors.foreach(neighbor => nodeLocator.findResponsibleActor(neighbor) ! AddToTree(neighbor, nodeIndex))
    TreeNodeInfo(parentIndex, Array.fill(neighbors.length){false}, 0, neighbors.length)
  }

  def updateResponses(nodeIndex: Int, treeNode: TreeNodeInfo, nodeLocator: NodeLocator[RedistributionEvent]): Boolean = {
    // TODO instead of returning boolean, have this method call buildTree... or distributeUsing...
    treeNode.waitingForResponses -= 1
    if (treeNode.waitingForResponses == 0) {
      if (treeNode.parent != nodeIndex) {
        nodeLocator.findResponsibleActor(treeNode.parent) ! IsChildOf(treeNode.parent, nodeIndex, treeNode.descendants)
        false
      } else {
        // this is the root of the tree, the building of the tree is done
        ctx.log.info("Tree for redistribution is built, now assigning g_nodes to workers")
        true
      }
    } else {
      false
    }
  }

  def assignNodesToWorker(waitingList: Seq[WaitingListEntry],
                          worker: ActorRef[SearchOnGraphEvent],
                          replicationStrategy: DataReplicationModel,
                          nodeLocator: NodeLocator[RedistributionEvent]): Unit = {
    waitingList.foreach { entry =>
      val nodeIndex = entry.g_node
      nodeLocator.findResponsibleActor(nodeIndex) ! AssignWithChildren(nodeIndex, worker)
      if (replicationStrategy != NoReplication) {
        nodeLocator.findResponsibleActor(nodeIndex) ! AssignWithParents(nodeIndex, worker)
      }
    }
  }

  def startSearchForNextWorkersNodes(nextNodeInSearch: Int,
                                     removeDescendants: Int,
                                     nodesToDistribute: Int,
                                     workersLeft: Set[ActorRef[SearchOnGraphEvent]],
                                     nodeLocator: NodeLocator[RedistributionEvent]): Unit = {
    if (workersLeft.isEmpty) {
      // start Redistribution protocol
      // tell all GraphRedistributers (get from NodeLocator?) to send their distributionInfo to ClusterCoordinator
      // depending on replication strategy fill all nodes with multiple assignments with all workers
      // send distributionInfo to ClusterCoordinator and have it too dataHolders and SearchOnGraphActor whom to send which data to
    }
    // TODO test different ranges?
    val minNodesPerWorker = nodesToDistribute / workersLeft.size - nodesToDistribute / (100 * (workersLeft.size - 1))
    val maxNodesPerWorker = nodesToDistribute / workersLeft.size + nodesToDistribute / (100 * (workersLeft.size - 1))

    nodeLocator.findResponsibleActor(nextNodeInSearch) ! FindNodesInRange(nextNodeInSearch, minNodesPerWorker, maxNodesPerWorker, removeDescendants, Seq.empty, workersLeft, nodesToDistribute)

  }
}
