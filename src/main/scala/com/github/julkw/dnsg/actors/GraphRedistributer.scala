package com.github.julkw.dnsg.actors

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.Coordinators.GraphRedistributionCoordinator.{AssignWithParentsDone, PrimaryAssignmentParents, PrimaryNodeAssignments, RedistributerDistributionInfo, RedistributionCoordinationEvent, SecondaryNodeAssignments}
import com.github.julkw.dnsg.actors.GraphConnector.CTreeNode
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.SearchOnGraphEvent
import com.github.julkw.dnsg.util.{NodeLocator, dNSGSerializable}

import scala.collection.mutable

object GraphRedistributer {

  sealed trait RedistributionEvent extends dNSGSerializable

  final case class DistributeData(workers: Set[ActorRef[SearchOnGraphEvent]], nodeLocator: NodeLocator[RedistributionEvent]) extends RedistributionEvent

  // calculating subtree-sizes
  final case class ChildSubtreeSize(g_node: Int, childIndex: Int, childSubtreeSize: Int) extends RedistributionEvent

  // Search for nodes to assign
  final case class FindNodesInRange(g_node: Int, minNodes: Int, maxNodes: Int, removeFromDescendants: Int, waitingList: Seq[WaitingListEntry], workersLeft: Set[ActorRef[SearchOnGraphEvent]], nodesLeft: Int) extends RedistributionEvent

  // assigning nodes to workers
  final case class AssignWithChildren(g_node: Int, worker: ActorRef[SearchOnGraphEvent]) extends RedistributionEvent

  final case class AssignWithParents(g_node: Int, worker: ActorRef[SearchOnGraphEvent]) extends RedistributionEvent

  final case object SendPrimaryAssignments extends RedistributionEvent

  final case object SendSecondaryAssignments extends RedistributionEvent

  // data structures for more readable code
  protected case class DistributionTreeInfo(var subTreeSize: Int, var stillToDistribute: Int, var waitingForResponses: Int, var assignedWorker: Option[ActorRef[SearchOnGraphEvent]])

  protected case class WaitingListEntry(g_node: Int, subTreeSize: Int)

  def apply(tree: Map[Int, CTreeNode], redistributionCoordinator: ActorRef[RedistributionCoordinationEvent]): Behavior[RedistributionEvent] = Behaviors.setup(ctx =>
    Behaviors.withTimers(timers =>
      new GraphRedistributer(tree, redistributionCoordinator, timers, ctx).setup()
    )
  )
}

class GraphRedistributer(tree: Map[Int, CTreeNode],
                         redistributionCoordinator: ActorRef[RedistributionCoordinationEvent],
                         timers: TimerScheduler[GraphRedistributer.RedistributionEvent],
                         ctx: ActorContext[GraphRedistributer.RedistributionEvent]) {
  import GraphRedistributer._

  def setup(): Behavior[RedistributionEvent] = {
    redistributionCoordinator ! RedistributerDistributionInfo(tree.keys.toSeq, ctx.self)
    val distributionTree: Map[Int, DistributionTreeInfo] = tree.transform((index, treeNode) => DistributionTreeInfo(1, 0, treeNode.children.size, None))
    waitForStartSignal(distributionTree)
  }

  def waitForStartSignal(distributionTree: Map[Int, DistributionTreeInfo]): Behavior[RedistributionEvent] = Behaviors.receiveMessagePartial {
    case DistributeData(workers, nodeLocator) =>
      tree.foreach { case (node, nodeInfo) =>
        if (nodeInfo.children.isEmpty) {
          nodeLocator.findResponsibleActor(nodeInfo.parent) ! ChildSubtreeSize(nodeInfo.parent, node, 1)
        }
      }
      calculateNodeSizes(distributionTree, workers, nodeLocator)

    case ChildSubtreeSize(g_node, childIndex, childSubtreeSize) =>
      val currentNode = distributionTree(g_node)
      if (currentNode.waitingForResponses == 1) {
        // I need the nodeLocator to tell the parent about subtree size, so just wait for that
        ctx.self ! ChildSubtreeSize(g_node, childIndex, childSubtreeSize)
      } else {
        currentNode.subTreeSize += childSubtreeSize
        currentNode.waitingForResponses -= 1
      }
      waitForStartSignal(distributionTree)
  }

  def calculateNodeSizes(distributionTree: Map[Int, DistributionTreeInfo],
                         workers: Set[ActorRef[SearchOnGraphEvent]],
                         nodeLocator: NodeLocator[RedistributionEvent]): Behavior[RedistributionEvent] =
    Behaviors.receiveMessagePartial {
      case ChildSubtreeSize(g_node, childIndex, childSubtreeSize) =>
        val currentNode = distributionTree(g_node)
        currentNode.subTreeSize += childSubtreeSize
        currentNode.waitingForResponses -= 1
        if (currentNode.waitingForResponses == 0) {
          val currentParent = tree(g_node).parent
          if (currentParent == g_node) {
            // this is the root
            assert(currentNode.subTreeSize == nodeLocator.graphSize)
            startSearchForNextWorkersNodes(g_node, 0, nodeLocator.graphSize, workers, nodeLocator)
            startDistribution(distributionTree, workers, nodeLocator)
          } else {
            nodeLocator.findResponsibleActor(currentParent) ! ChildSubtreeSize(currentParent, g_node, currentNode.subTreeSize)
            calculateNodeSizes(distributionTree, workers, nodeLocator)
          }
        } else {
          calculateNodeSizes(distributionTree, workers, nodeLocator)
        }

      case FindNodesInRange(g_node, minNodes, maxNodes, removeFromDescendants, waitingList, workersLeft, nodesLeft) =>
        ctx.self ! FindNodesInRange(g_node, minNodes, maxNodes, removeFromDescendants, waitingList, workersLeft, nodesLeft)
        startDistribution(distributionTree, workers, nodeLocator)

      case AssignWithChildren(g_node, worker) =>
        ctx.self ! AssignWithChildren(g_node, worker)
        startDistribution(distributionTree, workers, nodeLocator)
    }

  def distributeUsingTree(distributionTree: Map[Int, DistributionTreeInfo],
                          workers: Set[ActorRef[SearchOnGraphEvent]],
                          nodeLocator: NodeLocator[RedistributionEvent]): Behavior[RedistributionEvent] =
    Behaviors.receiveMessagePartial {
      case FindNodesInRange(g_node, minNodes, maxNodes, removeFromDescendants, waitingList, workersLeft, nodesLeft) =>
        val alreadyCollected = waitingList.map(_.subTreeSize).sum
        val treeNode = distributionTree(g_node)
        val parent = tree(g_node).parent
        treeNode.stillToDistribute -= removeFromDescendants
        val withMe = alreadyCollected + treeNode.stillToDistribute
        if (withMe < minNodes) {
          val myEntry = WaitingListEntry(g_node, treeNode.stillToDistribute)
          nodeLocator.findResponsibleActor(parent) ! FindNodesInRange(parent, minNodes, maxNodes, treeNode.subTreeSize, waitingList :+ myEntry, workersLeft, nodesLeft)
        } else if (withMe <= maxNodes) {
          val myEntry = WaitingListEntry(g_node, treeNode.stillToDistribute)
          val worker = workersLeft.head
          val updatedWaitingList = waitingList :+ myEntry
          assignNodesToWorker(updatedWaitingList, worker, nodeLocator)
          startSearchForNextWorkersNodes(parent, treeNode.subTreeSize, nodesLeft - withMe, workersLeft - worker, nodeLocator)
        } else {
          // TODO instead of choosing the first child in the list, choose the one closest to the last entry in the waitingList?
          // this would need exact locations, though
          val child = tree(g_node).children.head
          // remove the child because the next time the search reaches me it will have been assigned something or be in the waiting list
          tree(g_node).children -= child
          nodeLocator.findResponsibleActor(child) ! FindNodesInRange(child, minNodes, maxNodes, 0, waitingList, workersLeft, nodesLeft)
        }
        distributeUsingTree(distributionTree, workers, nodeLocator)

      case AssignWithChildren(g_node, worker) =>
        distributionTree(g_node).assignedWorker = Some(worker)
        tree(g_node).children.foreach(child => nodeLocator.findResponsibleActor(child) ! AssignWithChildren(child, worker))
        distributeUsingTree(distributionTree, workers, nodeLocator)

      case SendPrimaryAssignments =>
        // ensure all AssignWithChildren messages have been processed (all nodes have been assigned to a worker)
        if (distributionTree.valuesIterator.exists(treeNode => treeNode.assignedWorker.isEmpty)) {
          ctx.self ! SendPrimaryAssignments
          distributeUsingTree(distributionTree,  workers, nodeLocator)
        } else {
          redistributionCoordinator ! PrimaryNodeAssignments(distributionTree.transform((_, distInfo) => distInfo.assignedWorker.get))
          findSecondaryAssignments(distributionTree, Map.empty, nodeLocator)
        }
    }

  def findSecondaryAssignments(distributionTree: Map[Int, DistributionTreeInfo],
                               secondaryAssignments: Map[Int, Set[ActorRef[SearchOnGraphEvent]]],
                               nodeLocator: NodeLocator[RedistributionEvent]): Behavior[RedistributionEvent] =
    Behaviors.receiveMessagePartial {
      case AssignWithParents(g_node, worker) =>
        val currentAssignees = secondaryAssignments.getOrElse(g_node, Set.empty)
        val parent = tree(g_node).parent
        if (currentAssignees.contains(worker) || parent == g_node) {
          redistributionCoordinator ! AssignWithParentsDone
          findSecondaryAssignments(distributionTree, secondaryAssignments, nodeLocator)
        } else {
          nodeLocator.findResponsibleActor(parent) ! AssignWithParents(parent, worker)
          if (distributionTree(g_node).assignedWorker != worker) {
            findSecondaryAssignments(distributionTree, secondaryAssignments + (g_node -> (currentAssignees + worker)), nodeLocator)
          } else {
            // if it is already the primary worker it does not need to be added to the secondary Assignees as well
            findSecondaryAssignments(distributionTree, secondaryAssignments, nodeLocator)
          }
        }
      case SendSecondaryAssignments =>
        redistributionCoordinator ! SecondaryNodeAssignments(secondaryAssignments)
        Behaviors.stopped
    }

  def startDistribution(distributionTree: Map[Int, DistributionTreeInfo],
                        workers: Set[ActorRef[SearchOnGraphEvent]],
                        nodeLocator: NodeLocator[RedistributionEvent]): Behavior[RedistributionEvent] = {
    distributionTree.valuesIterator.foreach(nodeInfo => nodeInfo.stillToDistribute = nodeInfo.subTreeSize)
    distributeUsingTree(distributionTree, workers, nodeLocator)
  }

  def assignNodesToWorker(waitingList: Seq[WaitingListEntry],
                          worker: ActorRef[SearchOnGraphEvent],
                          nodeLocator: NodeLocator[RedistributionEvent]): Unit = {
    val inList = waitingList.map(_.g_node)
    redistributionCoordinator ! PrimaryAssignmentParents(worker, waitingList.map(_.g_node))
    waitingList.foreach { entry =>
      val nodeIndex = entry.g_node
      nodeLocator.findResponsibleActor(nodeIndex) ! AssignWithChildren(nodeIndex, worker)
    }
  }

  def startSearchForNextWorkersNodes(nextNodeInSearch: Int,
                                     removeDescendants: Int,
                                     nodesToDistribute: Int,
                                     workersLeft: Set[ActorRef[SearchOnGraphEvent]],
                                     nodeLocator: NodeLocator[RedistributionEvent]): Unit = {
    if (workersLeft.isEmpty) {
      nodeLocator.allActors.foreach(redistributer => redistributer ! SendPrimaryAssignments)
    } else if (workersLeft.size == 1) {
      nodeLocator.findResponsibleActor(nextNodeInSearch) ! FindNodesInRange(nextNodeInSearch, nodesToDistribute, nodesToDistribute, removeDescendants, Seq.empty, workersLeft, nodesToDistribute)
    } else {
      // TODO test different ranges?
      val minNodesPerWorker = nodesToDistribute / workersLeft.size - nodesToDistribute / (100 * (workersLeft.size - 1))
      val maxNodesPerWorker = nodesToDistribute / workersLeft.size + nodesToDistribute / (100 * (workersLeft.size - 1))
      nodeLocator.findResponsibleActor(nextNodeInSearch) ! FindNodesInRange(nextNodeInSearch, minNodesPerWorker, maxNodesPerWorker, removeDescendants, Seq.empty, workersLeft, nodesToDistribute)
    }
  }
}
