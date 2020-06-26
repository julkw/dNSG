package com.github.julkw.dnsg.actors

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.Coordinators.GraphRedistributionCoordinator.{AssignWithParentsDone, PrimaryAssignmentParents, RedistributionCoordinationEvent}
import com.github.julkw.dnsg.actors.GraphConnector.{CTreeNode, ConnectGraphEvent}
import com.github.julkw.dnsg.actors.NodeLocatorHolder.{GraphRedistributerGotGraphFrom, LocalPrimaryNodeAssignments, LocalSecondaryNodeAssignments, NodeLocationEvent}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.SearchOnGraphEvent
import com.github.julkw.dnsg.util.Data.LocalData
import com.github.julkw.dnsg.util.{Distance, NodeLocator, Settings, dNSGSerializable}

object GraphRedistributer {

  sealed trait RedistributionEvent extends dNSGSerializable

  final case class DistributeData(nodeLocator: NodeLocator[RedistributionEvent]) extends RedistributionEvent

  // calculating subtree-sizes
  final case class ChildSubtreeSizes(subtreeSizes: Seq[SubtreeInfo], sender: ActorRef[RedistributionEvent]) extends RedistributionEvent

  final case class GetChildSubtreeSizes(sender: ActorRef[RedistributionEvent]) extends RedistributionEvent

  // Search for nodes to assign
  final case class FindNodesInRange(g_node: Int, minNodes: Int, maxNodes: Int, removeFromDescendants: Int, waitingList: Seq[WaitingListEntry], workersLeft: Set[ActorRef[SearchOnGraphEvent]], nodesLeft: Int) extends RedistributionEvent

  final case class GetLocationForRedistribution(index: Int, sender: ActorRef[RedistributionEvent]) extends RedistributionEvent

  final case class LocationForRedistribution(index: Int, location: Seq[Float]) extends RedistributionEvent

  // assigning nodes to workers
  // TODO bundle these two
  final case class AssignWithChildren(g_node: Int, worker: ActorRef[SearchOnGraphEvent]) extends RedistributionEvent

  final case class AssignWithParents(g_node: Int, worker: ActorRef[SearchOnGraphEvent]) extends RedistributionEvent

  final case object SendPrimaryAssignments extends RedistributionEvent

  final case object SendSecondaryAssignments extends RedistributionEvent

  // data structures for more readable code
  protected case class DistributionTreeInfo(var subTreeSize: Int, var stillToDistribute: Int, var waitingForResponses: Int, var assignedWorker: Option[ActorRef[SearchOnGraphEvent]])

  protected case class WaitingListEntry(g_node: Int, subTreeSize: Int)

  protected case class SubtreeInfo(parentIndex: Int, childIndex: Int, childSubtreeSize: Int)

  def apply(data: LocalData[Float],
            tree: Map[Int, CTreeNode],
            graphNodeLocator: NodeLocator[SearchOnGraphEvent],
            redistributionCoordinator: ActorRef[RedistributionCoordinationEvent],
            nodeLocatorHolder: ActorRef[NodeLocationEvent],
            parent: ActorRef[ConnectGraphEvent]): Behavior[RedistributionEvent] =
    Behaviors.setup { ctx =>
      val maxMessageSize = Settings(ctx.system.settings.config).maxMessageSize
      val optimalRedistribution = Settings(ctx.system.settings.config).dataRedistribution == "optimalRedistribution"
      new GraphRedistributer(data, tree, optimalRedistribution, graphNodeLocator, redistributionCoordinator, nodeLocatorHolder, parent, maxMessageSize, ctx).setup()
    }
}

class GraphRedistributer(data: LocalData[Float],
                         tree: Map[Int, CTreeNode],
                         optimalRedistribution: Boolean,
                         graphNodeLocator: NodeLocator[SearchOnGraphEvent],
                         redistributionCoordinator: ActorRef[RedistributionCoordinationEvent],
                         nodeLocatorHolder: ActorRef[NodeLocationEvent],
                         parent: ActorRef[ConnectGraphEvent],
                         maxMessageSize: Int,
                         ctx: ActorContext[GraphRedistributer.RedistributionEvent]) extends Distance {
  import GraphRedistributer._

  def setup(): Behavior[RedistributionEvent] = {
    nodeLocatorHolder ! GraphRedistributerGotGraphFrom(parent, ctx.self)
    val distributionTree: Map[Int, DistributionTreeInfo] = tree.transform((_, treeNode) => DistributionTreeInfo(1, 0, treeNode.children.size, None))
    waitForStartSignal(distributionTree)
  }

  def waitForStartSignal(distributionTree: Map[Int, DistributionTreeInfo]): Behavior[RedistributionEvent] = Behaviors.receiveMessagePartial {
    case DistributeData(nodeLocator) =>
      val leafNodes = tree.collect { case (node, nodeInfo) if (nodeInfo.children.isEmpty) =>
         SubtreeInfo(nodeInfo.parent, node, 1)
      }
      val toSend = leafNodes.groupBy(leafNode => nodeLocator.findResponsibleActor(leafNode.parentIndex))
      val fullToSend = nodeLocator.allActors.map { redistributer =>
        if (toSend.contains(redistributer)) {
          redistributer -> (toSend(redistributer).toSeq, false)
        } else {
          redistributer -> (Seq.empty, false)
        }
      }.toMap
      nodeLocator.allActors.foreach(redistributer => redistributer ! GetChildSubtreeSizes(ctx.self))
      calculateNodeSizes(distributionTree, graphNodeLocator.allActors, nodeLocator, maxMessageSize / 3, fullToSend)

    case GetChildSubtreeSizes(sender) =>
      ctx.self ! GetChildSubtreeSizes(sender)
      waitForStartSignal(distributionTree)
  }

  def calculateNodeSizes(distributionTree: Map[Int, DistributionTreeInfo],
                         workers: Set[ActorRef[SearchOnGraphEvent]],
                         nodeLocator: NodeLocator[RedistributionEvent],
                         maxSubtreeMessageSize: Int,
                         toSend: Map[ActorRef[RedistributionEvent], (Seq[SubtreeInfo], Boolean)]): Behavior[RedistributionEvent] =
    Behaviors.receiveMessagePartial {
      case GetChildSubtreeSizes(sender) =>
        val allMessagesToSend = toSend(sender)._1
        if (allMessagesToSend.nonEmpty) {
          val messageToSend = allMessagesToSend.slice(0, maxSubtreeMessageSize)
          sender ! ChildSubtreeSizes(messageToSend, ctx.self)
          val toSendLater = allMessagesToSend.slice(maxSubtreeMessageSize, allMessagesToSend.length)
          val updatedToSend = toSend + (sender -> (toSendLater, false))
          calculateNodeSizes(distributionTree, workers, nodeLocator, maxSubtreeMessageSize, updatedToSend)
        } else {
          val updatedToSend = toSend + (sender -> (toSend(sender)._1, true))
          calculateNodeSizes(distributionTree, workers, nodeLocator, maxSubtreeMessageSize, updatedToSend)
        }

      case ChildSubtreeSizes(subtreeSizes, sender) =>
        var newMessages: Seq[SubtreeInfo] = Seq.empty
        var rootNodeDone = -1
        subtreeSizes.foreach { subtreeInfo =>
          val currentNode = distributionTree(subtreeInfo.parentIndex)
          currentNode.subTreeSize += subtreeInfo.childSubtreeSize
          currentNode.waitingForResponses -= 1
          if (currentNode.waitingForResponses == 0) {
            val currentParent = tree(subtreeInfo.parentIndex).parent
            if (currentParent == subtreeInfo.parentIndex) {
              // this is the root
              assert(currentNode.subTreeSize == nodeLocator.graphSize)
              rootNodeDone = currentParent
            } else {
              newMessages :+= SubtreeInfo(currentParent, subtreeInfo.parentIndex, currentNode.subTreeSize)
            }
          }
        }
        if (rootNodeDone >= 0) {
          // all subtree sizes have been calculated, start assigning workers
          startSearchForNextWorkersNodes(rootNodeDone, removeDescendants = 0, nodeLocator.graphSize, workers, nodeLocator)
          startDistribution(distributionTree, nodeLocator)
        } else {
          sender ! GetChildSubtreeSizes(ctx.self)
          // add new messages to toSend and send out new info to the actors waiting for some
          val groupedNewMessages = newMessages.groupBy(subtreeInfo => nodeLocator.findResponsibleActor(subtreeInfo.parentIndex))
          val updatedToSend = toSend.transform { (actor, toSendInfo) =>
            val updatedMessages = toSendInfo._1 ++ groupedNewMessages.getOrElse(actor, Seq.empty)
            if (toSendInfo._2 && updatedMessages.nonEmpty) {
              // send new info immediately
              val messageToSend = updatedMessages.slice(0, maxSubtreeMessageSize)
              actor ! ChildSubtreeSizes(messageToSend, ctx.self)
              val toSendLater = updatedMessages.slice(maxSubtreeMessageSize, updatedMessages.length)
              (toSendLater, false)
            } else {
              (updatedMessages, toSendInfo._2)
            }
          }
          calculateNodeSizes(distributionTree, workers, nodeLocator, maxSubtreeMessageSize, updatedToSend)
        }

      case FindNodesInRange(g_node, minNodes, maxNodes, removeFromDescendants, waitingList, workersLeft, nodesLeft) =>
        ctx.self ! FindNodesInRange(g_node, minNodes, maxNodes, removeFromDescendants, waitingList, workersLeft, nodesLeft)
        startDistribution(distributionTree, nodeLocator)

      case AssignWithChildren(g_node, worker) =>
        ctx.self ! AssignWithChildren(g_node, worker)
        startDistribution(distributionTree, nodeLocator)

      case GetLocationForRedistribution(index, sender) =>
        sender ! LocationForRedistribution(index, data.get(index))
        startDistribution(distributionTree, nodeLocator)
    }

  def distributeUsingTree(distributionTree: Map[Int, DistributionTreeInfo],
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
          distributeUsingTree(distributionTree, nodeLocator)
        } else if (withMe <= maxNodes) {
          val myEntry = WaitingListEntry(g_node, treeNode.stillToDistribute)
          val worker = workersLeft.head
          val updatedWaitingList = waitingList :+ myEntry
          assignNodesToWorker(updatedWaitingList, worker, nodeLocator)
          startSearchForNextWorkersNodes(parent, treeNode.subTreeSize, nodesLeft - withMe, workersLeft - worker, nodeLocator)
          distributeUsingTree(distributionTree, nodeLocator)
        } else if (optimalRedistribution && waitingList.nonEmpty) {
          // ask for locations of the potential next nodes to continue search so we can choose the closest one to the ones already in the waitingList
          val locationsNeeded = tree(g_node).children + waitingList.last.g_node
          locationsNeeded.foreach(graphIndex => nodeLocator.findResponsibleActor(graphIndex) ! GetLocationForRedistribution(graphIndex, ctx.self))
          chooseNodeToContinueSearch(Map.empty, locationsNeeded.size, g_node, minNodes, maxNodes, waitingList, workersLeft, nodesLeft, distributionTree, nodeLocator)
        } else {
          // continue search with random child
          val child = tree(g_node).children.head
          tree(g_node).children -= child
          nodeLocator.findResponsibleActor(child) ! FindNodesInRange(child, minNodes, maxNodes, 0, waitingList, workersLeft, nodesLeft)
          distributeUsingTree(distributionTree, nodeLocator)
        }

      case AssignWithChildren(g_node, worker) =>
        distributionTree(g_node).assignedWorker = Some(worker)
        tree(g_node).children.foreach(child => nodeLocator.findResponsibleActor(child) ! AssignWithChildren(child, worker))
        distributeUsingTree(distributionTree, nodeLocator)

      case GetLocationForRedistribution(index, sender) =>
        sender ! LocationForRedistribution(index, data.get(index))
        distributeUsingTree(distributionTree, nodeLocator)

      case SendPrimaryAssignments =>
        // ensure all AssignWithChildren messages have been processed (all nodes have been assigned to a worker)
        if (distributionTree.valuesIterator.exists(treeNode => treeNode.assignedWorker.isEmpty)) {
          ctx.self ! SendPrimaryAssignments
          distributeUsingTree(distributionTree, nodeLocator)
        } else {
          nodeLocatorHolder ! LocalPrimaryNodeAssignments(distributionTree.transform((_, distInfo) => distInfo.assignedWorker.get))
          findSecondaryAssignments(distributionTree, Map.empty, nodeLocator)
        }
    }

  def chooseNodeToContinueSearch(nodesToChooseFrom: Map[Int, Seq[Float]],
                                 nodesExpected: Int,
                                 parentNode: Int,
                                 minNodes: Int,
                                 maxNodes: Int,
                                 waitingList: Seq[WaitingListEntry],
                                 workersLeft: Set[ActorRef[SearchOnGraphEvent]],
                                 nodesLeft: Int,
                                 distributionTree: Map[Int, DistributionTreeInfo],
                                 nodeLocator: NodeLocator[RedistributionEvent]): Behavior[RedistributionEvent] =
    Behaviors.receiveMessagePartial {
      case LocationForRedistribution(index, location) =>
        val updatedNodesToChooseFrom = nodesToChooseFrom + (index -> location)
        if (updatedNodesToChooseFrom.size == nodesExpected) {
          val lastNode = waitingList.last.g_node
          val lastChosenNodeLocation = updatedNodesToChooseFrom(lastNode)
          val nextNode = (updatedNodesToChooseFrom - lastNode).toSeq.minBy(node => euclideanDist(node._2, lastChosenNodeLocation))._1
          tree(parentNode).children -= nextNode
          nodeLocator.findResponsibleActor(nextNode) ! FindNodesInRange(nextNode, minNodes, maxNodes, 0, waitingList, workersLeft, nodesLeft)
          distributeUsingTree(distributionTree, nodeLocator)
        } else {
          chooseNodeToContinueSearch(updatedNodesToChooseFrom, nodesExpected, parentNode, minNodes, maxNodes, waitingList, workersLeft, nodesLeft, distributionTree, nodeLocator)
        }

      case GetLocationForRedistribution(index, sender) =>
        sender ! LocationForRedistribution(index, data.get(index))
        chooseNodeToContinueSearch(nodesToChooseFrom, nodesExpected, parentNode, minNodes, maxNodes, waitingList, workersLeft, nodesLeft, distributionTree, nodeLocator)

      case AssignWithChildren(g_node, worker) =>
        distributionTree(g_node).assignedWorker = Some(worker)
        tree(g_node).children.foreach(child => nodeLocator.findResponsibleActor(child) ! AssignWithChildren(child, worker))
        chooseNodeToContinueSearch(nodesToChooseFrom, nodesExpected, parentNode, minNodes, maxNodes, waitingList, workersLeft, nodesLeft, distributionTree, nodeLocator)
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
          if (distributionTree(g_node).assignedWorker.get != worker) {
            findSecondaryAssignments(distributionTree, secondaryAssignments + (g_node -> (currentAssignees + worker)), nodeLocator)
          } else {
            // if it is already the primary worker it does not need to be added to the secondary Assignees as well
            findSecondaryAssignments(distributionTree, secondaryAssignments, nodeLocator)
          }
        }

      case SendSecondaryAssignments =>
        nodeLocatorHolder ! LocalSecondaryNodeAssignments(secondaryAssignments)
        Behaviors.stopped
    }

  def startDistribution(distributionTree: Map[Int, DistributionTreeInfo],
                        nodeLocator: NodeLocator[RedistributionEvent]): Behavior[RedistributionEvent] = {
    distributionTree.valuesIterator.foreach(nodeInfo => nodeInfo.stillToDistribute = nodeInfo.subTreeSize)
    distributeUsingTree(distributionTree, nodeLocator)
  }

  def assignNodesToWorker(waitingList: Seq[WaitingListEntry],
                          worker: ActorRef[SearchOnGraphEvent],
                          nodeLocator: NodeLocator[RedistributionEvent]): Unit = {
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
      val minNodesPerWorker = nodesToDistribute / workersLeft.size - nodesToDistribute / (100 * (workersLeft.size - 1))
      val maxNodesPerWorker = nodesToDistribute / workersLeft.size + nodesToDistribute / (100 * (workersLeft.size - 1))
      nodeLocator.findResponsibleActor(nextNodeInSearch) ! FindNodesInRange(nextNodeInSearch, minNodesPerWorker, maxNodesPerWorker, removeDescendants, Seq.empty, workersLeft, nodesToDistribute)
    }
  }
}
