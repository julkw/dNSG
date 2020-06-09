package com.github.julkw.dnsg.actors

import scala.concurrent.duration._
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.{CoordinationEvent, RedistributerDistributionInfo, RedistributionNodeAssignments}
import com.github.julkw.dnsg.actors.GraphConnector.CTreeNode
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.SearchOnGraphEvent
import com.github.julkw.dnsg.util.{NodeLocator, dNSGSerializable}

import scala.collection.mutable

object GraphRedistributer {

  sealed trait RedistributionEvent extends dNSGSerializable

  final case class DistributeData(workers: Set[ActorRef[SearchOnGraphEvent]], dataReplication: DataReplicationModel, nodeLocator: NodeLocator[ActorRef[RedistributionEvent]]) extends RedistributionEvent

  // calculating subtree-sizes
  final case class ChildSubtreeSize(g_node: Int, childIndex: Int, childSubtreeSize: Int) extends RedistributionEvent

  // Search for nodes to assign
  final case class FindNodesInRange(g_node: Int, minNodes: Int, maxNodes: Int, removeFromDescendants: Int, waitingList: Seq[WaitingListEntry], workersLeft: Set[ActorRef[SearchOnGraphEvent]], nodesLeft: Int) extends RedistributionEvent

  // assigning nodes to workers
  final case class AssignWithChildren(g_node: Int, worker: ActorRef[SearchOnGraphEvent]) extends RedistributionEvent

  final case class AssignWithParents(g_node: Int, worker: ActorRef[SearchOnGraphEvent]) extends RedistributionEvent

  final case object SendResults extends RedistributionEvent

  // data structures for more readable code
  protected case class DistributionTreeInfo(var subTreeSize: Int, var stillToDistribute: Int, var waitingForResponses: Int, assignedWorkers: mutable.Set[ActorRef[SearchOnGraphEvent]])

  protected case class WaitingListEntry(g_node: Int, subTreeSize: Int)

  trait DataReplicationModel

  case object NoReplication extends DataReplicationModel

  case object OnlyParentsReplication extends DataReplicationModel

  case object AllSharedReplication extends DataReplicationModel

  def apply(tree: Map[Int, CTreeNode], clusterCoordinator: ActorRef[CoordinationEvent]): Behavior[RedistributionEvent] = Behaviors.setup(ctx =>
    Behaviors.withTimers(timers =>
      new GraphRedistributer(tree, clusterCoordinator, timers, ctx).setup()
    )
  )
}

class GraphRedistributer(tree: Map[Int, CTreeNode],
                         clusterCoordinator: ActorRef[CoordinationEvent],
                         timers: TimerScheduler[GraphRedistributer.RedistributionEvent],
                         ctx: ActorContext[GraphRedistributer.RedistributionEvent]) {
  import GraphRedistributer._

  def setup(): Behavior[RedistributionEvent] = {
    clusterCoordinator ! RedistributerDistributionInfo(tree.keys.toSeq, ctx.self)
    val distributionTree: Map[Int, DistributionTreeInfo] = tree.transform((index, treeNode) => DistributionTreeInfo(1, 0, treeNode.children.size, mutable.Set.empty))
    waitForStartSignal(distributionTree)
  }

  def waitForStartSignal(distributionTree: Map[Int, DistributionTreeInfo]): Behavior[RedistributionEvent] = Behaviors.receiveMessagePartial {
    case DistributeData(workers, dataReplication, nodeLocator) =>
      ctx.log.info("Starting redistribution")
      tree.foreach { case (node, nodeInfo) =>
        if (nodeInfo.children.isEmpty) {
          nodeLocator.findResponsibleActor(nodeInfo.parent) ! ChildSubtreeSize(nodeInfo.parent, node, 1)
        }
      }
      calculateNodeSizes(distributionTree, workers, dataReplication, nodeLocator)

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
                         replicationStrategy: DataReplicationModel,
                         nodeLocator: NodeLocator[ActorRef[RedistributionEvent]]): Behavior[RedistributionEvent] =
    Behaviors.receiveMessagePartial {
      case ChildSubtreeSize(g_node, childIndex, childSubtreeSize) =>
        val currentNode = distributionTree(g_node)
        currentNode.subTreeSize += childSubtreeSize
        currentNode.waitingForResponses -= 1
        ctx.log.info("{} is still waiting for {} responses", g_node, currentNode.waitingForResponses)
        if (currentNode.waitingForResponses == 0) {
          val currentParent = tree(g_node).parent
          if (currentParent == g_node) {
            // this is the root
            assert(currentNode.subTreeSize == nodeLocator.graphSize)
            startSearchForNextWorkersNodes(g_node, 0, nodeLocator.graphSize, workers, replicationStrategy, nodeLocator)
            startDistribution(distributionTree, workers, replicationStrategy, nodeLocator)
          } else {
            nodeLocator.findResponsibleActor(currentParent) ! ChildSubtreeSize(currentParent, g_node, currentNode.subTreeSize)
            calculateNodeSizes(distributionTree, workers, replicationStrategy, nodeLocator)
          }
        } else {
          calculateNodeSizes(distributionTree, workers, replicationStrategy, nodeLocator)
        }

      case FindNodesInRange(g_node, minNodes, maxNodes, removeFromDescendants, waitingList, workersLeft, nodesLeft) =>
        ctx.self ! FindNodesInRange(g_node, minNodes, maxNodes, removeFromDescendants, waitingList, workersLeft, nodesLeft)
        startDistribution(distributionTree, workers, replicationStrategy, nodeLocator)

      case AssignWithChildren(g_node, worker) =>
        ctx.self ! AssignWithChildren(g_node, worker)
        startDistribution(distributionTree, workers, replicationStrategy, nodeLocator)
    }

  def distributeUsingTree(distributionTree: Map[Int, DistributionTreeInfo],
                          replicationStrategy: DataReplicationModel,
                          workers: Set[ActorRef[SearchOnGraphEvent]],
                          nodeLocator: NodeLocator[ActorRef[RedistributionEvent]]): Behavior[RedistributionEvent] =
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
          assignNodesToWorker(updatedWaitingList, worker, replicationStrategy, nodeLocator)
          startSearchForNextWorkersNodes(parent, treeNode.subTreeSize, nodesLeft - withMe, workersLeft - worker, replicationStrategy, nodeLocator)
        } else {
          // TODO instead of choosing the first child in the list, choose the one closest to the last entry in the waitingList?
          // this would need exact locations, though
          // TODO this break again. Find out why
          val child = tree(g_node).children.head
          // remove the child because the next time the search reaches me it will have been assigned something or be in the waiting list
          tree(g_node).children -= child
          nodeLocator.findResponsibleActor(child) ! FindNodesInRange(child, minNodes, maxNodes, 0, waitingList, workersLeft, nodesLeft)
        }
        distributeUsingTree(distributionTree, replicationStrategy, workers, nodeLocator)

      case AssignWithChildren(g_node, worker) =>
        distributionTree(g_node).assignedWorkers += worker
        tree(g_node).children.foreach(child => nodeLocator.findResponsibleActor(child) ! AssignWithChildren(child, worker))
        distributeUsingTree(distributionTree, replicationStrategy, workers, nodeLocator)

      case AssignWithParents(g_node, worker) =>
        val parent = tree(g_node).parent
        distributionTree(g_node).assignedWorkers += worker
        if (parent != g_node) {
          nodeLocator.findResponsibleActor(parent) ! AssignWithParents(parent, worker)
        } else if (distributionTree(g_node).assignedWorkers.size == workers.size) {
          // TODO this is not a good test as multiple g_nodes will propagate the same worker up to root, so this only ensures that one of those has reached
          ctx.log.info("root is now assigned to all workers")
          // check if all AssignWithParents messages are done
          nodeLocator.allActors().foreach(redistributer => redistributer ! SendResults)
        }
        distributeUsingTree(distributionTree, replicationStrategy, workers, nodeLocator)

      case SendResults =>
        // just for debugging purposes
        val leftToAssign = distributionTree.valuesIterator.filter(treeNode => treeNode.assignedWorkers.isEmpty)
        //ctx.log.info("Still waiting on {} nodes to be assigned", leftToAssign.toSeq)
        // ensure all AssignWithChildren messages have been processed (all nodes have been assigned to a worker)
        if (distributionTree.valuesIterator.exists(treeNode => treeNode.assignedWorkers.isEmpty)) {
          // TODO do this with timer
          ctx.self ! SendResults
          //distributeUsingTree(distributionTree,  replicationStrategy, workers, nodeLocator)
        } else {
          ctx.log.info("Send assignments to clusterCoordinator")
          if (replicationStrategy == AllSharedReplication) {
            val updatedDistInfo = distributionTree.transform {(_, treeNode) =>
              if (treeNode.assignedWorkers.size > 1) {
                workers
              } else {
                treeNode.assignedWorkers.toSet
              }
            }
            // TODO maybe add check that this has been received?
            clusterCoordinator ! RedistributionNodeAssignments(updatedDistInfo)
            //Behaviors.stopped
          } else {
            val distributionInfo = distributionTree.transform((_, treeNode) => treeNode.assignedWorkers.toSet)
            clusterCoordinator ! RedistributionNodeAssignments(distributionInfo)
            //Behaviors.stopped
          }
        }
        distributeUsingTree(distributionTree,  replicationStrategy, workers, nodeLocator)
    }

  def startDistribution(distributionTree: Map[Int, DistributionTreeInfo],
                        workers: Set[ActorRef[SearchOnGraphEvent]],
                        replicationStrategy: DataReplicationModel,
                        nodeLocator: NodeLocator[ActorRef[RedistributionEvent]]): Behavior[RedistributionEvent] = {
    ctx.log.info("Done with tree, now assigning g_nodes to workers")
    distributionTree.valuesIterator.foreach(nodeInfo => nodeInfo.stillToDistribute = nodeInfo.subTreeSize)
    distributeUsingTree(distributionTree, replicationStrategy, workers, nodeLocator)
  }

  def assignNodesToWorker(waitingList: Seq[WaitingListEntry],
                          worker: ActorRef[SearchOnGraphEvent],
                          replicationStrategy: DataReplicationModel,
                          nodeLocator: NodeLocator[ActorRef[RedistributionEvent]]): Unit = {
    val inList = waitingList.map(_.g_node)
    ctx.log.info("WL: {}", inList)
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
                                     replicationStrategy: DataReplicationModel,
                                     nodeLocator: NodeLocator[ActorRef[RedistributionEvent]]): Unit = {
    if (workersLeft.isEmpty) {
      ctx.log.info("All workers assigned")
      if (replicationStrategy == NoReplication) {
        nodeLocator.allActors().foreach(redistributer => redistributer ! SendResults)
      }
    } else if (workersLeft.size == 1) {
      ctx.log.info("Down to last worker")
      nodeLocator.findResponsibleActor(nextNodeInSearch) ! FindNodesInRange(nextNodeInSearch, nodesToDistribute, nodesToDistribute, removeDescendants, Seq.empty, workersLeft, nodesToDistribute)
    } else {
      // TODO test different ranges?
      val minNodesPerWorker = nodesToDistribute / workersLeft.size - nodesToDistribute / (100 * (workersLeft.size - 1))
      val maxNodesPerWorker = nodesToDistribute / workersLeft.size + nodesToDistribute / (100 * (workersLeft.size - 1))
      ctx.log.info("{} graph_nodes for {} workers left. Next range {} to {}", nodesToDistribute, workersLeft.size, minNodesPerWorker, maxNodesPerWorker)
      nodeLocator.findResponsibleActor(nextNodeInSearch) ! FindNodesInRange(nextNodeInSearch, minNodesPerWorker, maxNodesPerWorker, removeDescendants, Seq.empty, workersLeft, nodesToDistribute)
    }
  }
}
