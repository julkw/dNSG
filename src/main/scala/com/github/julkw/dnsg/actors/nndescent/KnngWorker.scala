package com.github.julkw.dnsg.actors.nndescent

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}

import math._
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import com.github.julkw.dnsg.util.{IndexTree, KdTree, LeafNode, PositionTree, SplitNode, TreeBuilder, TreeNode}

import scala.collection.mutable


object KnngWorker {

    sealed trait BuildGraphEvent
  // setup
  final case class ResponsibleFor(responsibility: Seq[Int]) extends BuildGraphEvent
  // data distribution
  final case class DistributionInfo(treeNode: TreeNode[ActorRef[BuildGraphEvent]], sender: ActorRef[BuildGraphEvent]) extends BuildGraphEvent
  final case class DistributionTree(distributionTree: PositionTree) extends  BuildGraphEvent
  // build approximate graph
  final case class FindCandidates(index: Int) extends BuildGraphEvent
  final case class GetCandidates(query: Seq[Float], index: Int, replyTo: ActorRef[BuildGraphEvent]) extends BuildGraphEvent
  final case class Candidates(candidates: Seq[Int], index: Int) extends BuildGraphEvent
  final case object FinishedApproximateGraph extends BuildGraphEvent
  // improve approximate graph
  final case class PotentialNeighbors(potentialNeighbors: Seq[Int]) extends BuildGraphEvent

  final case class ListingResponse(listing: Receptionist.Listing) extends BuildGraphEvent

  val knngServiceKey: ServiceKey[BuildGraphEvent] = ServiceKey[BuildGraphEvent]("knngWorker")

  def apply(data: Seq[Seq[Float]],
            maxResponsibility: Int,
            k: Int,
            supervisor: ActorRef[BuildGraphEvent]): Behavior[BuildGraphEvent] = Behaviors.setup { ctx =>
    ctx.log.info("Started KnngWorker")

    def buildDistributionTree(): Behavior[BuildGraphEvent] = Behaviors.receiveMessagePartial {
      case ResponsibleFor(responsibility) =>
        val treeBuilder: TreeBuilder = TreeBuilder(data, k)
        if(responsibility.length > maxResponsibility) {
          // further split the data
          val splitNode: SplitNode[Seq[Int]] = treeBuilder.oneLevelSplit(responsibility)
          val left = ctx.spawn(KnngWorker(data, maxResponsibility, k, ctx.self), name="KnngWorkerLeft")
          val right = ctx.spawn(KnngWorker(data, maxResponsibility, k, ctx.self), name="KnngWorkerRight")
          left ! ResponsibleFor(splitNode.left.data)
          right ! ResponsibleFor(splitNode.right.data)
          combineDistributionTree(left, right, None, None, splitNode.dimension, splitNode.border)
        } else {
          // this is a leaf node for data distribution
          ctx.system.receptionist ! Receptionist.Register(knngServiceKey, ctx.self)
          val locationNode: LeafNode[ActorRef[BuildGraphEvent]] = LeafNode(ctx.self)
          supervisor ! DistributionInfo(locationNode, ctx.self)

          // Build local tree while waiting on DistributionTree message
          val treeBuilder: TreeBuilder = TreeBuilder(data, k)
          val kdTree: IndexTree = treeBuilder.construct(responsibility)

          waitForDistributionInfo(kdTree, responsibility)
        }
    }

    def combineDistributionTree(left: ActorRef[BuildGraphEvent],
                                right: ActorRef[BuildGraphEvent],
                                leftNode: Option[TreeNode[ActorRef[BuildGraphEvent]]],
                                rightNode: Option[TreeNode[ActorRef[BuildGraphEvent]]],
                                dimension: Int, border: Float): Behavior[BuildGraphEvent] =
      Behaviors.receiveMessagePartial {
          case DistributionInfo(treeNode, sender) =>
            sender match {
              case `left` =>
                rightNode match {
                  case None =>
                    combineDistributionTree(left, right, Option(treeNode), rightNode, dimension, border)
                  case Some(node) =>
                    val combinedNode: SplitNode[ActorRef[BuildGraphEvent]] = SplitNode(treeNode, node, dimension, border)
                    supervisor ! DistributionInfo(combinedNode, ctx.self)
                    waitForApproximateGraphs(0)
                }
              case `right` =>
                leftNode match {
                  case None =>
                    combineDistributionTree(left, right, leftNode, Option(treeNode), dimension, border)
                  case Some(node) =>
                    val combineNode: SplitNode[ActorRef[BuildGraphEvent]] = SplitNode(node, treeNode, dimension, border)
                    supervisor ! DistributionInfo(combineNode, ctx.self)
                    waitForApproximateGraphs(0)
                }
            }
      }


    def waitForApproximateGraphs(finishedGraphs: Int): Behavior[BuildGraphEvent] =
      Behaviors.receiveMessagePartial {
        case FinishedApproximateGraph =>
          if (finishedGraphs > 0) {
            supervisor ! FinishedApproximateGraph
          }
          waitForApproximateGraphs(finishedGraphs + 1)
      }

    def waitForDistributionInfo(kdTree: IndexTree,
                                responsibility: Seq[Int]): Behavior[BuildGraphEvent] =
      Behaviors.receiveMessagePartial {
          case DistributionTree(distributionTree) =>
            ctx.log.info("Received Distribution Tree. Start building approximate graph")
            ctx.self ! FindCandidates(0)
            val candidates: Map[Int, Seq[Int]] = Map.empty
            val awaitingAnswer: Array[Int] = Array.fill(responsibility.length){0}
            val graph: Map[Int, Seq[Int]] = Map.empty
            buildApproximateGraph(kdTree, responsibility, candidates, awaitingAnswer, distributionTree, graph)
          case GetCandidates(query, index, sender) =>
            // TODO forward to self with timer
            ctx.log.info("Part of the problem seems to be getting the GetCandidates Message too early")
            waitForDistributionInfo(kdTree, responsibility)
      }


    def buildApproximateGraph(kdTree: IndexTree,
                              responsibility: Seq[Int],
                              candidates: Map [Int, Seq[Int]],
                              awaitingAnswer: Array[Int],
                              distributionTree: PositionTree,
                              graph: Map[Int, Seq[Int]]): Behavior[BuildGraphEvent] =
      Behaviors.receiveMessagePartial {
        case DistributionTree(distributionTree) =>
          ctx.log.info("Received Distribution Tree. Start building approximate graph")
          ctx.self ! FindCandidates(0)
          buildApproximateGraph(kdTree, responsibility, candidates, awaitingAnswer, distributionTree, graph)

        case FindCandidates(responsibilityIndex) =>
          // Also look for the closest neighbors of the other g_nodes
          if (responsibilityIndex < responsibility.length - 1) {
            ctx.self ! FindCandidates(responsibilityIndex + 1)
          }
          // find candidates for current node
          val query: Seq[Float] = data(responsibility(responsibilityIndex))
          var currentActorNode = distributionTree.root
          // Find actors to ask for candidates
          while (currentActorNode.inverseQueryChild(query) != currentActorNode) {
            val actorToAsk: ActorRef[BuildGraphEvent] = currentActorNode.inverseQueryChild(query).queryLeaf(query).data
            actorToAsk ! GetCandidates(query, responsibilityIndex, ctx.self)
            awaitingAnswer(responsibilityIndex) += 1
            currentActorNode = currentActorNode.queryChild(query)
          }
          // Find candidates on own tree
          var currentDataNode: TreeNode[Seq[Int]] = kdTree.root
          var localCandidates: Seq[Int] = Seq.empty
          while (currentDataNode.inverseQueryChild(query) != currentDataNode) {
            localCandidates = localCandidates ++ currentDataNode.inverseQueryChild(query).queryLeaf(query).data
            currentDataNode = currentDataNode.queryChild(query)
          }
          localCandidates = localCandidates ++ currentDataNode.data
          ctx.self ! Candidates(localCandidates, responsibilityIndex)
          awaitingAnswer(responsibilityIndex) += 1
          buildApproximateGraph(kdTree, responsibility, candidates, awaitingAnswer, distributionTree, graph)

        case GetCandidates(query, index, sender) =>
          val localCandidates: Seq[Int] = kdTree.root.queryLeaf(query).data
          sender ! Candidates(localCandidates, index)
          buildApproximateGraph(kdTree, responsibility, candidates, awaitingAnswer, distributionTree, graph)

        case Candidates(remoteCandidates, responsibilityIndex) =>
          val oldCandidates = candidates.getOrElse(responsibilityIndex, Seq.empty)
          val updatedCandidates = candidates + (responsibilityIndex -> (oldCandidates ++ remoteCandidates))
          awaitingAnswer(responsibilityIndex) -= 1
          if (awaitingAnswer(responsibilityIndex) == 0) {
            val graphIndex: Int = responsibility(responsibilityIndex)
            val neighbors: Seq[Int] = updatedCandidates(responsibilityIndex).sortBy(candidateIndex => euclideanDist(data(candidateIndex), data(graphIndex))).slice(0, k)
            val updatedGraph = graph + (graphIndex -> neighbors)
            if (updatedGraph.size == responsibility.length) {
              ctx.log.info("Should be able to switch to nnDescent now")
              supervisor ! FinishedApproximateGraph
              // TODO: Protocol for when to switch to nnDescent
            }
            buildApproximateGraph(kdTree, responsibility, updatedCandidates, awaitingAnswer, distributionTree, updatedGraph)
          } else {
            buildApproximateGraph(kdTree, responsibility, updatedCandidates, awaitingAnswer, distributionTree, graph)
          }
      }

    def nnDescent(k: Int,
                  supervisor: ActorRef[BuildGraphEvent]): Behavior[BuildGraphEvent] =
      Behaviors.receiveMessage {
        case PotentialNeighbors(potentialNeighbors) =>
          // TODO check them for viability and react accordingly
          nnDescent(k, supervisor)
      }


    // Start the graph building process
    buildDistributionTree()
  }

  def euclideanDist(pointX: Seq[Float], pointY: Seq[Float]): Double = {
    sqrt((pointX zip pointY).map { case (x,y) => pow(y - x, 2) }.sum)
  }

}
