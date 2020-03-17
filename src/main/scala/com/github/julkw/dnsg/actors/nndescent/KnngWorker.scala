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

  val subGraph: mutable.Map[Int, Seq[Int]] = mutable.Map[Int, Seq[Int]]()
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
          waitForDistributionInfo(responsibility)
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


    def waitForApproximateGraphs(finishedGraphs: Int): Behavior[BuildGraphEvent] = Behaviors.receiveMessagePartial {
      case FinishedApproximateGraph =>
        if (finishedGraphs > 0) {
          supervisor ! FinishedApproximateGraph
        } else {
          waitForApproximateGraphs(finishedGraphs + 1)
        }
      // this node has nothing more to do for now
      Behaviors.empty
    }

    def waitForDistributionInfo(responsibility: Seq[Int]): Behavior[BuildGraphEvent] = {
      // build the rest of the kdTree for candidate generation
      // can be done here since this function should only be called once
      val treeBuilder: TreeBuilder = TreeBuilder(data, k)
      val kdTree: IndexTree = treeBuilder.construct(responsibility)
      val candidates: Map[Int, Seq[Int]] = Map.empty
      val awaitingAnswer: Array[Int] = Array.fill(responsibility.length){0}
      Behaviors.receiveMessagePartial {
          case DistributionTree(distributionTree) =>
            ctx.log.info("Received Distribution Tree. Start building approximate graph")
            ctx.self ! FindCandidates(0)
            buildApproximateGraph(kdTree, responsibility, candidates, awaitingAnswer, distributionTree)
      }
    }

    def buildApproximateGraph(kdTree: IndexTree,
                              responsibility: Seq[Int],
                              candidates: Map [Int, Seq[Int]],
                              awaitingAnswer: Array[Int],
                              distributionTree: PositionTree): Behavior[BuildGraphEvent] =
      Behaviors.receiveMessagePartial {
        case FindCandidates(index) =>
          // Also look for the closest neighbors of the other g_nodes
          if (index < responsibility.length - 1) {
            ctx.self ! FindCandidates(index + 1)
          }
          // find candidates for current node
          val query: Seq[Float] = data(responsibility(index))
          var currentActorNode = distributionTree.root
          // Find actors to ask for candidates
          while (currentActorNode.inverseQueryChild(query) != currentActorNode) {
            val actorToAsk: ActorRef[BuildGraphEvent] = currentActorNode.inverseQueryChild(query).queryLeaf(query).data
            actorToAsk ! GetCandidates(query, index, ctx.self)
            awaitingAnswer(index) += 1
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
          val updatedCandidates = candidates + (index -> localCandidates)
          buildApproximateGraph(kdTree, responsibility, updatedCandidates, awaitingAnswer, distributionTree)

        case GetCandidates(query, index, sender) =>
          sender ! Candidates(kdTree.root.queryLeaf(query).data, index)
          Behaviors.same

        case Candidates(remoteCandidates, index) =>
          val updatedCandidates = candidates + (index -> remoteCandidates)
          awaitingAnswer(index) -= 1
          if (awaitingAnswer(index) == 0) {
            val graphIndex: Int = responsibility(index)
            val neighbors: Seq[Int] = candidates(index).sortBy(candidateIndex => euclideanDist(data(candidateIndex), data(graphIndex))).slice(0, k)
            subGraph += (graphIndex -> neighbors)
            if (subGraph.size == responsibility.length) {
              // TODO approximate graph seems to be done
              // TODO tell supervisor and switch to nnDescent to improve on graph
              nnDescent(k, supervisor)
            }
          }
          buildApproximateGraph(kdTree, responsibility, updatedCandidates, awaitingAnswer, distributionTree)
        case PotentialNeighbors(potentialNeighbors) =>
          // TODO: forward to self with timer?
          Behaviors.unhandled
    }

    def nnDescent(k: Int,
                  supervisor: ActorRef[BuildGraphEvent]): Behavior[BuildGraphEvent] = {
      ctx.log.info("Entered into NNDescent stage")
      Behaviors.receiveMessage {
        case PotentialNeighbors(potentialNeighbors) =>
          // TODO check them for viability and react accordingly
          Behaviors.same
      }
    }

    // Start the graph building process
    buildDistributionTree()
  }

  def euclideanDist(pointX: Seq[Float], pointY: Seq[Float]): Double = {
    sqrt((pointX zip pointY).map { case (x,y) => pow(y - x, 2) }.sum)
  }

}
