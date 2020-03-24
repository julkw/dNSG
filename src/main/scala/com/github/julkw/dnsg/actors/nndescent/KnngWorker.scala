package com.github.julkw.dnsg.actors.nndescent

import akka.actor.typed.javadsl.TimerScheduler
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}

import math._
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import com.github.julkw.dnsg.util.{IndexTree, LeafNode, PositionTree, SplitNode, TreeBuilder, TreeNode}

import scala.collection.mutable


object KnngWorker {

  sealed trait BuildGraphEvent

  // setup
  final case class ResponsibleFor(responsibility: Seq[Int]) extends BuildGraphEvent

  // data distribution
  final case class DistributionInfo(treeNode: TreeNode[ActorRef[BuildGraphEvent]], sender: ActorRef[BuildGraphEvent]) extends BuildGraphEvent

  final case class DistributionTree(distributionTree: PositionTree) extends BuildGraphEvent

  // build approximate graph
  final case class FindCandidates(index: Int) extends BuildGraphEvent

  final case class GetCandidates(query: Seq[Float], index: Int, replyTo: ActorRef[BuildGraphEvent]) extends BuildGraphEvent

  final case class Candidates(candidates: Seq[Int], index: Int) extends BuildGraphEvent

  final case object FinishedApproximateGraph extends BuildGraphEvent

  // improve approximate graph
  final case object StartNNDescent extends BuildGraphEvent

  final case class PotentialNeighbors(g_node: Int, potentialNeighbors: Seq[(Int, Double)], senderIndex: Int) extends BuildGraphEvent

  final case class RemoveReverseNeighbor(g_nodeIndex: Int, neighborIndex: Int) extends BuildGraphEvent

  final case class AddReverseNeighbor(g_nodeIndex: Int, neighborIndex: Int) extends BuildGraphEvent

  final case class ListingResponse(listing: Receptionist.Listing) extends BuildGraphEvent

  val knngServiceKey: ServiceKey[BuildGraphEvent] = ServiceKey[BuildGraphEvent]("knngWorker")

  def apply(data: Seq[Seq[Float]],
            maxResponsibility: Int,
            k: Int,
            supervisor: ActorRef[BuildGraphEvent]): Behavior[BuildGraphEvent] = Behaviors.setup { ctx =>
    ctx.log.info("Started KnngWorker")
    Behaviors.withTimers(timers => new KnngWorker(data, maxResponsibility, k, supervisor, timers).buildDistributionTree())
  }
}

  class KnngWorker(data: Seq[Seq[Float]],
                   maxResponsibility: Int,
                   k: Int,
                   supervisor: ActorRef[KnngWorker.BuildGraphEvent],
                  timers: TimerScheduler[KnngWorker.BuildGraphEvent]) {
    import KnngWorker._
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
          ctx.log.info("Finished building kdTree")
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
          val graph: Map[Int, Seq[(Int, Double)]] = Map.empty
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
                              graph:Map[Int, Seq[(Int, Double)]]): Behavior[BuildGraphEvent] =
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
            val neighbors: Seq[(Int, Double)] = updatedCandidates(responsibilityIndex).map(candidateIndex =>
              (candidateIndex, euclideanDist(data(candidateIndex), data(graphIndex)))).sortBy(_._2).slice(0, k)
            val updatedGraph = graph + (graphIndex -> neighbors)
            if (updatedGraph.size == responsibility.length) {
              ctx.log.info("Should be able to switch to nnDescent now")
              supervisor ! FinishedApproximateGraph
            }
            buildApproximateGraph(kdTree, responsibility, updatedCandidates, awaitingAnswer, distributionTree, updatedGraph)
          } else {
            buildApproximateGraph(kdTree, responsibility, updatedCandidates, awaitingAnswer, distributionTree, graph)
          }

        case StartNNDescent =>
          startNNDescent(distributionTree, graph)

        // in case one of the other actors got the message slightly earlier and has already started sending me nnDescent messages
        case AddReverseNeighbor(g_nodeIndex, neighborIndex) =>
          ctx.self ! AddReverseNeighbor(g_nodeIndex, neighborIndex)
          startNNDescent(distributionTree, graph)

        case RemoveReverseNeighbor(g_nodeIndex, neighborIndex) =>
          ctx.self ! RemoveReverseNeighbor(g_nodeIndex, neighborIndex)
          startNNDescent(distributionTree, graph)

        case PotentialNeighbors(g_node, potentialNeighbors, senderIndex) =>
          ctx.self ! PotentialNeighbors(g_node, potentialNeighbors, senderIndex)
          startNNDescent(distributionTree, graph)
      }

    def startNNDescent(distributionTree: PositionTree,
                       graph: Map[Int, Seq[(Int, Double)]]): Behavior[BuildGraphEvent] = {

      ctx.log.info("Starting nnDescent")
      graph.foreach{ case (index, neighbors) =>
        // send all reverse neighbors to the correct actors
        neighbors.foreach{case (neighborIndex, _) =>
          distributionTree.findResponsibleActor(data(neighborIndex)) ! AddReverseNeighbor(neighborIndex, index)
        }
        // send all PotentialNeighbors to the correct actors
        val distances: Seq[(Int, Int, Double)] = neighbors.map(_._1).combinations(2).map(combination =>
          (combination(0), combination(1), euclideanDist(data(combination(0)), data(combination(1))))).toSeq
        neighbors.foreach{case (neighbor, _) =>
          val potentialNeighbors = distances.collect{ case(n1, n2, dist) if n1 == neighbor || n2 == neighbor =>
            if (n1 == neighbor) {
              (n2, dist)
            } else {
              (n1, dist)
            }
          }
          distributionTree.findResponsibleActor(data(neighbor)) ! PotentialNeighbors(neighbor, potentialNeighbors, index)
        }
      }
      val reverseNeighbors: Map[Int, Seq[Int]] = graph.map{case (index, _) => index -> Seq.empty}
      nnDescent(distributionTree, graph, reverseNeighbors)
    }

    def nnDescent(distributionTree: PositionTree,
                  graph: Map[Int, Seq[(Int, Double)]],
                  reverseNeighbors: Map[Int, Seq[Int]]): Behavior[BuildGraphEvent] =
      Behaviors.receiveMessage {
        case StartNNDescent =>
          // already done, so do nothing
          nnDescent(distributionTree, graph, reverseNeighbors)
        case PotentialNeighbors(g_node, potentialNeighbors, senderIndex) =>
          val currentNeighbors: Seq[(Int, Double)] = graph(g_node)
          val currentMaxDist: Double = currentNeighbors(currentNeighbors.length - 1)._2
          val probableNeighbors = potentialNeighbors.filter(_._2 < currentMaxDist)
          if (probableNeighbors.isEmpty) {
            // nothing changes
            nnDescent(distributionTree, graph, reverseNeighbors)
          } // else update neighbors
        val mergedNeighbors = (currentNeighbors ++: probableNeighbors).sortBy(_._2)
          val updatedNeighbors = mergedNeighbors.slice(0, k)
          // update the reverse neighbors of changed neighbors
          val newNeighbors = updatedNeighbors.intersect(probableNeighbors).map(_._1)
          newNeighbors.foreach {index =>
            distributionTree.findResponsibleActor(data(index)) ! AddReverseNeighbor(index, g_node)
          }
          val removedNeighbors = mergedNeighbors.slice(k, mergedNeighbors.length).intersect(currentNeighbors).map(_._1)
          removedNeighbors.foreach {index =>
            distributionTree.findResponsibleActor(data(index)) ! RemoveReverseNeighbor(index, g_node)
          }
          // send out new potential neighbors (join the remaining old neighbors with the new ones)
          val keptNeighbors = updatedNeighbors.intersect(currentNeighbors.diff(Seq(senderIndex))).map(_._1) ++: reverseNeighbors(g_node)
          // first calculate distances to prevent double calculation
          val potentialPairs = for {
            n1 <- newNeighbors
            n2 <- keptNeighbors
            dist = euclideanDist(data(n1), data(n2))
          } yield(n1, n2, dist)
          // introduce old neighbors to new neighbors
          // (the new neighbors do not have to be joined, as that will already have happened at the g_node the message originated from)
          newNeighbors.foreach { index =>
            val thisNodesPotentialNeighbors = potentialPairs.collect{case (n1, n2, dist) if n1== index => (n2, dist)}
            distributionTree.findResponsibleActor(data(index)) ! PotentialNeighbors(index, thisNodesPotentialNeighbors, g_node)
          }
          // introduce old neighbors to new neighbors
          keptNeighbors.foreach {index =>
            val thisNodesPotentialNeighbors = potentialPairs.collect{case (n1, n2, dist) if n2 == index => (n1, dist)}
            distributionTree.findResponsibleActor(data(index)) ! PotentialNeighbors(index, thisNodesPotentialNeighbors, g_node)
          }
          // update graph
          val updatedGraph = graph + (g_node -> updatedNeighbors)
          nnDescent(distributionTree, updatedGraph, reverseNeighbors)

        case AddReverseNeighbor(g_nodeIndex, neighborIndex) =>
          val allNeighbors = graph(g_nodeIndex).map(_._1) ++: reverseNeighbors(g_nodeIndex)
          // introduce the new neighbor to all other neighbors
          val potentialNeighbors = allNeighbors.map(index => (index, euclideanDist(data(index), data(neighborIndex))))
          distributionTree.findResponsibleActor(data(neighborIndex)) ! PotentialNeighbors(neighborIndex, potentialNeighbors, g_nodeIndex)
          // introduce all other neighbors to the new neighbor
          potentialNeighbors.foreach { case(index, distance) =>
            distributionTree.findResponsibleActor(data(index)) ! PotentialNeighbors(index, Seq((neighborIndex, distance)), g_nodeIndex)
          }
          // update reverse neighbors
          val updatedReverseNeighbors = reverseNeighbors(g_nodeIndex) :+ neighborIndex
          nnDescent(distributionTree, graph, reverseNeighbors + (g_nodeIndex -> updatedReverseNeighbors))

        case RemoveReverseNeighbor(g_nodeIndex, neighborIndex) =>
          val updatedReverseNeighbors = reverseNeighbors(g_nodeIndex).diff(Seq(neighborIndex))
          nnDescent(distributionTree, graph, reverseNeighbors + (g_nodeIndex -> updatedReverseNeighbors))
      }
  }

  def euclideanDist(pointX: Seq[Float], pointY: Seq[Float]): Double = {
    sqrt((pointX zip pointY).map { case (x,y) => pow(y - x, 2) }.sum)
  }

