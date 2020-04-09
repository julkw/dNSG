package com.github.julkw.dnsg.actors.nndescent

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}

import math._
import scala.concurrent.duration._
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import com.github.julkw.dnsg.actors.SearchOnGraph
import com.github.julkw.dnsg.actors.SearchOnGraph.{Graph, GraphReceived, SearchOnGraphEvent}
import com.github.julkw.dnsg.util.{Distance, IndexTree, LeafNode, NodeLocator, SplitNode, TreeBuilder, TreeNode}

import scala.collection.mutable
import scala.language.postfixOps


object KnngWorker {

  sealed trait BuildGraphEvent

  // setup
  final case class ResponsibleFor(responsibility: Seq[Int]) extends BuildGraphEvent

  // data distribution
  final case class DistributionInfo(treeNode: TreeNode[ActorRef[BuildGraphEvent]], sender: ActorRef[BuildGraphEvent]) extends BuildGraphEvent

  final case class BuildApproximateGraph(nodeLocator: NodeLocator[BuildGraphEvent]) extends BuildGraphEvent

  // build approximate graph
  final case class FindCandidates(index: Int) extends BuildGraphEvent

  final case class GetCandidates(query: Seq[Float], index: Int, replyTo: ActorRef[BuildGraphEvent]) extends BuildGraphEvent

  final case class Candidates(candidates: Seq[Int], index: Int) extends BuildGraphEvent

  final case object FinishedApproximateGraph extends BuildGraphEvent

  // improve approximate graph
  final case object StartNNDescent extends BuildGraphEvent

  final case class CompleteLocalJoin(g_node: Int) extends BuildGraphEvent

  final case class PotentialNeighbors(g_node: Int, potentialNeighbors: Seq[(Int, Double)], senderIndex: Int) extends BuildGraphEvent

  final case class RemoveReverseNeighbor(g_nodeIndex: Int, neighborIndex: Int) extends BuildGraphEvent

  final case class AddReverseNeighbor(g_nodeIndex: Int, neighborIndex: Int) extends BuildGraphEvent

  final case class ListingResponse(listing: Receptionist.Listing) extends BuildGraphEvent

  final case object FinishedNNDescent extends BuildGraphEvent

  final case object CorrectFinishedNNDescent extends BuildGraphEvent

  private case object NNDescentTimeout extends  BuildGraphEvent

  private case object NNDescentTimerKey

  // give final graph to SearchOnGraph Actor
  final case class StartSearchOnGraph(graphHolder: ActorRef[SearchOnGraphEvent]) extends BuildGraphEvent

  final case class WrappedSearchOnGraphEvent(event: SearchOnGraph.SearchOnGraphEvent) extends BuildGraphEvent

  final case class SOGDistributionInfo(treeNode: TreeNode[ActorRef[SearchOnGraphEvent]], sender: ActorRef[BuildGraphEvent]) extends BuildGraphEvent


  val knngServiceKey: ServiceKey[BuildGraphEvent] = ServiceKey[BuildGraphEvent]("knngWorker")
  // TODO do over input
  val timeoutAfter = 3.second

  def apply(data: Seq[Seq[Float]],
            maxResponsibility: Int,
            k: Int,
            supervisor: ActorRef[BuildGraphEvent]): Behavior[BuildGraphEvent] = Behaviors.setup { ctx =>
    ctx.log.info("Started KnngWorker")
    Behaviors.withTimers(timers => new KnngWorker(data, maxResponsibility, k, supervisor, timers, ctx).buildDistributionTree())
  }
}

class KnngWorker(data: Seq[Seq[Float]],
                 maxResponsibility: Int,
                 k: Int,
                 supervisor: ActorRef[KnngWorker.BuildGraphEvent],
                 timers: TimerScheduler[KnngWorker.BuildGraphEvent],
                 ctx: ActorContext[KnngWorker.BuildGraphEvent]) extends Distance {
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
                              dimension: Int,
                              border: Float): Behavior[BuildGraphEvent] =
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
                waitForApproximateGraphs(0, 0, left, right, combinedNode)
            }
          case `right` =>
            leftNode match {
              case None =>
                combineDistributionTree(left, right, leftNode, Option(treeNode), dimension, border)
              case Some(node) =>
                val combinedNode: SplitNode[ActorRef[BuildGraphEvent]] = SplitNode(node, treeNode, dimension, border)
                supervisor ! DistributionInfo(combinedNode, ctx.self)
                waitForApproximateGraphs(0, 0, left, right, combinedNode)
            }
        }
    }


  def waitForApproximateGraphs(finishedGraphs: Int,
                               finishedNNDescent: Int,
                               leftChild: ActorRef[BuildGraphEvent],
                               rightChild: ActorRef[BuildGraphEvent],
                               splitNode: SplitNode[ActorRef[BuildGraphEvent]]): Behavior[BuildGraphEvent] =
    Behaviors.receiveMessagePartial {
      case FinishedApproximateGraph =>
        if (finishedGraphs > 0) {
          supervisor ! FinishedApproximateGraph
        }
        waitForApproximateGraphs(finishedGraphs + 1, 0, leftChild, rightChild, splitNode)

      case FinishedNNDescent =>
        if (finishedNNDescent > 0) {
          supervisor ! FinishedNNDescent
        }
        waitForApproximateGraphs(finishedGraphs, finishedNNDescent + 1, leftChild, rightChild, splitNode)

      case CorrectFinishedNNDescent =>
        if (finishedNNDescent == 2) {
          // if the wrong information has already been sent up, correct it
          supervisor ! CorrectFinishedNNDescent
        }
        waitForApproximateGraphs(finishedGraphs, finishedNNDescent - 1, leftChild, rightChild, splitNode)

      case SOGDistributionInfo(treeNode, sender) =>
        // only switch states here, where it is relatively certain, that nndescent is globally done
        ctx.self ! SOGDistributionInfo(treeNode, sender)
        combineSOGDistributionTree(leftChild, rightChild, None, None, splitNode)
    }

  def waitForDistributionInfo(kdTree: IndexTree,
                              responsibility: Seq[Int]): Behavior[BuildGraphEvent] =
    Behaviors.receiveMessagePartial {
      case BuildApproximateGraph(nodeLocator) =>
        ctx.log.info("Received Distribution Tree. Start building approximate graph")
        ctx.self ! FindCandidates(0)
        val candidates: Map[Int, Seq[Int]] = Map.empty
        val awaitingAnswer: Array[Int] = Array.fill(responsibility.length){0}
        val graph: Map[Int, Seq[(Int, Double)]] = Map.empty
        buildApproximateGraph(kdTree, responsibility, candidates, awaitingAnswer, nodeLocator, graph)
      case GetCandidates(query, index, sender) =>
        // TODO forward to self with timer
        val getCandidateKey = "GetCandidates"
        timers.startSingleTimer(getCandidateKey, GetCandidates(query, index, sender), 1.second)
        ctx.log.info("Got a request for candidates before the distribution info. Forwarded to self with delay.")
        waitForDistributionInfo(kdTree, responsibility)
    }


  def buildApproximateGraph(kdTree: IndexTree,
                            responsibility: Seq[Int],
                            candidates: Map [Int, Seq[Int]],
                            awaitingAnswer: Array[Int],
                            nodeLocator: NodeLocator[BuildGraphEvent],
                            graph:Map[Int, Seq[(Int, Double)]]): Behavior[BuildGraphEvent] =
    Behaviors.receiveMessagePartial {
      case BuildApproximateGraph(distributionTree) =>
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
        var currentActorNode = nodeLocator.positionTree.root
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
        buildApproximateGraph(kdTree, responsibility, candidates, awaitingAnswer, nodeLocator, graph)

      case GetCandidates(query, index, sender) =>
        val localCandidates: Seq[Int] = kdTree.root.queryLeaf(query).data
        sender ! Candidates(localCandidates, index)
        buildApproximateGraph(kdTree, responsibility, candidates, awaitingAnswer, nodeLocator, graph)

      case Candidates(remoteCandidates, responsibilityIndex) =>
        val oldCandidates = candidates.getOrElse(responsibilityIndex, Seq.empty)
        val updatedCandidates = candidates + (responsibilityIndex -> (oldCandidates ++ remoteCandidates))
        awaitingAnswer(responsibilityIndex) -= 1
        if (awaitingAnswer(responsibilityIndex) == 0) {
          val graphIndex: Int = responsibility(responsibilityIndex)
          val neighbors: Seq[(Int, Double)] = updatedCandidates(responsibilityIndex).map(candidateIndex =>
            // 1 and k+1 so the g_node itself is not added to its own neighbors
            (candidateIndex, euclideanDist(data(candidateIndex), data(graphIndex)))).sortBy(_._2).slice(1, k+1)
          val updatedGraph = graph + (graphIndex -> neighbors)
          if (updatedGraph.size == responsibility.length) {
            ctx.log.info("Should be able to switch to nnDescent now")
            supervisor ! FinishedApproximateGraph
          }
          buildApproximateGraph(kdTree, responsibility, updatedCandidates, awaitingAnswer, nodeLocator, updatedGraph)
        } else {
          buildApproximateGraph(kdTree, responsibility, updatedCandidates, awaitingAnswer, nodeLocator, graph)
        }

      case StartNNDescent =>
        startNNDescent(nodeLocator, graph)

      // in case one of the other actors got the message slightly earlier and has already started sending me nnDescent messages
      case AddReverseNeighbor(g_nodeIndex, neighborIndex) =>
        ctx.self ! AddReverseNeighbor(g_nodeIndex, neighborIndex)
        startNNDescent(nodeLocator, graph)

      case RemoveReverseNeighbor(g_nodeIndex, neighborIndex) =>
        ctx.self ! RemoveReverseNeighbor(g_nodeIndex, neighborIndex)
        startNNDescent(nodeLocator, graph)

      case PotentialNeighbors(g_node, potentialNeighbors, senderIndex) =>
        ctx.self ! PotentialNeighbors(g_node, potentialNeighbors, senderIndex)
        startNNDescent(nodeLocator, graph)
    }

  def startNNDescent(nodeLocator: NodeLocator[BuildGraphEvent],
                     graph: Map[Int, Seq[(Int, Double)]]): Behavior[BuildGraphEvent] = {
    ctx.log.info("Starting nnDescent")
    graph.foreach{ case (index, neighbors) =>
      // do the initial local joins through messages to self to prevent Heartbeat problems
      ctx.self ! CompleteLocalJoin(index)
      // send all reverse neighbors to the correct actors
      neighbors.foreach{case (neighborIndex, _) =>
        nodeLocator.findResponsibleActor(data(neighborIndex)) ! AddReverseNeighbor(neighborIndex, index)
      }
    }
    // setup timer used to determine when the graph is done
    timers.startSingleTimer(NNDescentTimerKey, NNDescentTimeout, timeoutAfter)
    // start nnDescent
    val reverseNeighbors: Map[Int, Seq[Int]] = graph.map{case (index, _) => index -> Seq.empty}
    val allNeighborCombinations = k * (k - 1) / 2
    val reusableDistanceStorage = Array.fill(allNeighborCombinations){(0, 0, 0d)}
    nnDescent(nodeLocator, graph, reverseNeighbors, reusableDistanceStorage)
  }

  def nnDescent(nodeLocator: NodeLocator[BuildGraphEvent],
                graph: Map[Int, Seq[(Int, Double)]],
                reverseNeighbors: Map[Int, Seq[Int]],
                reusableDistanceStorage: Array[(Int, Int, Double)]): Behavior[BuildGraphEvent] =
    Behaviors.receiveMessage {
      case StartNNDescent =>
        // already done, so do nothing
        nnDescent(nodeLocator, graph, reverseNeighbors, reusableDistanceStorage)

      case CompleteLocalJoin(g_node) =>
        ctx.log.info("Doing local join for {}", g_node)
        // prevent timeouts in the initial phase of graph nnDescent
        timers.cancel(NNDescentTimerKey)
        //ctx.log.info("Still working on initial local joins")
        val neighbors = graph(g_node)
        // send all PotentialNeighbors to the correct actors
        //val distances: Seq[(Int, Int, Double)] = neighbors.map(_._1).combinations(2).map(combination =>
        //  (combination(0), combination(1), euclideanDist(data(combination(0)), data(combination(1))))).toSeq

        var nextDistanceIndex = 0
        for (n1 <- 0 until k) {
          for (n2 <- n1 + 1 until k) {
            reusableDistanceStorage(nextDistanceIndex) = (n1, n2, euclideanDist(data(n1), data(n2)))
            nextDistanceIndex += 1
          }
        }

        //val reusableNeighborStorage = Array.fill(k - 1){(0, 0d)}
        neighbors.foreach{case (neighbor, _) =>
          val potentialNeighbors = reusableDistanceStorage.collect{ case(n1, n2, dist) if n1 == neighbor || n2 == neighbor =>
            if (n1 == neighbor) {
              (n2, dist)
            } else {
              (n1, dist)
            }
          }
          nodeLocator.findResponsibleActor(data(neighbor)) ! PotentialNeighbors(neighbor, potentialNeighbors, g_node)
        }
        // if this is the last inital join, the timer is needed from now on
        timers.startSingleTimer(NNDescentTimerKey, NNDescentTimeout, timeoutAfter)
        nnDescent(nodeLocator, graph, reverseNeighbors, reusableDistanceStorage)

      case PotentialNeighbors(g_node, potentialNeighbors, senderIndex) =>
        // TODO refactor with less memory allocation
        val currentNeighbors: Seq[(Int, Double)] = graph(g_node)
        val currentMaxDist: Double = currentNeighbors(currentNeighbors.length - 1)._2
        val probableNeighbors: Set[(Int, Double)] = potentialNeighbors.filter(_._2 < currentMaxDist).toSet -- currentNeighbors - ((g_node, 0.0))
        if (probableNeighbors.isEmpty) {
          // nothing changes
          nnDescent(nodeLocator, graph, reverseNeighbors, reusableDistanceStorage)
        } else {
          if (timers.isTimerActive(NNDescentTimerKey)) {
            timers.cancel(NNDescentTimerKey)
          } else {
            // if the timer is inactive, it has already run out and the actor has mistakenly told its supervisor that it is done
            supervisor ! CorrectFinishedNNDescent
          }
          // update neighbors
          val mergedNeighbors = (currentNeighbors ++: probableNeighbors.toSeq).sortBy(_._2)
          val updatedNeighbors = mergedNeighbors.slice(0, k)
          // update the reverse neighbors of changed neighbors
          val newNeighbors: Set[Int] = probableNeighbors.intersect(updatedNeighbors.toSet).map(_._1)
          newNeighbors.foreach {index =>
            nodeLocator.findResponsibleActor(data(index)) ! AddReverseNeighbor(index, g_node)
          }
          val removedNeighbors = mergedNeighbors.slice(k, mergedNeighbors.length).intersect(currentNeighbors).map(_._1)
          removedNeighbors.foreach {index =>
            nodeLocator.findResponsibleActor(data(index)) ! RemoveReverseNeighbor(index, g_node)
          }
          // send out new potential neighbors (join the remaining old neighbors with the new ones)
          val keptNeighbors = updatedNeighbors.intersect(currentNeighbors.diff(Seq(senderIndex))).map(_._1) ++: reverseNeighbors(g_node)
          // first calculate distances to prevent double calculation
          val newNeighborsToPair = newNeighbors -- reverseNeighbors(g_node) // those who need to be joined with the kept neighbors
          val potentialPairs = for {
            n1 <- newNeighborsToPair
            n2 <- keptNeighbors
            dist = euclideanDist(data(n1), data(n2))
          } yield(n1, n2, dist)
          // introduce old neighbors to new neighbors
          // (the new neighbors do not have to be joined, as that will already have happened at the g_node the message originated from)
          newNeighborsToPair.foreach { index =>
            val thisNodesPotentialNeighbors = potentialPairs.collect{case (n1, n2, dist) if n1== index => (n2, dist)}.toSeq
            nodeLocator.findResponsibleActor(data(index)) ! PotentialNeighbors(index, thisNodesPotentialNeighbors, g_node)
          }
          // introduce old neighbors to new neighbors
          keptNeighbors.foreach {index =>
            val thisNodesPotentialNeighbors = potentialPairs.collect{case (n1, n2, dist) if n2 == index => (n1, dist)}.toSeq
            nodeLocator.findResponsibleActor(data(index)) ! PotentialNeighbors(index, thisNodesPotentialNeighbors, g_node)
          }
          // update graph
          val updatedGraph = graph + (g_node -> updatedNeighbors)
          ctx.log.info("Updating graph")
          // something changed so reset the NNDescent Timer
          timers.startSingleTimer(NNDescentTimerKey, NNDescentTimeout, timeoutAfter)
          nnDescent(nodeLocator, updatedGraph, reverseNeighbors, reusableDistanceStorage)
        }

      case AddReverseNeighbor(g_nodeIndex, neighborIndex) =>
        // all neighbors without duplicates and without the new neighbor being introduced
        val allNeighbors = graph(g_nodeIndex).map(_._1).toSet ++ reverseNeighbors(g_nodeIndex).toSet - neighborIndex
        // introduce the new neighbor to all other neighbors
        val potentialNeighbors = allNeighbors.map(index => (index, euclideanDist(data(index), data(neighborIndex)))).toSeq
        nodeLocator.findResponsibleActor(data(neighborIndex)) ! PotentialNeighbors(neighborIndex, potentialNeighbors, g_nodeIndex)
        // introduce all other neighbors to the new neighbor
        potentialNeighbors.foreach { case(index, distance) =>
          nodeLocator.findResponsibleActor(data(index)) ! PotentialNeighbors(index, Seq((neighborIndex, distance)), g_nodeIndex)
        }
        // update reverse neighbors
        val updatedReverseNeighbors = reverseNeighbors(g_nodeIndex) :+ neighborIndex
        nnDescent(nodeLocator, graph, reverseNeighbors + (g_nodeIndex -> updatedReverseNeighbors), reusableDistanceStorage)

      case RemoveReverseNeighbor(g_nodeIndex, neighborIndex) =>
        val updatedReverseNeighbors = reverseNeighbors(g_nodeIndex).diff(Seq(neighborIndex))
        nnDescent(nodeLocator, graph, reverseNeighbors + (g_nodeIndex -> updatedReverseNeighbors), reusableDistanceStorage)

      case NNDescentTimeout =>
        ctx.log.info("Looks like I'm done with NNDescent")
        supervisor ! FinishedNNDescent
        nnDescent(nodeLocator, graph, reverseNeighbors, reusableDistanceStorage)

      case StartSearchOnGraph(graphHolder) =>
        // all nodes are done with NNDescent, nothing will change with the graph anymore, so it is moved to SearchOnGraph actors
        val cleanedGraph: Map[Int, Seq[Int]] = graph.map{case (index, neighbors) => index -> neighbors.map(_._1)}
        val searchOnGraphEventAdapter: ActorRef[SearchOnGraph.SearchOnGraphEvent] =
          ctx.messageAdapter { event => WrappedSearchOnGraphEvent(event)}
        graphHolder ! Graph(cleanedGraph, searchOnGraphEventAdapter)
        initializeSearchOnGraph(searchOnGraphEventAdapter, cleanedGraph)
    }

  def initializeSearchOnGraph(searchOnGraphEventAdapter: ActorRef[SearchOnGraph.SearchOnGraphEvent],
                              graph: Map[Int, Seq[Int]]): Behavior[BuildGraphEvent] =
    Behaviors.receiveMessagePartial{
      case WrappedSearchOnGraphEvent(event) =>
        event match {
            // TODO add some kind of check/timer to make sure the graph arrived safely
          case GraphReceived(graphHolder) =>
            supervisor ! SOGDistributionInfo(LeafNode(graphHolder), ctx.self)
            Behaviors.empty
        }
    }

  def combineSOGDistributionTree(leftChild: ActorRef[BuildGraphEvent],
                                 rightChild: ActorRef[BuildGraphEvent],
                                 leftNode: Option[TreeNode[ActorRef[SearchOnGraphEvent]]],
                                 rightNode: Option[TreeNode[ActorRef[SearchOnGraphEvent]]],
                                 oldNode: SplitNode[ActorRef[BuildGraphEvent]]): Behavior[BuildGraphEvent] =
    Behaviors.receiveMessagePartial {
      case SOGDistributionInfo(treeNode, sender) =>
        sender match {
          case `leftChild` =>
            rightNode match {
              case None =>
                combineSOGDistributionTree(leftChild, rightChild, Option(treeNode), rightNode, oldNode)
              case Some(node) =>
                val combinedNode = SplitNode(treeNode, node, oldNode.dimension, oldNode.border)
                supervisor ! SOGDistributionInfo(combinedNode, ctx.self)
                Behaviors.empty
            }
          case `rightChild` =>
            leftNode match {
              case None =>
                combineSOGDistributionTree(leftChild, rightChild, leftNode, Option(treeNode), oldNode)
              case Some(node) =>
                val combinedNode = SplitNode(node, treeNode, oldNode.dimension, oldNode.border)
                supervisor ! SOGDistributionInfo(combinedNode, ctx.self)
                Behaviors.empty
            }
        }
  }
}



