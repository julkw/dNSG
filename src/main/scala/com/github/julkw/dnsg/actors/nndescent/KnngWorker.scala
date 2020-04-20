package com.github.julkw.dnsg.actors.nndescent

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}

import scala.concurrent.duration._
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import com.github.julkw.dnsg.actors.SearchOnGraph
import com.github.julkw.dnsg.actors.SearchOnGraph.{Graph, GraphReceived, SearchOnGraphEvent}
import com.github.julkw.dnsg.util.{IndexTree, LeafNode, NodeLocator, SplitNode, TreeBuilder, TreeNode}

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

  final case class PotentialNeighbor(g_node: Int, potentialNeighbor: (Int, Double), senderIndex: Int) extends BuildGraphEvent

  final case class RemoveReverseNeighbor(g_nodeIndex: Int, neighborIndex: Int) extends BuildGraphEvent

  final case class AddReverseNeighbor(g_nodeIndex: Int, neighborIndex: Int) extends BuildGraphEvent

  final case class ListingResponse(listing: Receptionist.Listing) extends BuildGraphEvent

  final case object FinishedNNDescent extends BuildGraphEvent

  final case object CorrectFinishedNNDescent extends BuildGraphEvent

  final case object NNDescentTimeout extends  BuildGraphEvent

  final case object NNDescentTimerKey

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
            sampleRate: Double,
            supervisor: ActorRef[BuildGraphEvent]): Behavior[BuildGraphEvent] = Behaviors.setup { ctx =>
    //ctx.log.info("Started KnngWorker")
    Behaviors.withTimers(timers => new KnngWorker(data, maxResponsibility, k, sampleRate, supervisor, timers, ctx).buildDistributionTree())
  }
}

class KnngWorker(data: Seq[Seq[Float]],
                 maxResponsibility: Int,
                 k: Int,
                 sampleRate: Double,
                 supervisor: ActorRef[KnngWorker.BuildGraphEvent],
                 timers: TimerScheduler[KnngWorker.BuildGraphEvent],
                 ctx: ActorContext[KnngWorker.BuildGraphEvent]) extends Joiner(k, sampleRate, data, supervisor, timers) {
  import KnngWorker._

  def buildDistributionTree(): Behavior[BuildGraphEvent] = Behaviors.receiveMessagePartial {
    case ResponsibleFor(responsibility) =>
      val treeBuilder: TreeBuilder = TreeBuilder(data, k)
      if(responsibility.length > maxResponsibility) {
        // further split the data
        val splitNode: SplitNode[Seq[Int]] = treeBuilder.oneLevelSplit(responsibility)
        val left = ctx.spawn(KnngWorker(data, maxResponsibility, k, sampleRate, ctx.self), name="KnngWorkerLeft")
        val right = ctx.spawn(KnngWorker(data, maxResponsibility, k, sampleRate, ctx.self), name="KnngWorkerRight")
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
        //ctx.log.info("Received Distribution Tree. Start building approximate graph")
        ctx.self ! FindCandidates(0)
        val candidates: Map[Int, Seq[Int]] = Map.empty
        val awaitingAnswer: Array[Int] = Array.fill(responsibility.length){0}
        val graph: Map[Int, Seq[(Int, Double)]] = Map.empty
        buildApproximateGraph(kdTree, responsibility, candidates, awaitingAnswer, nodeLocator, graph)
      case GetCandidates(query, index, sender) =>
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
            // ctx.log.info("Should be able to switch to nnDescent now")
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

      case PotentialNeighbor(g_node, potentialNeighbor, senderIndex) =>
        ctx.self ! PotentialNeighbor(g_node, potentialNeighbor, senderIndex)
        startNNDescent(nodeLocator, graph)
    }

  def startNNDescent(nodeLocator: NodeLocator[BuildGraphEvent],
                     graph: Map[Int, Seq[(Int, Double)]]): Behavior[BuildGraphEvent] = {
    //ctx.log.info("Starting nnDescent")
    // for debugging
    ctx.log.info("Average distance in graph before nndescent: {}", averageGraphDist(graph, k))
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
    val reverseNeighbors: Map[Int, Set[Int]] = graph.map{case (index, _) => index -> Set.empty}
    nnDescent(nodeLocator, graph, reverseNeighbors)
  }

  def nnDescent(nodeLocator: NodeLocator[BuildGraphEvent],
                graph: Map[Int, Seq[(Int, Double)]],
                reverseNeighbors: Map[Int, Set[Int]]): Behavior[BuildGraphEvent] =
    Behaviors.receiveMessage {
      case StartNNDescent =>
        // already done, so do nothing
        nnDescent(nodeLocator, graph, reverseNeighbors)

      case CompleteLocalJoin(g_node) =>
        //ctx.log.info("local join")
        // prevent timeouts in the initial phase of graph nnDescent
        timers.cancel(NNDescentTimerKey)
        val neighbors = graph(g_node)
        joinNeighbors(neighbors, nodeLocator, g_node)
        // if this is the last inital join, the timer is needed from now on
        timers.startSingleTimer(NNDescentTimerKey, NNDescentTimeout, timeoutAfter)
        nnDescent(nodeLocator, graph, reverseNeighbors)

      case PotentialNeighbor(g_node, potentialNeighbor, senderIndex) =>
        val currentNeighbors = graph(g_node)
        val currentReverseNeighbors = reverseNeighbors(g_node)
        val currentMaxDist = currentNeighbors(currentNeighbors.length - 1)._2
        val isNew: Boolean = !(currentNeighbors.exists(neighbor => neighbor._1 == potentialNeighbor._1)
          || currentReverseNeighbors.contains(potentialNeighbor._1))
        if (currentMaxDist > potentialNeighbor._2 && potentialNeighbor._1 != g_node && isNew) {
          if (timers.isTimerActive(NNDescentTimerKey)) {
            timers.cancel(NNDescentTimerKey)
          } else {
            // if the timer is inactive, it has already run out and the actor has mistakenly told its supervisor that it is done
            supervisor ! CorrectFinishedNNDescent
          }
          joinNewNeighbor(currentNeighbors.slice(0, k-1), currentReverseNeighbors, g_node, potentialNeighbor._1, senderIndex, nodeLocator)
          timers.startSingleTimer(NNDescentTimerKey, NNDescentTimeout, timeoutAfter)
          val updatedNeighbors = (currentNeighbors :+ potentialNeighbor).sortBy(_._2).slice(0, k)
          nnDescent(nodeLocator, graph + (g_node -> updatedNeighbors), reverseNeighbors)
        } else {
          nnDescent(nodeLocator, graph, reverseNeighbors)
        }

      case AddReverseNeighbor(g_nodeIndex, neighborIndex) =>
        //ctx.log.info("add reverse neighbor")
        // if new and not already a neighbor, introduce to all current neighbors
        if (!graph(g_nodeIndex).exists(neighbor => neighbor._1 == neighborIndex)) {
          joinNewNeighbor(graph(g_nodeIndex), reverseNeighbors(g_nodeIndex), g_nodeIndex, neighborIndex, neighborIndex, nodeLocator)
        }
        // update reverse neighbors
        val updatedReverseNeighbors = reverseNeighbors(g_nodeIndex) + neighborIndex
        nnDescent(nodeLocator, graph, reverseNeighbors + (g_nodeIndex -> updatedReverseNeighbors))

      case RemoveReverseNeighbor(g_nodeIndex, neighborIndex) =>
        //ctx.log.info("remove reverse neighbor")
        val updatedReverseNeighbors = reverseNeighbors(g_nodeIndex) - neighborIndex
        nnDescent(nodeLocator, graph, reverseNeighbors + (g_nodeIndex -> updatedReverseNeighbors))

      case NNDescentTimeout =>
        supervisor ! FinishedNNDescent
        nnDescent(nodeLocator, graph, reverseNeighbors)

      case StartSearchOnGraph(graphHolder) =>
        ctx.log.info("Average distance in graph after nndescent: {}", averageGraphDist(graph, k))
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
                Behaviors.stopped
            }
          case `rightChild` =>
            leftNode match {
              case None =>
                combineSOGDistributionTree(leftChild, rightChild, leftNode, Option(treeNode), oldNode)
              case Some(node) =>
                val combinedNode = SplitNode(node, treeNode, oldNode.dimension, oldNode.border)
                supervisor ! SOGDistributionInfo(combinedNode, ctx.self)
                Behaviors.stopped
            }
        }
  }
}



