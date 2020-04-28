package com.github.julkw.dnsg.actors.nndescent

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}

import scala.concurrent.duration._
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import com.github.julkw.dnsg.actors.ClusterCoordinator.{CoordinationEvent, CorrectFinishedNNDescent, FinishedApproximateGraph, FinishedNNDescent, KnngDistributionInfo}
import com.github.julkw.dnsg.actors.NodeCoordinator.{LocalKnngWorker, NodeCoordinationEvent}
import com.github.julkw.dnsg.actors.SearchOnGraph
import com.github.julkw.dnsg.actors.SearchOnGraph.{Graph, GraphReceived, SearchOnGraphEvent}
import com.github.julkw.dnsg.util.{IndexTree, LeafNode, LocalData, NodeLocator, SplitNode, TreeBuilder, TreeNode}

import scala.language.postfixOps


object KnngWorker {

  sealed trait BuildGraphEvent

  // setup
  final case class ResponsibleFor(responsibility: Seq[Int]) extends BuildGraphEvent

  final case class BuildApproximateGraph(nodeLocator: NodeLocator[BuildGraphEvent], workers: Set[ActorRef[BuildGraphEvent]]) extends BuildGraphEvent

  // build approximate graph
  final case class FindCandidates(index: Int) extends BuildGraphEvent

  final case class GetCandidates(query: Seq[Float], index: Int, replyTo: ActorRef[BuildGraphEvent]) extends BuildGraphEvent

  final case class Candidates(candidates: Seq[(Int, Double)], index: Int) extends BuildGraphEvent

  // improve approximate graph
  final case object StartNNDescent extends BuildGraphEvent

  final case class CompleteLocalJoin(g_node: Int) extends BuildGraphEvent

  final case class PotentialNeighbor(g_node: Int, potentialNeighbor: (Int, Double), senderIndex: Int) extends BuildGraphEvent

  final case class RemoveReverseNeighbor(g_nodeIndex: Int, neighborIndex: Int) extends BuildGraphEvent

  final case class AddReverseNeighbor(g_nodeIndex: Int, neighborIndex: Int) extends BuildGraphEvent

  final case class ListingResponse(listing: Receptionist.Listing) extends BuildGraphEvent

  final case object NNDescentTimeout extends  BuildGraphEvent

  final case object NNDescentTimerKey

  // give final graph to SearchOnGraph Actor
  final case class MoveGraph(graphHolder: ActorRef[SearchOnGraphEvent]) extends BuildGraphEvent

  final case class WrappedSearchOnGraphEvent(event: SearchOnGraph.SearchOnGraphEvent) extends BuildGraphEvent

  final case class SOGDistributionInfo(treeNode: TreeNode[ActorRef[SearchOnGraphEvent]], sender: ActorRef[BuildGraphEvent]) extends BuildGraphEvent


  val knngServiceKey: ServiceKey[BuildGraphEvent] = ServiceKey[BuildGraphEvent]("knngWorker")
  // TODO do over input
  val timeoutAfter = 3.second

  def apply(data: LocalData[Float],
            maxResponsibility: Int,
            k: Int,
            sampleRate: Double,
            clusterCoordinator: ActorRef[CoordinationEvent],
            localCoordinator: ActorRef[NodeCoordinationEvent]): Behavior[BuildGraphEvent] = Behaviors.setup { ctx =>
    //ctx.log.info("Started KnngWorker")
    localCoordinator ! LocalKnngWorker(ctx.self)
    Behaviors.withTimers(timers =>
      new KnngWorker(data, maxResponsibility, k, sampleRate, clusterCoordinator, localCoordinator, timers, ctx).buildDistributionTree()
    )
  }
}

class KnngWorker(data: LocalData[Float],
                 maxResponsibility: Int,
                 k: Int,
                 sampleRate: Double,
                 clusterCoordinator: ActorRef[CoordinationEvent],
                 localCoordinator: ActorRef[NodeCoordinationEvent],
                 timers: TimerScheduler[KnngWorker.BuildGraphEvent],
                 ctx: ActorContext[KnngWorker.BuildGraphEvent]) extends Joiner(sampleRate, data) {
  import KnngWorker._

  def buildDistributionTree(): Behavior[BuildGraphEvent] = Behaviors.receiveMessagePartial {
    case ResponsibleFor(responsibility) =>
      val treeBuilder: TreeBuilder = TreeBuilder(data, k)
      if(responsibility.length > maxResponsibility) {
        // further split the data
        val splitNode: SplitNode[Seq[Int]] = treeBuilder.oneLevelSplit(responsibility)
        val right = ctx.spawn(KnngWorker(data, maxResponsibility, k, sampleRate, clusterCoordinator, localCoordinator), name="KnngWorker")
        ctx.self ! ResponsibleFor(splitNode.left.data)
        right ! ResponsibleFor(splitNode.right.data)
        buildDistributionTree()
      } else {
        // this is a leaf node for data distribution
        ctx.system.receptionist ! Receptionist.Register(knngServiceKey, ctx.self)
        val locationNode: LeafNode[ActorRef[BuildGraphEvent]] = LeafNode(ctx.self)
        clusterCoordinator ! KnngDistributionInfo(responsibility, ctx.self)

        // Build local tree while waiting on DistributionTree message
        val treeBuilder: TreeBuilder = TreeBuilder(data, k)
        val kdTree: IndexTree = treeBuilder.construct(responsibility)
        ctx.log.info("Finished building kdTree")
        waitForDistributionInfo(kdTree, responsibility)
      }
  }

  def waitForDistributionInfo(kdTree: IndexTree,
                              responsibility: Seq[Int]): Behavior[BuildGraphEvent] =
    Behaviors.receiveMessagePartial {
      case BuildApproximateGraph(nodeLocator, workers) =>
        //ctx.log.info("Received Distribution Tree. Start building approximate graph")
        ctx.self ! FindCandidates(0)
        val candidates: Array[Seq[(Int, Double)]] = Array.fill(responsibility.length){Seq.empty}
        val awaitingAnswer: Array[Int] = Array.fill(responsibility.length){0}
        val graph: Map[Int, Seq[(Int, Double)]] = Map.empty
        buildApproximateGraph(kdTree, responsibility, candidates, awaitingAnswer, nodeLocator, workers, graph)
      case GetCandidates(query, index, sender) =>
        val getCandidateKey = "GetCandidates"
        timers.startSingleTimer(getCandidateKey, GetCandidates(query, index, sender), 1.second)
        ctx.log.info("Got a request for candidates before the distribution info. Forwarded to self with delay.")
        waitForDistributionInfo(kdTree, responsibility)
    }

  def buildApproximateGraph(kdTree: IndexTree,
                            responsibility: Seq[Int],
                            candidates: Array[Seq[(Int, Double)]],
                            awaitingAnswer: Array[Int],
                            nodeLocator: NodeLocator[BuildGraphEvent],
                            knngWorkers: Set[ActorRef[BuildGraphEvent]],
                            graph:Map[Int, Seq[(Int, Double)]]): Behavior[BuildGraphEvent] =
    Behaviors.receiveMessagePartial {
      case FindCandidates(responsibilityIndex) =>
        // Also look for the closest neighbors of the other g_nodes
        if (responsibilityIndex < responsibility.length - 1) {
          ctx.self ! FindCandidates(responsibilityIndex + 1)
        }
        val index = responsibility(responsibilityIndex)
        val query: Seq[Float] = data.at(index).get
        // find candidates for current node by asking all workers for candidates to ensure a connected graph across all nodes
        knngWorkers.foreach { case worker if worker != ctx.self =>
          worker ! GetCandidates(query, responsibilityIndex, ctx.self)
          awaitingAnswer(responsibilityIndex) += 1
        }

        // Find candidates on own tree using Efanna method
        // TODO test how much time this costs and if it improves the graph significantly
        var currentDataNode: TreeNode[Seq[Int]] = kdTree.root
        var localCandidates: Seq[(Int, Double)] = Seq.empty
        while (currentDataNode.inverseQueryChild(query) != currentDataNode) {
          val newCandidates = currentDataNode.inverseQueryChild(query).queryLeaf(query).data.map(index =>
            (index, euclideanDist(data.at(index).get, query))
          )
          localCandidates = localCandidates ++ newCandidates
          currentDataNode = currentDataNode.queryChild(query)
        }
        localCandidates = localCandidates ++ currentDataNode.data.map(index =>
          (index, euclideanDist(data.at(index).get, query))
        )
        ctx.self ! Candidates(localCandidates, responsibilityIndex)
        awaitingAnswer(responsibilityIndex) += 1
        buildApproximateGraph(kdTree, responsibility, candidates, awaitingAnswer, nodeLocator, knngWorkers, graph)

      case GetCandidates(query, index, sender) =>
        val localCandidates = kdTree.root.queryLeaf(query).data.map(index =>
          (index, euclideanDist(data.at(index).get, query))
        )
        sender ! Candidates(localCandidates, index)
        buildApproximateGraph(kdTree, responsibility, candidates, awaitingAnswer, nodeLocator, knngWorkers, graph)

      case Candidates(newCandidates, responsibilityIndex) =>
        val graphIndex: Int = responsibility(responsibilityIndex)
        val oldCandidates = candidates(responsibilityIndex)
        val mergedCandidates = (oldCandidates ++ newCandidates).sortBy(_._2)
        val skipSelf = if (mergedCandidates(0)._1 == graphIndex) 1 else 0
        candidates(responsibilityIndex) = mergedCandidates.slice(skipSelf, k + skipSelf)
        awaitingAnswer(responsibilityIndex) -= 1
        if (awaitingAnswer(responsibilityIndex) == 0) {
          val updatedGraph = graph + (graphIndex -> candidates(responsibilityIndex))
          if (updatedGraph.size == responsibility.length) {
            clusterCoordinator ! FinishedApproximateGraph
          }
          buildApproximateGraph(kdTree, responsibility, candidates, awaitingAnswer, nodeLocator, knngWorkers, updatedGraph)
        } else {
          buildApproximateGraph(kdTree, responsibility, candidates, awaitingAnswer, nodeLocator, knngWorkers, graph)
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
            clusterCoordinator ! CorrectFinishedNNDescent
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
        clusterCoordinator ! FinishedNNDescent
        nnDescent(nodeLocator, graph, reverseNeighbors)

      case MoveGraph(graphHolder) =>
        ctx.log.info("Average distance in graph after nndescent: {}", averageGraphDist(graph, k))
        // all nodes are done with NNDescent, nothing will change with the graph anymore, so it is moved to SearchOnGraph actors
        val cleanedGraph: Map[Int, Seq[Int]] = graph.map{case (index, neighbors) => index -> neighbors.map(_._1)}
        val searchOnGraphEventAdapter: ActorRef[SearchOnGraph.SearchOnGraphEvent] =
          ctx.messageAdapter { event => WrappedSearchOnGraphEvent(event)}
        graphHolder ! Graph(cleanedGraph, searchOnGraphEventAdapter)
        // TODO add some kind of check to ensure the arrival of the graph?
        Behaviors.empty
    }
}



