package com.github.julkw.dnsg.actors.nndescent

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.{ConfirmFinishedNNDescent, CoordinationEvent, CorrectFinishedNNDescent, FinishedApproximateGraph, FinishedNNDescent}
import com.github.julkw.dnsg.actors.Coordinators.NodeCoordinator.{LocalKnngWorker, NodeCoordinationEvent}
import com.github.julkw.dnsg.actors.NodeLocatorHolder.{LocalKnngDistributionInfo, NodeLocationEvent}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.{GraphAndData, SearchOnGraphEvent}
import com.github.julkw.dnsg.actors.nndescent.NNDInfo.{AddReverseNeighbor, JoinNodes, NNDescentEvent, PotentialNeighbor, PotentialNeighborLocation, RemoveReverseNeighbor, SendLocation}
import com.github.julkw.dnsg.util.Data.{CacheData, LocalData}
import com.github.julkw.dnsg.util.KdTree.{IndexTree, KdTree, SplitNode, TreeBuilder, TreeNode}
import com.github.julkw.dnsg.util.{NodeLocator, Settings, WaitingOnLocation, dNSGSerializable}

import scala.language.postfixOps


object KnngWorker {

  sealed trait BuildGraphEvent extends dNSGSerializable

  // setup
  final case class ResponsibleFor(responsibility: Seq[Int], treeDepth: Int, workers: Int) extends BuildGraphEvent

  final case class BuildApproximateGraph(nodeLocator: NodeLocator[BuildGraphEvent]) extends BuildGraphEvent

  // build approximate graph
  final case class GetCandidates(nodes: Seq[(Int, Seq[Float])], batchNumber: Int, sender: ActorRef[BuildGraphEvent]) extends BuildGraphEvent

  final case class Candidates(candidates: Seq[(Int, Seq[(Int, Double)])], batchNumber: Int, sender: ActorRef[BuildGraphEvent]) extends BuildGraphEvent

  // improve approximate graph
  // start NNDescent
  final case object StartNNDescent extends BuildGraphEvent

  final case class CompleteLocalJoin(g_node: Int) extends BuildGraphEvent

  final case class SendReverseNeighbors(g_node: Int) extends BuildGraphEvent

  // during NNDescent
  final case class GetNNDescentInfo(sender: ActorRef[BuildGraphEvent]) extends BuildGraphEvent

  final case class NNDescentInfo(info: collection.Seq[NNDescentEvent], sender: ActorRef[BuildGraphEvent]) extends BuildGraphEvent

  final case class NoNewInfo(sender: ActorRef[BuildGraphEvent]) extends BuildGraphEvent

  final case object GetNNDescentFinishedConfirmation extends BuildGraphEvent

  // after NNDescent
  final case class MoveGraph(graphHolder: ActorRef[SearchOnGraphEvent]) extends BuildGraphEvent

  final case object AllKnngWorkersDone extends BuildGraphEvent

  def apply(data: LocalData[Float],
            clusterCoordinator: ActorRef[CoordinationEvent],
            nodeCoordinator: ActorRef[NodeCoordinationEvent],
            localNodeLocatorHolder: ActorRef[NodeLocationEvent]): Behavior[BuildGraphEvent] = Behaviors.setup { ctx =>
    nodeCoordinator ! LocalKnngWorker(ctx.self)
    val settings = Settings(ctx.system.settings.config)
    Behaviors.withTimers(timers =>
      new KnngWorker(CacheData(settings.cacheSize, data),
        new WaitingOnLocation, settings, clusterCoordinator, nodeCoordinator, localNodeLocatorHolder, timers, ctx).buildDistributionTree()
    )
  }
}

class KnngWorker(data: CacheData[Float],
                 waitingOnLocation: WaitingOnLocation,
                 settings: Settings,
                 clusterCoordinator: ActorRef[CoordinationEvent],
                 nodeCoordinator: ActorRef[NodeCoordinationEvent],
                 localNodeLocatorHolder: ActorRef[NodeLocationEvent],
                 timers: TimerScheduler[KnngWorker.BuildGraphEvent],
                 ctx: ActorContext[KnngWorker.BuildGraphEvent]) extends Joiner(settings.sampleRate, data) {
  import KnngWorker._

  def buildDistributionTree(): Behavior[BuildGraphEvent] = Behaviors.receiveMessagePartial {
    case ResponsibleFor(responsibility, treeDepth, workers) =>
      val treeBuilder: TreeBuilder = TreeBuilder(data.data, settings.k)
      if(workers > 1 && responsibility.length > 1) {
        val leftWorkers: Int = workers / 2
        val rightWorkers: Int = workers - leftWorkers
        val splitPoint: Float = leftWorkers.toFloat / workers.toFloat
        // further split the data
        val splitNode: SplitNode[Seq[Int]] = treeBuilder.oneLevelSplit(responsibility, splitPoint)
        ctx.log.info("Splitting {} nodes", responsibility.length)
        assert(splitNode.left.data.nonEmpty && splitNode.right.data.nonEmpty)
        val right = ctx.spawn(KnngWorker(data.data, clusterCoordinator, nodeCoordinator, localNodeLocatorHolder), "KnngWorker" + treeDepth.toString)
        ctx.self ! ResponsibleFor(splitNode.left.data, treeDepth + 1, leftWorkers)
        right ! ResponsibleFor(splitNode.right.data, treeDepth + 1, rightWorkers)
        buildDistributionTree()
      } else {
        ctx.log.info("Build local kdTree with {} graph_nodes", responsibility.length)
        // this is a leaf node for data distribution
        localNodeLocatorHolder ! LocalKnngDistributionInfo(responsibility, ctx.self)
        // Build local tree while waiting on DistributionTree message
        val treeBuilder: TreeBuilder = TreeBuilder(data.data, settings.k)
        val kdTree: IndexTree = treeBuilder.construct(responsibility)
        waitForDistributionInfo(kdTree, responsibility)
      }
  }

  def waitForDistributionInfo(kdTree: IndexTree,
                              responsibility: Seq[Int]): Behavior[BuildGraphEvent] =
    Behaviors.receiveMessagePartial {
      case BuildApproximateGraph(nodeLocator) =>
        // define for how many nodes the candidates can be asked for at once (so that the answering message also stays within messageSize)
        val maxMessageSize = settings.maxMessageSize / math.max(data.data.dimension + 1, settings.k * 2 + 1)
        val candidateBatches = responsibility.grouped(maxMessageSize).toSeq
        val firstBatch = candidateBatches.head.map(index => (index, data.get(index)))
        // TODO in this version of buildApproximateGraph the quality of the graph depends solely on the number of workers in the cluster
        nodeLocator.allActors.foreach(worker => worker ! GetCandidates(firstBatch, 0, ctx.self))

        val candidates: Map[Int, Seq[(Int, Double)]] = responsibility.map(index => index -> Seq.empty).toMap
        val awaitingAnswer: Array[Int] = Array.fill(candidateBatches.length){nodeLocator.allActors.size}
        val graph: Map[Int, Seq[(Int, Double)]] = Map.empty
        buildApproximateGraph(kdTree, candidateBatches, candidates, awaitingAnswer, nodeLocator, graph, maxMessageSize)

      case GetCandidates(nodes, alreadyAskedFor, sender) =>
        val candidates = nodes.map { case (responsibilityIndex, location) =>
          (responsibilityIndex, findLocalCandidatesFast(location, kdTree))
        }
        sender ! Candidates(candidates, alreadyAskedFor, ctx.self)
        waitForDistributionInfo(kdTree, responsibility)
    }

  def buildApproximateGraph(kdTree: IndexTree,
                            candidateBatches: Seq[Seq[Int]],
                            currentCandidates: Map[Int, Seq[(Int, Double)]],
                            awaitingAnswer: Array[Int],
                            nodeLocator: NodeLocator[BuildGraphEvent],
                            graph:Map[Int, Seq[(Int, Double)]],
                            maxMessageSize: Int): Behavior[BuildGraphEvent] =
    Behaviors.receiveMessagePartial {
      case GetCandidates(nodes, batchNumber, sender) =>
        val candidates = nodes.map { case (responsibilityIndex, location) =>
          (responsibilityIndex, findLocalCandidatesFast(location, kdTree))
        }
        sender ! Candidates(candidates, batchNumber, ctx.self)
        buildApproximateGraph(kdTree, candidateBatches, currentCandidates, awaitingAnswer, nodeLocator, graph, maxMessageSize)

      case Candidates(candidates, batchNumber, sender) =>
        // update candidates
        val newCandidates = candidates.map { case (graphIndex, newCandidates) =>
          val oldCandidates = currentCandidates(graphIndex)
          val mergedCandidates = (oldCandidates ++ newCandidates).sortBy(_._2)
          val skipSelf = if (mergedCandidates.head._1 == graphIndex) 1 else 0
          (graphIndex, mergedCandidates.slice(skipSelf, settings.k + skipSelf))
        }
        val updatedCandidates = currentCandidates ++ newCandidates
        // update graph
        awaitingAnswer(batchNumber) -= 1
        val updatedGraph =
          if (awaitingAnswer(batchNumber) == 0) {
            graph ++ candidateBatches(batchNumber).map(index => (index, updatedCandidates(index)))
          } else { graph }

        // ask for candidates for more nodes
        val nextBatchNumber = batchNumber + 1
        if (nextBatchNumber < candidateBatches.length) {
          val nodes = candidateBatches(nextBatchNumber).map(index => (index, data.get(index)))
          sender ! GetCandidates(nodes, nextBatchNumber, ctx.self)
        } else if (awaitingAnswer.forall(_ == 0)) {
          clusterCoordinator ! FinishedApproximateGraph
        }
        buildApproximateGraph(kdTree, candidateBatches, updatedCandidates, awaitingAnswer, nodeLocator, updatedGraph, maxMessageSize)

      case StartNNDescent =>
        startNNDescent(nodeLocator, graph)

      case GetNNDescentInfo(sender) =>
        ctx.self ! GetNNDescentInfo(sender)
        startNNDescent(nodeLocator, graph)
    }

  def startNNDescent(nodeLocator: NodeLocator[BuildGraphEvent],
                     graph: Map[Int, Seq[(Int, Double)]]): Behavior[BuildGraphEvent] = {
    // for debugging
    //ctx.log.info("Average distance in graph before nndescent: {}", averageGraphDist(graph, settings.k))
    graph.keys.foreach{ g_node =>
      // do the initial local joins through messages to self to prevent Heartbeat problems
      ctx.self ! SendReverseNeighbors(g_node)
      ctx.self ! CompleteLocalJoin(g_node)
    }
    nodeLocator.allActors.foreach(worker => worker ! GetNNDescentInfo(ctx.self))
    val toSend = nodeLocator.allActors.map(worker => worker -> new NNDInfo).toMap
    // setup timer used to determine when the graph is done
    // start nnDescent
    val reverseNeighbors: Map[Int, Set[Int]] = graph.map{case (index, _) => index -> Set.empty}
    nnDescent(nodeLocator, graph, reverseNeighbors, toSend, Set.empty, saidImDone = false)
  }

  def nnDescent(nodeLocator: NodeLocator[BuildGraphEvent],
                graph: Map[Int, Seq[(Int, Double)]],
                reverseNeighbors: Map[Int, Set[Int]],
                toSend: Map[ActorRef[BuildGraphEvent], NNDInfo],
                mightBeDone: Set[ActorRef[BuildGraphEvent]],
                saidImDone: Boolean): Behavior[BuildGraphEvent] =
    Behaviors.receiveMessagePartial {
      case StartNNDescent =>
        // already done, so do nothing
        nnDescent(nodeLocator, graph, reverseNeighbors, toSend, mightBeDone, saidImDone)

      case SendReverseNeighbors(g_node) =>
        val neighbors = graph(g_node)
        neighbors.foreach{case (neighborIndex, _) =>
          val responsibleNeighbor = nodeLocator.findResponsibleActor(neighborIndex)
          toSend(responsibleNeighbor).addMessage(AddReverseNeighbor(neighborIndex, g_node))
        }
        nnDescent(nodeLocator, graph, reverseNeighbors, toSend, mightBeDone, saidImDone)

      case CompleteLocalJoin(g_node) =>
        // prevent timeouts in the initial phase of graph nnDescent
        val neighbors = graph(g_node)
        joinNeighbors(neighbors, toSend, nodeLocator)
        nnDescent(nodeLocator, graph, reverseNeighbors, toSend, mightBeDone, saidImDone)

      case GetNNDescentInfo(sender) =>
        val messagesToSend = toSend(sender).sendMessage(settings.maxMessageSize)
        if (messagesToSend.nonEmpty) {
          sender ! NNDescentInfo(messagesToSend, ctx.self)
          nnDescent(nodeLocator, graph, reverseNeighbors, toSend, mightBeDone, saidImDone)
        } else {
          sender ! NoNewInfo(ctx.self)
          toSend(sender).sendImmediately = true
          val probablyDone = checkIfDone(mightBeDone, nodeLocator, toSend, saidImDone)
          nnDescent(nodeLocator, graph, reverseNeighbors, toSend, mightBeDone, probablyDone)
        }

      case NoNewInfo(sender) =>
        val mightBeDoneWorkers = mightBeDone + sender
        val probablyDone = checkIfDone(mightBeDoneWorkers, nodeLocator, toSend, saidImDone)
        nnDescent(nodeLocator, graph, reverseNeighbors, toSend, mightBeDoneWorkers, probablyDone)

      case NNDescentInfo(info, sender) =>
        if (saidImDone) {
          clusterCoordinator ! CorrectFinishedNNDescent(ctx.self)
        }
        var updatedGraph = graph
        var updatedReverseNeighbors = reverseNeighbors
        info.foreach {
          case JoinNodes(g_nodes, potentialNeighborIndex) =>
            if (data.isLocal(potentialNeighborIndex)) {
              val neighborData = data.get(potentialNeighborIndex)
              g_nodes.foreach(g_node =>
                joinLocals(g_node, data.get(g_node), potentialNeighborIndex, neighborData, toSend, nodeLocator)
              )
            }
            else {
              sendForLocation(nodeLocator, potentialNeighborIndex, g_nodes, toSend)
            }

          case SendLocation(g_node) =>
            toSend(sender).addMessage(PotentialNeighborLocation(g_node, data.get(g_node)))

          case PotentialNeighborLocation(potentialNeighborIndex, potentialNeighbor) =>
            data.add(potentialNeighborIndex, potentialNeighbor)
            waitingOnLocation.received(potentialNeighborIndex).foreach(g_node =>
              joinLocals(g_node, data.get(g_node), potentialNeighborIndex, potentialNeighbor, toSend, nodeLocator)
            )

          case PotentialNeighbor(g_node, potentialNeighbor) =>
            updatedGraph = handlePotentialNeighbor(g_node, potentialNeighbor, updatedGraph, updatedReverseNeighbors, toSend, nodeLocator)

          case AddReverseNeighbor(g_nodeIndex, neighborIndex) =>
            // if new and not already a neighbor, introduce to all current neighbors
            if (!updatedGraph(g_nodeIndex).exists(neighbor => neighbor._1 == neighborIndex)) {
              joinNewNeighbor(updatedGraph(g_nodeIndex), updatedReverseNeighbors(g_nodeIndex), neighborIndex, toSend, nodeLocator)
            }
            // update reverse neighbors
            updatedReverseNeighbors += (g_nodeIndex -> (updatedReverseNeighbors(g_nodeIndex) + neighborIndex))

          case RemoveReverseNeighbor(g_nodeIndex, neighborIndex) =>
            updatedReverseNeighbors += (g_nodeIndex -> (updatedReverseNeighbors(g_nodeIndex) - neighborIndex))
        }
        sender ! GetNNDescentInfo(ctx.self)
        sendChangesImmediately(toSend)
        nnDescent(nodeLocator, updatedGraph, updatedReverseNeighbors, toSend, mightBeDone - sender, saidImDone = false)

      case GetNNDescentFinishedConfirmation =>
        if (saidImDone) {
          clusterCoordinator ! ConfirmFinishedNNDescent(ctx.self)
        } // else my correction did not make it there in time but is still on the way so I do not need to send it again
        nnDescent(nodeLocator, graph, reverseNeighbors, toSend, mightBeDone, saidImDone)

      case MoveGraph(graphHolder) =>
        // move graph to SearchOnGraphActor
        //ctx.log.info("Average distance in graph after nndescent: {}", averageGraphDist(graph, settings.k))
        val cleanedGraph: Map[Int, Seq[Int]] = graph.map{case (index, neighbors) => index -> neighbors.map(_._1)}
        graphHolder ! GraphAndData(cleanedGraph, data, ctx.self)
        waitForShutdown()

      case AllKnngWorkersDone =>
        ctx.log.info("Got the message to shutdown before moving my graph of size: {}", graph.size)
        Behaviors.stopped
    }

  def waitForShutdown(): Behavior[BuildGraphEvent] = Behaviors.receiveMessagePartial {
    case AllKnngWorkersDone =>
      ctx.log.info("Shutting down knngWorker as it is not needed anymore")
      Behaviors.stopped
  }

  def findLocalCandidates(query: Seq[Float], kdTree: KdTree[Seq[Int]]): Seq[(Int, Double)] = {
    // Find candidates on own tree using Efanna method
    var currentDataNode: TreeNode[Seq[Int]] = kdTree.root
    var localCandidates: Seq[(Int, Double)] = Seq.empty
    while (currentDataNode.inverseQueryChild(query) != currentDataNode) {
      val newCandidates = currentDataNode.inverseQueryChild(query).queryLeaf(query).data.map(index =>
        (index, euclideanDist(data.get(index), query))
      )
      localCandidates = (localCandidates ++ newCandidates).sortBy(_._2).slice(0, settings.k)
      currentDataNode = currentDataNode.queryChild(query)
    }
    localCandidates = localCandidates ++ currentDataNode.data.map(index =>
      (index, euclideanDist(data.get(index), query))
    ).sortBy(_._2).slice(0, settings.k)
    localCandidates
  }

  def findLocalCandidatesFast(query: Seq[Float], kdTree: KdTree[Seq[Int]]): Seq[(Int, Double)] = {
    // this is less precise but faster
    kdTree.root.queryLeaf(query).data.map(index =>
      (index, euclideanDist(data.get(index), query))
    )
  }

  def handlePotentialNeighbor(g_node: Int,
                              potentialNeighbor: (Int, Double),
                              graph: Map[Int, Seq[(Int, Double)]],
                              reverseNeighbors: Map[Int, Set[Int]],
                              toSend: Map[ActorRef[BuildGraphEvent], NNDInfo],
                              nodeLocator: NodeLocator[BuildGraphEvent]): Map[Int, Seq[(Int, Double)]] = {
    val currentNeighbors = graph(g_node)
    val currentReverseNeighbors = reverseNeighbors(g_node)
    val currentMaxDist = currentNeighbors.last._2
    val isNew: Boolean = !(currentNeighbors.exists(neighbor => neighbor._1 == potentialNeighbor._1)
      || currentReverseNeighbors.contains(potentialNeighbor._1))
    if (currentMaxDist > potentialNeighbor._2 && potentialNeighbor._1 != g_node && isNew) {
      joinNewNeighbor(currentNeighbors.slice(0, settings.k-1), currentReverseNeighbors, potentialNeighbor._1, toSend, nodeLocator)
      val removedNeighbor = currentNeighbors.last._1
      val responsibleActor = nodeLocator.findResponsibleActor(removedNeighbor)
      toSend(responsibleActor).addMessage(RemoveReverseNeighbor(removedNeighbor, g_node))
      val updatedNeighbors = (currentNeighbors :+ potentialNeighbor).sortBy(_._2).slice(0, settings.k)
      graph + (g_node -> updatedNeighbors)
    } else {
      graph
    }
  }

  def sendForLocation(nodeLocator: NodeLocator[BuildGraphEvent], remoteIndex: Int, waitingNodes: Seq[Int], toSend: Map[ActorRef[BuildGraphEvent], NNDInfo]): Unit = {
    val sendForLocation = waitingOnLocation.insertMultiple(remoteIndex, waitingNodes.toSet)
    if (sendForLocation) {
      toSend(nodeLocator.findResponsibleActor(remoteIndex)).addMessage(SendLocation(remoteIndex))
    }
  }

  def sendChangesImmediately(toSend: Map[ActorRef[BuildGraphEvent], NNDInfo]): Unit = {
    toSend.foreach { case (worker, nndInfo) =>
      if (nndInfo.sendImmediately && nndInfo.nonEmpty) {
        val messageToSend = nndInfo.sendMessage(settings.maxMessageSize)
        worker ! NNDescentInfo(messageToSend, ctx.self)
        nndInfo.sendImmediately = false
      }
    }
  }

  def checkIfDone(mightBeDoneWorkers: Set[ActorRef[BuildGraphEvent]],
                  nodeLocator: NodeLocator[BuildGraphEvent],
                  toSend: Map[ActorRef[BuildGraphEvent], NNDInfo],
                  alreadySaidImDone: Boolean): Boolean = {
    if (alreadySaidImDone) {
      true
    } else {
      val probablyDone = nodeLocator.allActors.size == mightBeDoneWorkers.size && toSend.forall(toSendTo => toSendTo._2.isEmpty)
      if (probablyDone) {
        clusterCoordinator ! FinishedNNDescent(ctx.self)
      }
      probablyDone
    }
  }
}



