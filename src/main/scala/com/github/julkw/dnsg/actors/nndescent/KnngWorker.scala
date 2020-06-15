package com.github.julkw.dnsg.actors.nndescent

import akka.actor.typed.receptionist.Receptionist

import scala.concurrent.duration._
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.{CoordinationEvent, CorrectFinishedNNDescent, FinishedApproximateGraph, FinishedNNDescent, KnngDistributionInfo}
import com.github.julkw.dnsg.actors.Coordinators.NodeCoordinator.{LocalKnngWorker, NodeCoordinationEvent}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.{GraphAndData, SearchOnGraphEvent}
import com.github.julkw.dnsg.actors.nndescent.NNDInfo.{AddReverseNeighbor, JoinNodes, NNDescentEvent, PotentialNeighbor, PotentialNeighborLocation, RemoveReverseNeighbor, SendLocation}
import com.github.julkw.dnsg.util.Data.{CacheData, LocalData}
import com.github.julkw.dnsg.util.KdTree.{IndexTree, KdTree, SplitNode, TreeBuilder, TreeNode}
import com.github.julkw.dnsg.util.{NodeLocator, Settings, WaitingOnLocation, dNSGSerializable}

import scala.language.postfixOps


object KnngWorker {

  sealed trait BuildGraphEvent extends dNSGSerializable

  // setup
  final case class ResponsibleFor(responsibility: Seq[Int], treeDepth: Int) extends BuildGraphEvent

  final case class BuildApproximateGraph(nodeLocator: NodeLocator[BuildGraphEvent]) extends BuildGraphEvent

  // build approximate graph
  final case class FindCandidates(index: Int) extends BuildGraphEvent

  final case class GetCandidates(query: Seq[Float], index: Int, replyTo: ActorRef[BuildGraphEvent]) extends BuildGraphEvent

  final case class Candidates(candidates: Seq[(Int, Double)], index: Int) extends BuildGraphEvent

  // improve approximate graph
  // start NNDescent
  final case object StartNNDescent extends BuildGraphEvent

  final case class CompleteLocalJoin(g_node: Int) extends BuildGraphEvent

  final case class SendReverseNeighbors(g_node: Int) extends BuildGraphEvent

  // during NNDescent
  final case class GetNNDescentInfo(sender: ActorRef[BuildGraphEvent]) extends BuildGraphEvent

  final case class NNDescentInfo(info: collection.Seq[NNDescentEvent], sender: ActorRef[BuildGraphEvent]) extends BuildGraphEvent

  // end NNDescent
  final case object NNDescentTimeout extends  BuildGraphEvent

  final case object NNDescentTimerKey

  // give final graph to SearchOnGraph Actor
  final case class MoveGraph(graphHolder: ActorRef[SearchOnGraphEvent]) extends BuildGraphEvent

  final case class WrappedSearchOnGraphEvent(event: SearchOnGraphActor.SearchOnGraphEvent) extends BuildGraphEvent

  final case class SOGDistributionInfo(treeNode: TreeNode[ActorRef[SearchOnGraphEvent]], sender: ActorRef[BuildGraphEvent]) extends BuildGraphEvent

  // TODO do over input
  val timeoutAfter = 3.second

  def apply(data: LocalData[Float],
            maxResponsibility: Int,
            clusterCoordinator: ActorRef[CoordinationEvent],
            localCoordinator: ActorRef[NodeCoordinationEvent]): Behavior[BuildGraphEvent] = Behaviors.setup { ctx =>
    //ctx.log.info("Started KnngWorker")
    localCoordinator ! LocalKnngWorker(ctx.self)
    val settings = Settings(ctx.system.settings.config)
    Behaviors.withTimers(timers =>
      new KnngWorker(CacheData(settings.cacheSize, data),
        new WaitingOnLocation, maxResponsibility, settings, clusterCoordinator, localCoordinator, timers, ctx).buildDistributionTree()
    )
  }
}

class KnngWorker(data: CacheData[Float],
                 waitingOnLocation: WaitingOnLocation,
                 maxResponsibility: Int,
                 settings: Settings,
                 clusterCoordinator: ActorRef[CoordinationEvent],
                 localCoordinator: ActorRef[NodeCoordinationEvent],
                 timers: TimerScheduler[KnngWorker.BuildGraphEvent],
                 ctx: ActorContext[KnngWorker.BuildGraphEvent]) extends Joiner(settings.sampleRate, data) {
  import KnngWorker._

  def buildDistributionTree(): Behavior[BuildGraphEvent] = Behaviors.receiveMessagePartial {
    case ResponsibleFor(responsibility, treeDepth) =>
      ctx.log.info("Holding {} nodes of max {}", responsibility.length, maxResponsibility)
      val treeBuilder: TreeBuilder = TreeBuilder(data.data, settings.k)
      if(responsibility.length > maxResponsibility) {
        // further split the data
        val splitNode: SplitNode[Seq[Int]] = treeBuilder.oneLevelSplit(responsibility)
        val right = ctx.spawn(KnngWorker(data.data, maxResponsibility, clusterCoordinator, localCoordinator), "KnngWorker" + treeDepth.toString)
        ctx.self ! ResponsibleFor(splitNode.left.data, treeDepth + 1)
        right ! ResponsibleFor(splitNode.right.data, treeDepth + 1)
        buildDistributionTree()
      } else {
        // this is a leaf node for data distribution
        clusterCoordinator ! KnngDistributionInfo(responsibility, ctx.self)
        // Build local tree while waiting on DistributionTree message
        val treeBuilder: TreeBuilder = TreeBuilder(data.data, settings.k)
        val kdTree: IndexTree = treeBuilder.construct(responsibility)
        ctx.log.info("Finished building kdTree")
        waitForDistributionInfo(kdTree, responsibility)
      }
  }

  def waitForDistributionInfo(kdTree: IndexTree,
                              responsibility: Seq[Int]): Behavior[BuildGraphEvent] =
    Behaviors.receiveMessagePartial {
      case BuildApproximateGraph(nodeLocator) =>
        ctx.log.info("Received Distribution Info. Start building approximate graph")
        ctx.self ! FindCandidates(0)
        val candidates: Array[Seq[(Int, Double)]] = Array.fill(responsibility.length){Seq.empty}
        val awaitingAnswer: Array[Int] = Array.fill(responsibility.length){0}
        val graph: Map[Int, Seq[(Int, Double)]] = Map.empty
        buildApproximateGraph(kdTree, responsibility, candidates, awaitingAnswer, nodeLocator, graph)

      case GetCandidates(query, index, sender) =>
        val localCandidates = findLocalCandidates(query, kdTree)
        sender ! Candidates(localCandidates, index)
        waitForDistributionInfo(kdTree, responsibility)
    }

  def buildApproximateGraph(kdTree: IndexTree,
                            responsibility: Seq[Int],
                            candidates: Array[Seq[(Int, Double)]],
                            awaitingAnswer: Array[Int],
                            nodeLocator: NodeLocator[BuildGraphEvent],
                            graph:Map[Int, Seq[(Int, Double)]]): Behavior[BuildGraphEvent] =
    Behaviors.receiveMessagePartial {
      case FindCandidates(responsibilityIndex) =>
        // Also look for the closest neighbors of the other g_nodes
        if (responsibilityIndex < responsibility.length - 1) {
          ctx.self ! FindCandidates(responsibilityIndex + 1)
        }
        val index = responsibility(responsibilityIndex)
        val query: Seq[Float] = data.get(index)
        // find candidates for current node by asking all workers for candidates to ensure a connected graph across all nodes
        nodeLocator.allActors.foreach { worker =>
          if (worker != ctx.self) {
            worker ! GetCandidates(query, responsibilityIndex, ctx.self)
            awaitingAnswer(responsibilityIndex) += 1
          }
        }
        val localCandidates = findLocalCandidates(query, kdTree)
        ctx.self ! Candidates(localCandidates, responsibilityIndex)
        awaitingAnswer(responsibilityIndex) += 1
        buildApproximateGraph(kdTree, responsibility, candidates, awaitingAnswer, nodeLocator, graph)

      case GetCandidates(query, index, sender) =>
        val localCandidates = findLocalCandidates(query, kdTree)
        sender ! Candidates(localCandidates, index)
        buildApproximateGraph(kdTree, responsibility, candidates, awaitingAnswer, nodeLocator, graph)

      case Candidates(newCandidates, responsibilityIndex) =>
        val graphIndex: Int = responsibility(responsibilityIndex)
        val oldCandidates = candidates(responsibilityIndex)
        val mergedCandidates = (oldCandidates ++ newCandidates).sortBy(_._2)
        val skipSelf = if (mergedCandidates.head._1 == graphIndex) 1 else 0
        candidates(responsibilityIndex) = mergedCandidates.slice(skipSelf, settings.k + skipSelf)
        awaitingAnswer(responsibilityIndex) -= 1
        if (awaitingAnswer(responsibilityIndex) == 0) {
          val updatedGraph = graph + (graphIndex -> candidates(responsibilityIndex))
          if (updatedGraph.size == responsibility.length) {
            clusterCoordinator ! FinishedApproximateGraph
          }
          buildApproximateGraph(kdTree, responsibility, candidates, awaitingAnswer, nodeLocator, updatedGraph)
        } else {
          buildApproximateGraph(kdTree, responsibility, candidates, awaitingAnswer, nodeLocator, graph)
        }

      case StartNNDescent =>
        startNNDescent(nodeLocator, graph)

      case GetNNDescentInfo(sender) =>
        ctx.self ! GetNNDescentInfo(sender)
        startNNDescent(nodeLocator, graph)
    }

  def startNNDescent(nodeLocator: NodeLocator[BuildGraphEvent],
                     graph: Map[Int, Seq[(Int, Double)]]): Behavior[BuildGraphEvent] = {
    //ctx.log.info("Starting nnDescent")
    // for debugging
    ctx.log.info("Average distance in graph before nndescent: {}", averageGraphDist(graph, settings.k))
    graph.keys.foreach{ g_node =>
      // do the initial local joins through messages to self to prevent Heartbeat problems
      ctx.self ! SendReverseNeighbors(g_node)
      ctx.self ! CompleteLocalJoin(g_node)
    }
    nodeLocator.allActors.foreach(worker => worker ! GetNNDescentInfo(ctx.self))
    val toSend = nodeLocator.allActors.map(worker => worker -> new NNDInfo).toMap
    // setup timer used to determine when the graph is done
    timers.startSingleTimer(NNDescentTimerKey, NNDescentTimeout, timeoutAfter)
    // start nnDescent
    val reverseNeighbors: Map[Int, Set[Int]] = graph.map{case (index, _) => index -> Set.empty}
    nnDescent(nodeLocator, graph, reverseNeighbors, toSend)
  }

  def nnDescent(nodeLocator: NodeLocator[BuildGraphEvent],
                graph: Map[Int, Seq[(Int, Double)]],
                reverseNeighbors: Map[Int, Set[Int]],
                toSend: Map[ActorRef[BuildGraphEvent], NNDInfo]): Behavior[BuildGraphEvent] =
    Behaviors.receiveMessagePartial {
      case StartNNDescent =>
        // already done, so do nothing
        nnDescent(nodeLocator, graph, reverseNeighbors, toSend)

      case SendReverseNeighbors(g_node) =>
        val neighbors = graph(g_node)
        neighbors.foreach{case (neighborIndex, _) =>
          val responsibleNeighbor = nodeLocator.findResponsibleActor(neighborIndex)
          toSend(responsibleNeighbor).addMessage(AddReverseNeighbor(neighborIndex, g_node))
        }
        nnDescent(nodeLocator, graph, reverseNeighbors, toSend)

      case CompleteLocalJoin(g_node) =>
        //ctx.log.info("local join")
        // prevent timeouts in the initial phase of graph nnDescent
        timers.cancel(NNDescentTimerKey)
        val neighbors = graph(g_node)
        joinNeighbors(neighbors, toSend, nodeLocator)
        // if this is the last inital join, the timer is needed from now on
        timers.startSingleTimer(NNDescentTimerKey, NNDescentTimeout, timeoutAfter)
        nnDescent(nodeLocator, graph, reverseNeighbors, toSend)

      case GetNNDescentInfo(sender) =>
        // TODO check if anything to send there, if not remember to send when there is
        val messagesToSend = toSend(sender).sendMessage(settings.nnDescentMessageSize)
        if (messagesToSend.nonEmpty) {
          sender ! NNDescentInfo(messagesToSend, ctx.self)
        } else {
          toSend(sender).sendImmediately = true
        }
        nnDescent(nodeLocator, graph, reverseNeighbors, toSend)

      case NNDescentInfo(info, sender) =>
        if (timers.isTimerActive(NNDescentTimerKey)) {
          timers.cancel(NNDescentTimerKey)
        } else {
          // if the timer is inactive, it has already run out and the actor has mistakenly told its supervisor that it is done
          clusterCoordinator ! CorrectFinishedNNDescent
        }
        // TODO find a nicer way to do this
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
            //ctx.log.info("add reverse neighbor")
            // if new and not already a neighbor, introduce to all current neighbors
            if (!updatedGraph(g_nodeIndex).exists(neighbor => neighbor._1 == neighborIndex)) {
              joinNewNeighbor(updatedGraph(g_nodeIndex), updatedReverseNeighbors(g_nodeIndex), neighborIndex, toSend, nodeLocator)
            }
            // update reverse neighbors
            updatedReverseNeighbors += (g_nodeIndex -> (updatedReverseNeighbors(g_nodeIndex) + neighborIndex))

          case RemoveReverseNeighbor(g_nodeIndex, neighborIndex) =>
            //ctx.log.info("remove reverse neighbor")
            updatedReverseNeighbors += (g_nodeIndex -> (updatedReverseNeighbors(g_nodeIndex) - neighborIndex))
        }
        sender ! GetNNDescentInfo(ctx.self)
        sendChangesImmediately(toSend)
        timers.startSingleTimer(NNDescentTimerKey, NNDescentTimeout, timeoutAfter)
        nnDescent(nodeLocator, updatedGraph, updatedReverseNeighbors, toSend)

      case NNDescentTimeout =>
        clusterCoordinator ! FinishedNNDescent
        nnDescent(nodeLocator, graph, reverseNeighbors, toSend)

      case MoveGraph(graphHolder) =>
        ctx.log.info("Average distance in graph after nndescent: {}", averageGraphDist(graph, settings.k))
        // all nodes are done with NNDescent, nothing will change with the graph anymore, so it is moved to SearchOnGraph actors
        val cleanedGraph: Map[Int, Seq[Int]] = graph.map{case (index, neighbors) => index -> neighbors.map(_._1)}
        val searchOnGraphEventAdapter: ActorRef[SearchOnGraphActor.SearchOnGraphEvent] =
          ctx.messageAdapter { event => WrappedSearchOnGraphEvent(event)}
        graphHolder ! GraphAndData(cleanedGraph, data, searchOnGraphEventAdapter)
        // TODO add some kind of check to ensure the arrival of the graph?
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
      if (nndInfo.sendImmediately) {
        val messageToSend = nndInfo.sendMessage(settings.nnDescentMessageSize)
        if (messageToSend.nonEmpty) {
          worker ! NNDescentInfo(messageToSend, ctx.self)
          nndInfo.sendImmediately = false
        }
      }
    }
  }
}



