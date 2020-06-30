package com.github.julkw.dnsg.actors.nndescent

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.{ConfirmFinishedNNDescent, CoordinationEvent, CorrectFinishedNNDescent, FinishedApproximateGraph, FinishedNNDescent, KNearestNeighborsWithDist}
import com.github.julkw.dnsg.actors.NodeLocatorHolder.{KnngWorkerGotGraphFrom, NodeLocationEvent}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.{FindNearestNeighbors, GraphAndData, SearchOnGraphEvent}
import com.github.julkw.dnsg.actors.nndescent.NNDInfo.{AddReverseNeighbor, JoinNodes, NNDescentEvent, PotentialNeighbor, PotentialNeighborLocation, RemoveReverseNeighbor, SendLocation}
import com.github.julkw.dnsg.util.Data.{CacheData, LocalData}
import com.github.julkw.dnsg.util.{NodeLocator, Settings, WaitingOnLocation, dNSGSerializable}

import scala.language.postfixOps


object KnngWorker {

  sealed trait BuildKNNGEvent extends dNSGSerializable

  // setup
  final case class WrappedCoordinationEvent(event: CoordinationEvent) extends BuildKNNGEvent

  final case class KnngWorkerNodeLocator(nodeLocator: NodeLocator[BuildKNNGEvent]) extends BuildKNNGEvent

  // improve approximate graph
  // start NNDescent
  final case object StartNNDescent extends BuildKNNGEvent

  final case class CompleteLocalJoin(g_node: Int) extends BuildKNNGEvent

  // during NNDescent
  final case class GetNNDescentInfo(sender: ActorRef[BuildKNNGEvent]) extends BuildKNNGEvent

  final case class NNDescentInfo(info: collection.Seq[NNDescentEvent], sender: ActorRef[BuildKNNGEvent]) extends BuildKNNGEvent

  final case class NoNewInfo(sender: ActorRef[BuildKNNGEvent]) extends BuildKNNGEvent

  final case object GetNNDescentFinishedConfirmation extends BuildKNNGEvent

  // after NNDescent
  final case object MoveGraph extends BuildKNNGEvent

  final case object AllKnngWorkersDone extends BuildKNNGEvent

  def apply(data: LocalData[Float],
            graphNodeLocator: NodeLocator[SearchOnGraphEvent],
            parent: ActorRef[SearchOnGraphEvent],
            clusterCoordinator: ActorRef[CoordinationEvent],
            localNodeLocatorHolder: ActorRef[NodeLocationEvent]): Behavior[BuildKNNGEvent] = Behaviors.setup { ctx =>
    val settings = Settings(ctx.system.settings.config)
    val coordinationEventAdapter: ActorRef[CoordinationEvent] =
      ctx.messageAdapter { event => WrappedCoordinationEvent(event)}
    new KnngWorker(CacheData(settings.cacheSize, data),
      new WaitingOnLocation, graphNodeLocator,
      parent, settings, clusterCoordinator, localNodeLocatorHolder, coordinationEventAdapter, ctx).setup()
  }
}

class KnngWorker(data: CacheData[Float],
                 waitingOnLocation: WaitingOnLocation,
                 graphNodeLocator: NodeLocator[SearchOnGraphEvent],
                 parent: ActorRef[SearchOnGraphEvent],
                 settings: Settings,
                 clusterCoordinator: ActorRef[CoordinationEvent],
                 localNodeLocatorHolder: ActorRef[NodeLocationEvent],
                 coordinationEventAdapter: ActorRef[CoordinationEvent],
                 ctx: ActorContext[KnngWorker.BuildKNNGEvent]) extends Joiner(settings.sampleRate, data) {
  import KnngWorker._

  def setup(): Behavior[BuildKNNGEvent] = {
    localNodeLocatorHolder ! KnngWorkerGotGraphFrom(parent, ctx.self)
    val queries = graphNodeLocator.nodesOf(parent).map(index => data.get(index) -> index).toMap
    // ask for one neighbor too many in case the node itself ends up in the result set
    // send all requests at once since this is a local message and therefore not limited in size
    val sendWithDist = true
    parent ! FindNearestNeighbors(queries.keys.toSeq, settings.preNNDescentK + 1, coordinationEventAdapter, sendWithDist, false)
    buildInitialGraph(queries, Map.empty, None)
  }

  def buildInitialGraph(queries: Map[Seq[Float], Int], graph: Map[Int, Seq[(Int, Double)]], nodeLocator: Option[NodeLocator[BuildKNNGEvent]]): Behavior[BuildKNNGEvent] = Behaviors.receiveMessagePartial {
    case WrappedCoordinationEvent(event) =>
      event match {
        case KNearestNeighborsWithDist(query, neighbors) =>
          val node = queries(query)
          val selfIn = if (neighbors.head._1 == node) { 1 } else { 0 }
          val actualNeighbors = neighbors.slice(selfIn, settings.k + selfIn)
          val updatedGraph = graph + (node -> actualNeighbors)
          if (updatedGraph.size == queries.size && nodeLocator.isDefined) {
            clusterCoordinator ! FinishedApproximateGraph(ctx.self)
            waitForNNDescent(nodeLocator.get, updatedGraph)
          } else {
            buildInitialGraph(queries, updatedGraph, nodeLocator)
          }
      }

    case KnngWorkerNodeLocator(newNodeLocator) =>
      if (graph.size == queries.size) {
        clusterCoordinator ! FinishedApproximateGraph(ctx.self)
        waitForNNDescent(newNodeLocator, graph)
      } else {
        buildInitialGraph(queries, graph, Some(newNodeLocator))
      }

    case StartNNDescent =>
      ctx.self ! StartNNDescent
      buildInitialGraph(queries, graph, nodeLocator)

    case GetNNDescentInfo(sender) =>
      ctx.self ! GetNNDescentInfo(sender)
      buildInitialGraph(queries, graph, nodeLocator)
  }

  def waitForNNDescent(nodeLocator: NodeLocator[BuildKNNGEvent],
                       graph: Map[Int, Seq[(Int, Double)]]): Behavior[BuildKNNGEvent] = Behaviors.receiveMessagePartial {
    case StartNNDescent =>
      startNNDescent(nodeLocator, graph)

    case GetNNDescentInfo(sender) =>
      ctx.self ! GetNNDescentInfo(sender)
      startNNDescent(nodeLocator, graph)
  }

  def startNNDescent(nodeLocator: NodeLocator[BuildKNNGEvent],
                     graph: Map[Int, Seq[(Int, Double)]]): Behavior[BuildKNNGEvent] = {
    // for debugging
    //ctx.log.info("Average distance in graph before nndescent: {}", averageGraphDist(graph, settings.k))
    ctx.log.info("Starting NNDescent")

    graph.keys.foreach{ g_node =>
      // do the initial local joins through messages to self to prevent Heartbeat problems
      ctx.self ! CompleteLocalJoin(g_node)
    }
    nodeLocator.allActors.foreach(worker => worker ! GetNNDescentInfo(ctx.self))

    val toSend = nodeLocator.allActors.map(worker => worker -> new NNDInfo).toMap
    graph.keys.foreach { g_node =>
      graph(g_node).foreach { case (neighborIndex, _) =>
        val responsibleNeighbor = nodeLocator.findResponsibleActor(neighborIndex)
        toSend(responsibleNeighbor).addMessage(AddReverseNeighbor(neighborIndex, g_node))
      }
    }
    // setup timer used to determine when the graph is done
    // start nnDescent
    val reverseNeighbors: Map[Int, Set[Int]] = graph.map{case (index, _) => index -> Set.empty}
    nnDescent(nodeLocator, graph, reverseNeighbors, toSend, Set.empty, saidImDone = false)
  }

  def nnDescent(nodeLocator: NodeLocator[BuildKNNGEvent],
                graph: Map[Int, Seq[(Int, Double)]],
                reverseNeighbors: Map[Int, Set[Int]],
                toSend: Map[ActorRef[BuildKNNGEvent], NNDInfo],
                mightBeDone: Set[ActorRef[BuildKNNGEvent]],
                saidImDone: Boolean): Behavior[BuildKNNGEvent] =
    Behaviors.receiveMessagePartial {
      case StartNNDescent =>
        // already done, so do nothing
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

      case MoveGraph =>
        // move graph to SearchOnGraphActor
        //ctx.log.info("Average distance in graph after nndescent: {}", averageGraphDist(graph, settings.k))
        val cleanedGraph: Map[Int, Seq[Int]] = graph.map{case (index, neighbors) => index -> neighbors.map(_._1)}
        parent ! GraphAndData(cleanedGraph, data, ctx.self)
        waitForShutdown()

      case AllKnngWorkersDone =>
        ctx.log.info("Got the message to shutdown before moving my graph of size: {}", graph.size)
        Behaviors.stopped
    }

  def waitForShutdown(): Behavior[BuildKNNGEvent] = Behaviors.receiveMessagePartial {
    case AllKnngWorkersDone =>
      Behaviors.stopped
  }

  def handlePotentialNeighbor(g_node: Int,
                              potentialNeighbor: (Int, Double),
                              graph: Map[Int, Seq[(Int, Double)]],
                              reverseNeighbors: Map[Int, Set[Int]],
                              toSend: Map[ActorRef[BuildKNNGEvent], NNDInfo],
                              nodeLocator: NodeLocator[BuildKNNGEvent]): Map[Int, Seq[(Int, Double)]] = {
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

  def sendForLocation(nodeLocator: NodeLocator[BuildKNNGEvent], remoteIndex: Int, waitingNodes: Seq[Int], toSend: Map[ActorRef[BuildKNNGEvent], NNDInfo]): Unit = {
    val sendForLocation = waitingOnLocation.insertMultiple(remoteIndex, waitingNodes.toSet)
    if (sendForLocation) {
      toSend(nodeLocator.findResponsibleActor(remoteIndex)).addMessage(SendLocation(remoteIndex))
    }
  }

  def sendChangesImmediately(toSend: Map[ActorRef[BuildKNNGEvent], NNDInfo]): Unit = {
    toSend.foreach { case (worker, nndInfo) =>
      if (nndInfo.sendImmediately && nndInfo.nonEmpty) {
        val messageToSend = nndInfo.sendMessage(settings.maxMessageSize)
        worker ! NNDescentInfo(messageToSend, ctx.self)
        nndInfo.sendImmediately = false
      }
    }
  }

  def checkIfDone(mightBeDoneWorkers: Set[ActorRef[BuildKNNGEvent]],
                  nodeLocator: NodeLocator[BuildKNNGEvent],
                  toSend: Map[ActorRef[BuildKNNGEvent], NNDInfo],
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



