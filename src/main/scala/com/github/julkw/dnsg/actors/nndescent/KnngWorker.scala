package com.github.julkw.dnsg.actors.nndescent

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.{ConfirmFinishedNNDescent, CoordinationEvent, CorrectFinishedNNDescent, FinishedApproximateGraph, FinishedNNDescent, KNearestNeighborsWithDist}
import com.github.julkw.dnsg.actors.NodeLocatorHolder.{KnngWorkerGotGraphFrom, NodeLocationEvent}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.{FindNearestNeighbors, GraphAndData, SearchOnGraphEvent}
import com.github.julkw.dnsg.actors.nndescent.NNDescentMessageBuffer.{AddReverseNeighbor, JoinNodes, NNDescentEvent, PotentialNeighbor, PotentialNeighborLocation, RemoveReverseNeighbor, SendLocation}
import com.github.julkw.dnsg.util.Data.LocalData
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
    new KnngWorker(data, new WaitingOnLocation, graphNodeLocator,
      parent, settings, clusterCoordinator, localNodeLocatorHolder, coordinationEventAdapter, ctx).setup()
  }
}

class KnngWorker(data: LocalData[Float],
                 waitingOnLocation: WaitingOnLocation[(Int, Int)],
                 graphNodeLocator: NodeLocator[SearchOnGraphEvent],
                 parent: ActorRef[SearchOnGraphEvent],
                 settings: Settings,
                 clusterCoordinator: ActorRef[CoordinationEvent],
                 localNodeLocatorHolder: ActorRef[NodeLocationEvent],
                 coordinationEventAdapter: ActorRef[CoordinationEvent],
                 ctx: ActorContext[KnngWorker.BuildKNNGEvent]) extends Joiner(settings.nnDescentIterations, data) {
  import KnngWorker._

  def setup(): Behavior[BuildKNNGEvent] = {
    localNodeLocatorHolder ! KnngWorkerGotGraphFrom(parent, ctx.self)
    val myNodes = graphNodeLocator.nodesOf(parent)
    val queries = myNodes.map(index => (data.get(index), index))
    // ask for one neighbor too many in case the node itself ends up in the result set
    // send all requests at once since this is a local message and therefore not limited in size
    val sendWithDist = true
    parent ! FindNearestNeighbors(queries, settings.preNNDescentK + 1, coordinationEventAdapter, sendWithDist, false)
    buildInitialGraph(myNodes.toSet, NNDGraph(settings.k, myNodes.toArray, settings.maxReverseNeighborsNND), None)
  }

  def buildInitialGraph(waitingOnNodes: Set[Int], graph: NNDGraph, nodeLocator: Option[NodeLocator[BuildKNNGEvent]]): Behavior[BuildKNNGEvent] = Behaviors.receiveMessagePartial {
    case WrappedCoordinationEvent(event) =>
      event match {
        case KNearestNeighborsWithDist(node, neighbors) =>
          val selfIn = if (neighbors.head._1 == node) { 1 } else { 0 }
          graph.insertInitial(node, neighbors.slice(selfIn, settings.k + selfIn))
          if (waitingOnNodes.size == 1 && nodeLocator.isDefined) {
            clusterCoordinator ! FinishedApproximateGraph(ctx.self)
            waitForNNDescent(nodeLocator.get, graph)
          } else {
            buildInitialGraph(waitingOnNodes - node, graph, nodeLocator)
          }
      }

    case KnngWorkerNodeLocator(newNodeLocator) =>
      if (waitingOnNodes.isEmpty) {
        clusterCoordinator ! FinishedApproximateGraph(ctx.self)
        waitForNNDescent(newNodeLocator, graph)
      } else {
        buildInitialGraph(waitingOnNodes, graph, Some(newNodeLocator))
      }

    case StartNNDescent =>
      ctx.self ! StartNNDescent
      buildInitialGraph(waitingOnNodes, graph, nodeLocator)

    case GetNNDescentInfo(sender) =>
      ctx.self ! GetNNDescentInfo(sender)
      buildInitialGraph(waitingOnNodes, graph, nodeLocator)
  }

  def waitForNNDescent(nodeLocator: NodeLocator[BuildKNNGEvent],
                       graph: NNDGraph): Behavior[BuildKNNGEvent] = Behaviors.receiveMessagePartial {
    case StartNNDescent =>
      startNNDescent(nodeLocator, graph)

    case NNDescentInfo(info, sender) =>
      ctx.self ! NNDescentInfo(info, sender)
      startNNDescent(nodeLocator, graph)
  }

  def startNNDescent(nodeLocator: NodeLocator[BuildKNNGEvent],
                     graph: NNDGraph): Behavior[BuildKNNGEvent] = {
    // ctx.log.info("Average distance in graph before nndescent: {}", averageGraphDist(graph))
    graph.nodes.foreach { g_node =>
      // do the initial local joins through messages to self to prevent Heartbeat problems
      ctx.self ! CompleteLocalJoin(g_node)
    }
    val toSend = NNDescentMessageBuffer(graph.nodes, nodeLocator.allActors)
    logToSendSize(toSend)
    // add reverse neighbors
    graph.nodes.foreach { g_node =>
      graph.getNeighbors(g_node).foreach { neighbor =>
        val responsibleNeighbor = nodeLocator.findResponsibleActor(neighbor)
        toSend.addNodeMessage(AddReverseNeighbor(neighbor, g_node, 0), responsibleNeighbor, g_node)
      }
    }
    nnDescent(nodeLocator, graph, toSend, Set.empty, saidImDone = false)
  }

  def nnDescent(nodeLocator: NodeLocator[BuildKNNGEvent],
                graph: NNDGraph,
                toSend: NNDescentMessageBuffer,
                mightBeDone: Set[ActorRef[BuildKNNGEvent]],
                saidImDone: Boolean): Behavior[BuildKNNGEvent] =
    Behaviors.receiveMessagePartial {
      case StartNNDescent =>
        // already done, so do nothing
        nnDescent(nodeLocator, graph, toSend, mightBeDone, saidImDone)

      case CompleteLocalJoin(g_node) =>
        // prevent timeouts in the initial phase of graph nnDescent
        val neighbors = graph.getNeighbors(g_node)
        joinNeighbors(neighbors, iteration = 1, g_node, toSend, nodeLocator)
        sendChangesImmediately(toSend, nodeLocator)
        logToSendSize(toSend)
        nnDescent(nodeLocator, graph, toSend, mightBeDone, saidImDone)

      case GetNNDescentInfo(sender) =>
        val messagesToSend = toSend.messageTo(sender, settings.maxMessageSize)
        sender ! NNDescentInfo(messagesToSend, ctx.self)
        logToSendSize(toSend)
        val probablyDone = if (messagesToSend.isEmpty) {
          checkIfDone(mightBeDone, nodeLocator, toSend, saidImDone)
        } else { saidImDone }
        nnDescent(nodeLocator, graph, toSend, mightBeDone, probablyDone)

      case NNDescentInfo(info, sender) =>
        if (info.isEmpty) {
          val mightBeDoneWorkers = mightBeDone + sender
          val probablyDone = checkIfDone(mightBeDoneWorkers, nodeLocator, toSend, saidImDone)
          nnDescent(nodeLocator, graph, toSend, mightBeDoneWorkers, probablyDone)
        } else {
          sender ! GetNNDescentInfo(ctx.self)
          logToSendSize(toSend)
          if (saidImDone) {
            clusterCoordinator ! CorrectFinishedNNDescent(ctx.self)
          }
          info.foreach {
            case JoinNodes(g_node, potentialNeighbor, iteration) =>
              if (data.isLocal(potentialNeighbor)) {
                val neighborData = data.get(potentialNeighbor)
                joinLocals(g_node, data.get(g_node), potentialNeighbor, neighborData, iteration, joiningNode = -1, toSend, nodeLocator)
              }
              else {
                sendForLocation(nodeLocator, potentialNeighbor, (g_node, iteration), toSend)
              }

            case SendLocation(g_node) =>
              toSend.addNodeIndependentMessage(PotentialNeighborLocation(g_node, data.get(g_node)), sender)

            case PotentialNeighborLocation(potentialNeighborIndex, location) =>
              waitingOnLocation.received(potentialNeighborIndex).foreach { case (g_node, iteration) =>
                joinLocals(g_node, data.get(g_node), potentialNeighborIndex, location, iteration, joiningNode = -1, toSend, nodeLocator)
              }

            case PotentialNeighbor(g_node, potentialNeighbor, distance, iteration) =>
              handlePotentialNeighbor(g_node, potentialNeighbor, distance, iteration, graph, toSend, nodeLocator)

            case AddReverseNeighbor(g_node, newNeighbor, iteration) =>
              // if new and not already a neighbor, introduce to all current neighbors
              graph.addReverseNeighbor(g_node, newNeighbor)
              if (!graph.getNeighbors(g_node).contains(newNeighbor)) {
                joinNode(newNeighbor, graph.getNeighbors(g_node), graph.getReversedNeighbors(g_node), iteration, g_node, toSend, nodeLocator)
              }

            case RemoveReverseNeighbor(g_nodeIndex, neighborIndex) =>
              graph.removeReverseNeighbor(g_nodeIndex, neighborIndex, toSend)
          }
          logToSendSize(toSend)
          sendChangesImmediately(toSend, nodeLocator)
          nnDescent(nodeLocator, graph, toSend, mightBeDone - sender, saidImDone = false)
        }

      case GetNNDescentFinishedConfirmation =>
        if (saidImDone) {
          clusterCoordinator ! ConfirmFinishedNNDescent(ctx.self)
        } // else my correction did not make it there in time but is still on the way so I do not need to send it again
        nnDescent(nodeLocator, graph, toSend, mightBeDone, saidImDone)

      case MoveGraph =>
        // move graph to SearchOnGraphActor
        // ctx.log.info("Average distance in graph after nndescent: {}", averageGraphDist(graph))
        parent ! GraphAndData(graph.cleanedGraph(), data, ctx.self)
        waitForShutdown()

      case AllKnngWorkersDone =>
        ctx.log.info("Got the message to shutdown before moving my graph")
        Behaviors.stopped
    }

  def waitForShutdown(): Behavior[BuildKNNGEvent] = Behaviors.receiveMessagePartial {
    case AllKnngWorkersDone =>
      Behaviors.stopped
  }

  def handlePotentialNeighbor(g_node: Int,
                              potentialNeighbor: Int,
                              distance: Double,
                              iteration: Int,
                              graph: NNDGraph,
                              toSend: NNDescentMessageBuffer,
                              nodeLocator: NodeLocator[BuildKNNGEvent]): Unit = {
    val isNeighbor = graph.insert(g_node, potentialNeighbor, distance, iteration, toSend, nodeLocator)
    if (isNeighbor) {
      joinNode(potentialNeighbor, graph.getNeighbors(g_node), graph.getReversedNeighbors(g_node), iteration, g_node, toSend, nodeLocator)
    }
  }

  def sendForLocation(nodeLocator: NodeLocator[BuildKNNGEvent], remoteIndex: Int, waitingNode: (Int, Int), toSend: NNDescentMessageBuffer): Unit = {
    val sendForLocation = waitingOnLocation.insert(remoteIndex, waitingNode)
    if (sendForLocation) {
      toSend.addNodeIndependentMessage(SendLocation(remoteIndex), nodeLocator.findResponsibleActor(remoteIndex))
    }
  }

  def sendChangesImmediately(toSend: NNDescentMessageBuffer, nodeLocator: NodeLocator[BuildKNNGEvent]): Unit = {
    nodeLocator.allActors.foreach { worker =>
      if (toSend.sendImmediately(worker) && toSend.nonEmpty(worker)) {
        val messageToSend = toSend.messageTo(worker, settings.maxMessageSize)
        worker ! NNDescentInfo(messageToSend, ctx.self)
      }
    }
    logToSendSize(toSend)
  }

  def checkIfDone(mightBeDoneWorkers: Set[ActorRef[BuildKNNGEvent]],
                  nodeLocator: NodeLocator[BuildKNNGEvent],
                  toSend: NNDescentMessageBuffer,
                  alreadySaidImDone: Boolean): Boolean = {
    if (alreadySaidImDone) {
      true
    } else {
      val probablyDone = nodeLocator.allActors.size == mightBeDoneWorkers.size && toSend.nothingToSend
      if (probablyDone) {
        clusterCoordinator ! FinishedNNDescent(ctx.self)
      }
      probablyDone
    }
  }

  def logToSendSize(toSend: NNDescentMessageBuffer): Unit = {
    ctx.log.info("Currently holding {} messages to send later", toSend.numMessages)
  }
}



