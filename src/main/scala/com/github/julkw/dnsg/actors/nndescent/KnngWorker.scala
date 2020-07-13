package com.github.julkw.dnsg.actors.nndescent

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.{ConfirmFinishedNNDescent, CoordinationEvent, CorrectFinishedNNDescent, FinishedApproximateGraph, FinishedNNDescent, KNearestNeighborsWithDist}
import com.github.julkw.dnsg.actors.NodeLocatorHolder.{KnngWorkerGotGraphFrom, NodeLocationEvent}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.{FindNearestNeighbors, GraphAndData, SearchOnGraphEvent}
import com.github.julkw.dnsg.actors.nndescent.NNDInfo.{AddReverseNeighbor, JoinNodes, NNDescentEvent, PotentialNeighbor, PotentialNeighborLocation, RemoveReverseNeighbor, SendLocation}
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
    buildInitialGraph(myNodes.toSet, NNDGraph(settings.k, myNodes, settings.maxReverseNeighborsNND), None)
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

    case GetNNDescentInfo(sender) =>
      ctx.self ! GetNNDescentInfo(sender)
      startNNDescent(nodeLocator, graph)
  }

  def startNNDescent(nodeLocator: NodeLocator[BuildKNNGEvent],
                     graph: NNDGraph): Behavior[BuildKNNGEvent] = {
    // ctx.log.info("Average distance in graph before nndescent: {}", averageGraphDist(graph))
    graph.nodes.foreach { g_node =>
      // do the initial local joins through messages to self to prevent Heartbeat problems
      ctx.self ! CompleteLocalJoin(g_node)
    }
    nodeLocator.allActors.foreach(worker => worker ! GetNNDescentInfo(ctx.self))
    val toSend = nodeLocator.allActors.map(worker => worker -> new NNDInfo).toMap
    // add reverse neighbors
    graph.nodes.foreach { g_node =>
      graph.getNeighbors(g_node).foreach { neighbor =>
        val responsibleNeighbor = nodeLocator.findResponsibleActor(neighbor)
        toSend(responsibleNeighbor).addMessage(AddReverseNeighbor(neighbor, g_node, 0))
      }
    }
    nnDescent(nodeLocator, graph, toSend, Set.empty, saidImDone = false)
  }

  def nnDescent(nodeLocator: NodeLocator[BuildKNNGEvent],
                graph: NNDGraph,
                toSend: Map[ActorRef[BuildKNNGEvent], NNDInfo],
                mightBeDone: Set[ActorRef[BuildKNNGEvent]],
                saidImDone: Boolean): Behavior[BuildKNNGEvent] =
    Behaviors.receiveMessagePartial {
      case StartNNDescent =>
        // already done, so do nothing
        nnDescent(nodeLocator, graph, toSend, mightBeDone, saidImDone)

      case CompleteLocalJoin(g_node) =>
        // prevent timeouts in the initial phase of graph nnDescent
        val neighbors = graph.getNeighbors(g_node)
        joinNeighbors(neighbors, iteration = 1, toSend, nodeLocator)
        nnDescent(nodeLocator, graph, toSend, mightBeDone, saidImDone)

      case GetNNDescentInfo(sender) =>
        val messagesToSend = toSend(sender).sendMessage(settings.maxMessageSize)
        if (messagesToSend.nonEmpty) {
          sender ! NNDescentInfo(messagesToSend, ctx.self)
          nnDescent(nodeLocator, graph, toSend, mightBeDone, saidImDone)
        } else {
          sender ! NoNewInfo(ctx.self)
          toSend(sender).sendImmediately = true
          val probablyDone = checkIfDone(mightBeDone, nodeLocator, toSend, saidImDone)
          nnDescent(nodeLocator, graph, toSend, mightBeDone, probablyDone)
        }

      case NoNewInfo(sender) =>
        val mightBeDoneWorkers = mightBeDone + sender
        val probablyDone = checkIfDone(mightBeDoneWorkers, nodeLocator, toSend, saidImDone)
        nnDescent(nodeLocator, graph, toSend, mightBeDoneWorkers, probablyDone)

      case NNDescentInfo(info, sender) =>
        if (saidImDone) {
          clusterCoordinator ! CorrectFinishedNNDescent(ctx.self)
        }
        info.foreach {
          case JoinNodes(g_node, potentialNeighbor, iteration) =>
            if (data.isLocal(potentialNeighbor)) {
              val neighborData = data.get(potentialNeighbor)
              joinLocals(g_node, data.get(g_node), potentialNeighbor, neighborData, iteration, toSend, nodeLocator)
            }
            else {
              sendForLocation(nodeLocator, potentialNeighbor, (g_node, iteration), toSend)
            }

          case SendLocation(g_node) =>
            toSend(sender).addMessage(PotentialNeighborLocation(g_node, data.get(g_node)))

          case PotentialNeighborLocation(potentialNeighborIndex, location) =>
            waitingOnLocation.received(potentialNeighborIndex).foreach { case (g_node, iteration) =>
              joinLocals(g_node, data.get(g_node), potentialNeighborIndex, location, iteration, toSend, nodeLocator)
            }

          case PotentialNeighbor(g_node, potentialNeighbor, distance, iteration) =>
            handlePotentialNeighbor(g_node, potentialNeighbor, distance, iteration, graph, toSend, nodeLocator)

          case AddReverseNeighbor(g_nodeIndex, newNeighbor, iteration) =>
            // if new and not already a neighbor, introduce to all current neighbors
            graph.addReverseNeighbor(g_nodeIndex, newNeighbor)
            if (!graph.getNeighbors(g_nodeIndex).contains(newNeighbor)) {
              joinNode(newNeighbor, graph.getNeighbors(g_nodeIndex), graph.getReversedNeighbors(g_nodeIndex), iteration, toSend, nodeLocator)
            }

          case RemoveReverseNeighbor(g_nodeIndex, neighborIndex) =>
            graph.removeReverseNeighbor(g_nodeIndex, neighborIndex)
        }
        sender ! GetNNDescentInfo(ctx.self)
        sendChangesImmediately(toSend)
        nnDescent(nodeLocator, graph, toSend, mightBeDone - sender, saidImDone = false)

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
                              toSend: Map[ActorRef[BuildKNNGEvent], NNDInfo],
                              nodeLocator: NodeLocator[BuildKNNGEvent]): Unit = {
    val isNeighbor = graph.insert(g_node, potentialNeighbor, distance, iteration, toSend, nodeLocator)
    if (isNeighbor) {
      joinNode(potentialNeighbor, graph.getNeighbors(g_node), graph.getReversedNeighbors(g_node), iteration, toSend, nodeLocator)
//=======
//                              nodeLocator: NodeLocator[BuildKNNGEvent]): Map[Int, Seq[NeighborWithDist]] = {
//    val currentNeighbors = graph(g_node)
//    val currentReverseNeighbors = reverseNeighbors(g_node)
//    val currentMaxDist = currentNeighbors.last.distance
//    val isNew: Boolean = !currentNeighbors.exists(neighbor => neighbor.index == potentialNeighbor.index)
//    if (currentMaxDist > potentialNeighbor.distance && potentialNeighbor.index != g_node && isNew) {
//      if (!currentReverseNeighbors.map(_.index).contains(potentialNeighbor.index)) {
//        joinNewNeighbor(currentNeighbors.slice(0, settings.k-1).map(n => Neighbor(n.index, n.iteration)), currentReverseNeighbors, Neighbor(potentialNeighbor.index, potentialNeighbor.iteration), toSend, nodeLocator)
//      }
//      val removedNeighbor = currentNeighbors.last.index
//      val responsibleActor = nodeLocator.findResponsibleActor(removedNeighbor)
//      toSend(responsibleActor).addMessage(RemoveReverseNeighbor(removedNeighbor, g_node))
//      val position = currentNeighbors.indexWhere { oldNeighbor => oldNeighbor.distance > potentialNeighbor.distance}
//      val updatedNeighbors = (currentNeighbors.slice(0, position) :+ potentialNeighbor) ++ currentNeighbors.slice(position, settings.k - 1)
//      graph + (g_node -> updatedNeighbors)
//    } else {
//      graph
//>>>>>>> removeCache
    }
  }

  def sendForLocation(nodeLocator: NodeLocator[BuildKNNGEvent], remoteIndex: Int, waitingNode: (Int, Int), toSend: Map[ActorRef[BuildKNNGEvent], NNDInfo]): Unit = {
    val sendForLocation = waitingOnLocation.insert(remoteIndex, waitingNode)
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



