package com.github.julkw.dnsg.actors.createNSG

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.{CoordinationEvent, InitialNSGDone}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.{PartialNSG, SearchOnGraphEvent}
import com.github.julkw.dnsg.actors.createNSG.NSGMerger.MergeNSGEvent
import com.github.julkw.dnsg.util.{NodeLocator, dNSGSerializable}

import scala.language.postfixOps

object NSGMerger {

  sealed trait MergeNSGEvent extends dNSGSerializable

  final case class ReverseNeighbors(nodeIndex: Int, reverseNeighbors: Seq[Int]) extends MergeNSGEvent

  final case class GetNeighbors(sender: ActorRef[MergeNSGEvent]) extends MergeNSGEvent

  // TODO on big graphs this somehow leads to sendqueue overflow. Figure out why
  final case class AddNeighbors(edges: Seq[(Int, Int)], moreToSend: Boolean, sender: ActorRef[MergeNSGEvent]) extends MergeNSGEvent

  final case object LocalNSGDone extends MergeNSGEvent

  final case class GetPartialNSG(nodes: Set[Int], sender: ActorRef[SearchOnGraphEvent]) extends MergeNSGEvent

  final case object NSGDistributed extends MergeNSGEvent

  val nsgMergerServiceKey: ServiceKey[NSGMerger.MergeNSGEvent] = ServiceKey[MergeNSGEvent]("nsgMergerService")

  private case class ListingResponse(listing: Receptionist.Listing) extends MergeNSGEvent

  def apply(supervisor: ActorRef[CoordinationEvent],
            responsibility: Seq[Int],
            nodesExpected: Int,
            maxMessageSize: Int,
            nodeLocator: NodeLocator[SearchOnGraphEvent]): Behavior[MergeNSGEvent] = Behaviors.setup { ctx =>
    val listingResponseAdapter = ctx.messageAdapter[Receptionist.Listing](ListingResponse)
    new NSGMerger(supervisor, nodesExpected, maxMessageSize / 2, nodeLocator, ctx).setup(responsibility, listingResponseAdapter)
  }
}

class NSGMerger(supervisor: ActorRef[CoordinationEvent],
                nodesExpected: Int,
                maxMessageSize: Int,
                nodeLocator: NodeLocator[SearchOnGraphEvent],
                ctx: ActorContext[MergeNSGEvent]) {
  import NSGMerger._

  def setup(responsibility: Seq[Int],
            listingAdapter: ActorRef[Receptionist.Listing]): Behavior[MergeNSGEvent] = {
    ctx.system.receptionist ! Receptionist.Register(nsgMergerServiceKey, ctx.self)
    ctx.system.receptionist ! Receptionist.Subscribe(nsgMergerServiceKey, listingAdapter)

    val nsg = responsibility.map(index => index -> Set.empty[Int]).toMap
    waitForRegistrations(nsg, Set.empty)
  }

  // TODO: The graph is a set here because for some reason edges are added multiple times
  def waitForRegistrations(graph: Map[Int, Set[Int]], mergers: Set[ActorRef[MergeNSGEvent]]): Behavior[MergeNSGEvent] =
    Behaviors.receiveMessagePartial {
      case ListingResponse(nsgMergerServiceKey.Listing(listings)) =>
        if (listings.size == nodesExpected) {
          val toSend = listings.map(merger => merger -> (Seq.empty, true)).toMap
          buildGraph(graph, graph.size, nodesExpected, toSend, listings)
        } else {
          waitForRegistrations(graph, listings)
        }

      case ReverseNeighbors(nodeIndex, reverseNeighbors) =>
        ctx.self ! ReverseNeighbors(nodeIndex, reverseNeighbors)
        waitForRegistrations(graph, mergers)

      case AddNeighbors(edges, moreToSend, sender) =>
        ctx.self ! AddNeighbors(edges, moreToSend, sender)
        waitForRegistrations(graph, mergers)
    }

  def buildGraph(graph: Map[Int, Set[Int]],
                 waitingOnReverseNeighbors: Int,
                 waitingOnMergers: Int,
                 toSend: Map[ActorRef[MergeNSGEvent], (Seq[(Int, Int)], Boolean)],
                 mergers: Set[ActorRef[MergeNSGEvent]]): Behavior[MergeNSGEvent] = Behaviors.receiveMessagePartial {
    case ListingResponse(nsgMergerServiceKey.Listing(listings)) =>
      // this shouldn't happen here
      buildGraph(graph, waitingOnReverseNeighbors, waitingOnMergers, toSend, listings)

    case ReverseNeighbors(nodeIndex, reverseNeighbors) =>
      // ctx.log.info("Still waiting for the reverse neighbors for {} nodes", waitingOnNSGWorkers - 1)
      if (waitingOnReverseNeighbors == 0) {
        ctx.log.info("Somehow getting too many reverse neighbors")
      }
      val updatedMessages = reverseNeighbors.groupBy { neighborIndex =>
        val node = nodeLocator.findResponsibleActor(neighborIndex).path.parent
        mergers.find(merger => merger.path.parent == node).get
      }.transform { (responsibleMerger, neighbors) =>
        val newEdges = neighbors.map(neighborIndex => (neighborIndex, nodeIndex))
        (toSend(responsibleMerger)._1 ++ newEdges, toSend(responsibleMerger)._2)
      }
      val updatedToSend = toSend ++ updatedMessages
      sendImmediately(updatedToSend, waitingOnReverseNeighbors <= 1)
      if (waitingOnMergers == 0 && waitingOnReverseNeighbors == 1) {
        supervisor ! InitialNSGDone(ctx.self)
      }
      buildGraph(graph, waitingOnReverseNeighbors - 1, waitingOnMergers, updatedToSend, mergers)

    case GetNeighbors(sender) =>
      val newSendInfo = sendNeighbors(toSend(sender)._1, waitingOnReverseNeighbors == 0, sender)
      buildGraph(graph, waitingOnReverseNeighbors, waitingOnMergers, toSend + (sender -> newSendInfo), mergers)

    case AddNeighbors(edges, moreToSend, sender) =>
      val updatedGraph = addEdgesToGraph(graph, edges)
      if (moreToSend) {
        sender ! GetNeighbors(ctx.self)
        buildGraph(updatedGraph, waitingOnReverseNeighbors, waitingOnMergers, toSend, mergers)
      } else {
        if (waitingOnMergers == 1 && waitingOnReverseNeighbors == 0) {
          //ctx.log.info("Local NSGMerger is done after receiving last message from other Merger")
          supervisor ! InitialNSGDone(ctx.self)
        }
        buildGraph(updatedGraph, waitingOnReverseNeighbors, waitingOnMergers - 1, toSend, mergers)
      }

    case GetPartialNSG(nodes, sender) =>
      // should only get this message after all NSGMergers told the NodeCoordinator that they are done
      ctx.self ! GetPartialNSG(nodes, sender)
      distributeGraph(graph)
  }

  def distributeGraph(graph: Map[Int, Set[Int]]): Behavior[MergeNSGEvent] = Behaviors.receiveMessagePartial {
    case GetPartialNSG(nodes, sender) =>
      val partialGraph = graph.filter { case(node, _) =>
        nodes.contains(node)
      }.transform((_, neighbors) => neighbors.toSeq)
      // can be send as a whole because it is only send to other actors on the same node
      sender ! PartialNSG(partialGraph)
      distributeGraph(graph)

    case GetNeighbors(sender) =>
      // this can happen if I told another Merger that I had more to send them, because I had not yet gotten results for all my local nodes
      // if those nodes didn't have anything for this Merger, however, I switched states without telling them there was nothing to get, so they kept asking
      distributeGraph(graph)

    case NSGDistributed =>
      Behaviors.empty
  }

  def addEdgesToGraph(graph: Map[Int, Set[Int]], edges: Seq[(Int, Int)]): Map[Int, Set[Int]] = {
    val updatedNeighbors = edges.groupBy(_._1).transform { (nodeIndex, newEdges) =>
      val newNeighbors = newEdges.map(_._2)
      graph(nodeIndex) ++ newNeighbors
    }
    graph ++ updatedNeighbors
  }

  def sendNeighbors(messagesToSend: Seq[(Int, Int)], receivedAllLocalMessages: Boolean, sendTo: ActorRef[MergeNSGEvent]): (Seq[(Int, Int)], Boolean) = {
    if (messagesToSend.nonEmpty || receivedAllLocalMessages) {
      val sendNow = messagesToSend.slice(0, maxMessageSize)
      val lastMessage = receivedAllLocalMessages && sendNow.length == messagesToSend.length
      sendTo ! AddNeighbors(sendNow, !lastMessage, ctx.self)
      val sendLater = messagesToSend.slice(maxMessageSize, messagesToSend.length)
      (sendLater, sendNow.isEmpty && !receivedAllLocalMessages)
    } else {
      (messagesToSend, true)
    }
  }

  def sendImmediately(toSend: Map[ActorRef[MergeNSGEvent], (Seq[(Int, Int)], Boolean)], receivedAllLocalMessages: Boolean): Map[ActorRef[MergeNSGEvent], (Seq[(Int, Int)], Boolean)] = {
    toSend.transform { (merger, sendInfo) =>
      if (sendInfo._2) {
        sendNeighbors(sendInfo._1, receivedAllLocalMessages, merger)
      } else {
        sendInfo
      }
    }
  }
}



