package com.github.julkw.dnsg.actors.createNSG

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.{CoordinationEvent, InitialNSGDone}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.{PartialNSG, SearchOnGraphEvent}
import com.github.julkw.dnsg.actors.createNSG.NSGMerger.MergeNSGEvent
import com.github.julkw.dnsg.util.{NodeLocator, dNSGSerializable}

import scala.collection.mutable
import scala.language.postfixOps


object NSGMerger {

  sealed trait MergeNSGEvent extends dNSGSerializable

  final case class ReverseNeighbors(nodeIndex: Int, reverseNeighbors: Seq[Int]) extends MergeNSGEvent

  final case class AddNeighbor(nodeIndex: Int, neighborIndex: Int, sendAckTo: ActorRef[MergeNSGEvent]) extends MergeNSGEvent

  final case object NeighborReceived extends MergeNSGEvent

  final case object NSGToSearchOnGraph extends MergeNSGEvent

  final case object LocalNSGDone extends MergeNSGEvent

  final case class GetPartialGraph(nodes: Set[Int], sender: ActorRef[SearchOnGraphEvent]) extends MergeNSGEvent

  final case object NSGDistributed extends MergeNSGEvent

  val nsgMergerServiceKey: ServiceKey[NSGMerger.MergeNSGEvent] = ServiceKey[MergeNSGEvent]("nsgMergerService")

  private case class ListingResponse(listing: Receptionist.Listing) extends MergeNSGEvent

  def apply(supervisor: ActorRef[CoordinationEvent], responsibility: Seq[Int], nodesExpected: Int, nodeLocator: NodeLocator[ActorRef[SearchOnGraphEvent]]): Behavior[MergeNSGEvent] = Behaviors.setup { ctx =>
    ctx.log.info("Started NSGMerger")
    val listingResponseAdapter = ctx.messageAdapter[Receptionist.Listing](ListingResponse)
    Behaviors.setup { ctx =>
      Behaviors.withTimers {timers =>
        new NSGMerger(supervisor, nodesExpected, nodeLocator, timers, ctx).setup(responsibility, listingResponseAdapter)}
    }
  }
}

class NSGMerger(supervisor: ActorRef[CoordinationEvent],
                nodesExpected: Int,
                nodeLocator: NodeLocator[ActorRef[SearchOnGraphEvent]],
                timers: TimerScheduler[MergeNSGEvent],
                ctx: ActorContext[MergeNSGEvent]) {
  import NSGMerger._

  def setup(responsibility: Seq[Int],
            listingAdapter: ActorRef[Receptionist.Listing]): Behavior[MergeNSGEvent] = {
    ctx.system.receptionist ! Receptionist.Register(nsgMergerServiceKey, ctx.self)
    ctx.system.receptionist ! Receptionist.Subscribe(nsgMergerServiceKey, listingAdapter)

    val nsg: mutable.Map[Int, Seq[Int]] = mutable.Map.empty
    responsibility.foreach(index => nsg += (index -> Seq.empty[Int]))
    waitForRegistrations(nsg, Set.empty)
  }

  def waitForRegistrations(graph: mutable.Map[Int, Seq[Int]], mergers: Set[ActorRef[MergeNSGEvent]]): Behavior[MergeNSGEvent] =
    Behaviors.receiveMessagePartial {
      case ListingResponse(nsgMergerServiceKey.Listing(listings)) =>
        if (listings.size >= nodesExpected) {
          buildGraph(graph, messagesReceived=0, graph.size, awaitingNeighborAcks=0, listings)
        } else {
          waitForRegistrations(graph, listings)
        }

      case ReverseNeighbors(nodeIndex, reverseNeighbors) =>
        // TODO is not the best solution
        ctx.self ! ReverseNeighbors(nodeIndex, reverseNeighbors)
        waitForRegistrations(graph, mergers)

      case AddNeighbor(nodeIndex, neighborIndex, sendAckTo) =>
        graph(nodeIndex) = graph(nodeIndex) :+ neighborIndex
        sendAckTo ! NeighborReceived
        waitForRegistrations(graph, mergers)
    }

  // TODO change messagesReceived and Expected to waitingOnReverseNeighbors or something similar
  def buildGraph(graph: mutable.Map[Int, Seq[Int]],
                 messagesReceived: Int,
                 messagesExpected: Int,
                 awaitingNeighborAcks: Int,
                 mergers: Set[ActorRef[MergeNSGEvent]]): Behavior[MergeNSGEvent] = Behaviors.receiveMessagePartial {
    case ListingResponse(nsgMergerServiceKey.Listing(listings)) =>
      // this shouldn't happen here
      buildGraph(graph, messagesReceived, messagesExpected, awaitingNeighborAcks, listings)

    case ReverseNeighbors(nodeIndex, reverseNeighbors) =>
      var awaitingAcks = 0
      var extraMessagesExpected = 0
      reverseNeighbors.foreach { neighborIndex =>
        val node = nodeLocator.findResponsibleActor(neighborIndex).path.parent
        val responsibleMerger = mergers.find(merger => merger.path.parent == node)
        responsibleMerger match {
          case Some(merger) =>
            awaitingAcks += 1
            merger ! AddNeighbor(neighborIndex, nodeIndex, ctx.self)
          case None =>
            // this should not happen
            ctx.log.info("Could not find the correct merger for node.")
            extraMessagesExpected += 1
            ctx.self ! ReverseNeighbors(nodeIndex, Seq(neighborIndex))
        }
      }
      val updatedMessagesExpected = messagesExpected + extraMessagesExpected
      val updatedNeighborAcks = awaitingNeighborAcks + awaitingAcks
      if (messagesReceived + 1 == updatedMessagesExpected && updatedNeighborAcks == 0) {
        ctx.log.info("Local NSGWorkers are done")
        supervisor ! InitialNSGDone(ctx.self)
      }
      buildGraph(graph, messagesReceived + 1, updatedMessagesExpected, updatedNeighborAcks, mergers)

    case AddNeighbor(nodeIndex, neighborIndex, sendAckTo) =>
      graph(nodeIndex) = graph(nodeIndex) :+ neighborIndex
      sendAckTo ! NeighborReceived
      buildGraph(graph, messagesReceived, messagesExpected, awaitingNeighborAcks, mergers)

    case NeighborReceived =>
      if (messagesReceived == messagesExpected && awaitingNeighborAcks == 1) {
        ctx.log.info("Local NSGWorkers are done")
        supervisor ! InitialNSGDone(ctx.self)
      }
      buildGraph(graph, messagesReceived, messagesExpected, awaitingNeighborAcks - 1, mergers)

    case NSGToSearchOnGraph =>
      distributeGraph(graph.toMap)

    case GetPartialGraph(nodes, sender) =>
      // should only get this message after all NSGMergers told the NodeCoordinator that they are done
      ctx.log.info("Asked for graph before being told by the ClusterCoordinator that the NSG is done. Assuming it is and switching states")
      ctx.self ! GetPartialGraph(nodes, sender)
      distributeGraph(graph.toMap)
  }

  def distributeGraph(graph: Map[Int, Seq[Int]]): Behavior[MergeNSGEvent] = Behaviors.receiveMessagePartial {
    case NSGToSearchOnGraph =>
      // If I switch when I get GetPartialGraph, this message should follow soon after
      distributeGraph(graph)

    case GetPartialGraph(nodes, sender) =>
      val partialGraph = graph.filter{case(node, _) =>
        nodes.contains(node)
      }
      sender ! PartialNSG(partialGraph)
      distributeGraph(graph)

    case NSGDistributed =>
      Behaviors.empty
  }


}



