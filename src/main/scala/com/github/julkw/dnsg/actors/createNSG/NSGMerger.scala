package com.github.julkw.dnsg.actors.createNSG

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.julkw.dnsg.actors.Coordinator.{CoordinationEvent, InitialNSGDone}
import com.github.julkw.dnsg.actors.SearchOnGraph.{PartialNSG, SearchOnGraphEvent}
import com.github.julkw.dnsg.actors.createNSG.NSGMerger.MergeNSGEvent

import scala.collection.mutable
import scala.language.postfixOps


object NSGMerger {

  sealed trait MergeNSGEvent

  final case class ReverseNeighbors(nodeIndex: Int, reverseNeighbors: Seq[Int]) extends MergeNSGEvent

  final case class GetPartialGraph(nodes: Set[Int], sender: ActorRef[SearchOnGraphEvent]) extends MergeNSGEvent

  final case object NSGDistributed extends MergeNSGEvent

  def apply(supervisor: ActorRef[CoordinationEvent], responsibility: Seq[Int]): Behavior[MergeNSGEvent] = Behaviors.setup { ctx =>
    ctx.log.info("Started NSGMerger")
    Behaviors.setup(ctx => new NSGMerger(supervisor, ctx).setup(responsibility))
  }
}

class NSGMerger(supervisor: ActorRef[CoordinationEvent],
                ctx: ActorContext[MergeNSGEvent]) {
  import NSGMerger._

  def setup(responsibility: Seq[Int]): Behavior[MergeNSGEvent] = {
    val nsg: mutable.Map[Int, Seq[Int]] = mutable.Map.empty
    responsibility.foreach(index => nsg += (index -> Seq.empty[Int]))
    buildGraph(nsg, messagesReceived = 0, responsibility.length)
  }

  def buildGraph(graph: mutable.Map[Int, Seq[Int]],
                  messagesReceived: Int,
                  messagesExpected: Int): Behavior[MergeNSGEvent] = Behaviors.receiveMessagePartial {
    case ReverseNeighbors(nodeIndex, reverseNeighbors) =>
      reverseNeighbors.foreach{ neighborIndex =>
        graph(neighborIndex) = graph(neighborIndex) :+ nodeIndex
      }
      if (messagesReceived + 1 == messagesExpected) {
        ctx.log.info("Building of NSG seems to be done")
        supervisor ! InitialNSGDone(ctx.self)
        distributeGraph(graph.toMap)
      } else {
        buildGraph(graph, messagesReceived + 1, messagesExpected)
      }
  }

  def distributeGraph(graph: Map[Int, Seq[Int]]): Behavior[MergeNSGEvent] = Behaviors.receiveMessagePartial {
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



