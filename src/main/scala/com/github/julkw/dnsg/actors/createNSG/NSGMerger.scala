package com.github.julkw.dnsg.actors.createNSG

import math._
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.julkw.dnsg.actors.Coordinator.CoordinationEvent
import com.github.julkw.dnsg.actors.createNSG.NSGMerger.MergeNSGEvent

import scala.collection.mutable
import scala.language.postfixOps


object NSGMerger {

  sealed trait MergeNSGEvent

  final case class ReverseNeighbors(nodeIndex: Int, reverseNeighbors: Seq[Int]) extends MergeNSGEvent

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
    updateGraph(nsg, messagesReceived = 0, responsibility.length)
  }

  def updateGraph(graph: mutable.Map[Int, Seq[Int]],
                  messagesReceived: Int,
                  messagesExpected: Int): Behavior[MergeNSGEvent] = Behaviors.receiveMessagePartial {
    case ReverseNeighbors(nodeIndex, reverseNeighbors) =>
      reverseNeighbors.foreach{ neighborIndex =>
        graph(neighborIndex) = graph(neighborIndex) :+ nodeIndex
      }
      if (messagesReceived + 1 == messagesExpected) {
        ctx.log.info("Building of NSG seems to be done")
        // TODO tell supervisor
      }
      updateGraph(graph, messagesReceived + 1, messagesExpected)
  }

}



