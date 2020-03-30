package com.github.julkw.dnsg.actors.createNSG

import math._
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.julkw.dnsg.actors.Coordinator.{CoordinationEvent}

import scala.language.postfixOps


object NSGWorker {

  sealed trait BuildNSGEvent

  // setup
  // TODO get responsible nodes -> from SearchOnGraph or Coordinator? Already on spawning?
  // TODO also send NodeLocator
  final case class StartOnNSG(navigatingNode: Seq[Float]) extends BuildNSGEvent

  // build NSG


  def apply(supervisor: ActorRef[CoordinationEvent]): Behavior[BuildNSGEvent] = Behaviors.setup { ctx =>
    ctx.log.info("Started NSGWorker")
    Behaviors.setup(ctx => new NSGWorker(supervisor, ctx).setup())
  }
}

class NSGWorker(supervisor: ActorRef[CoordinationEvent],
                ctx: ActorContext[NSGWorker.BuildNSGEvent]) {
  import NSGWorker._

  def setup(): Behavior[BuildNSGEvent] =
    Behaviors.receiveMessagePartial{
      case _ =>
        Behaviors.same
    }

  def buildNSG(knng: Map[Int, Seq[Int]]): Behavior[BuildNSGEvent] =
    Behaviors.receiveMessagePartial{
      case StartOnNSG(navigatingNode) =>

      buildNSG(knng)
    }

  // TODO move to util
  def euclideanDist(pointX: Seq[Float], pointY: Seq[Float]): Double = {
    sqrt((pointX zip pointY).map { case (x,y) => pow(y - x, 2) }.sum)
  }
}



