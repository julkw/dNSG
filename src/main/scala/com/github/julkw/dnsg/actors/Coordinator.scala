package com.github.julkw.dnsg.actors

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.github.julkw.dnsg.actors.DataHolder.LoadSiftDataFromFile

object Coordinator {

  sealed trait CoordinationEvent
  final case class DataRef(dataRef: Seq[Seq[Float]]) extends CoordinationEvent

  var data : Seq[Seq[Float]] = Seq.empty

  def apply(): Behavior[CoordinationEvent] = {
    val filename: String = "/home/juliane/code/dNSG/data/siftsmall/siftsmall_base.fvecs"
    startUp(filename)
  }


  def startUp(filename: String): Behavior[CoordinationEvent] = Behaviors.setup { ctx =>
    val dh = ctx.spawn(DataHolder(), name = "DataHolder")
    // TODO specify file and type through configurations
    dh ! LoadSiftDataFromFile(filename, ctx.self)

    Behaviors.receive { (ctx, message) =>
      message match {
        case DataRef(dataRef) =>
          data = dataRef
          ctx.log.info("Successfully loaded data")

          // TODO spawn NNDescent workers and distribute data over them (kd-tree)
      }
      Behaviors.same
    }
  }

}
