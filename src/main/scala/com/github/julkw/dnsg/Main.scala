package com.github.julkw.dnsg
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}
import akka.cluster.typed.{Cluster, Join}
import com.github.julkw.dnsg.actors.NodeCoordinator
import com.typesafe.config.ConfigFactory

object Main {

  object RootBehavior {
    def apply(filename: Option[String]): Behavior[Nothing] = Behaviors.setup[Nothing] { context =>
      context.log.info("Started up dNSG app")
      context.spawn(NodeCoordinator(filename), name="Coordinator")
      Behaviors.empty
    }
  }

  def main(args: Array[String]): Unit = {
    // TODO get filename and port from command line
    val config = ConfigFactory.load()
    val filename = "data/siftsmall/siftsmall_base.fvecs"
    // Create an Akka system
    val system = ActorSystem[Nothing](RootBehavior(Some(filename)), "dNSGSystem", config)
    val cluster = Cluster(system)
    cluster.manager ! Join(cluster.selfMember.address)
    // TODO add Reaper
  }




}