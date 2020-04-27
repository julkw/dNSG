package com.github.julkw.dnsg
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.cluster.typed.{Cluster, Join}
import com.github.julkw.dnsg.actors.Coordinator
import com.typesafe.config.ConfigFactory

object Main {

  object RootBehavior {
    def apply(): Behavior[Nothing] = Behaviors.setup[Nothing] { context =>
      context.log.info("Started up dNSG app")
      // Create an actor that handles cluster domain events
      //context.spawn(ClusterListener(), "ClusterListener")
      context.spawn(Coordinator(), name="Coordinator")
      Behaviors.empty
    }
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()

    // Create an Akka system
    val system = ActorSystem[Nothing](RootBehavior(), "dNSGSystem", config)
    val cluster = Cluster(system)
    cluster.manager ! Join(cluster.selfMember.address)
    // TODO add Reaper
  }




}