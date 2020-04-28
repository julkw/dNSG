package com.github.julkw.dnsg
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior, SupervisorStrategy}
import akka.cluster.typed.{Cluster, ClusterSingleton, Join, SingletonActor}
import com.github.julkw.dnsg.actors.{ClusterCoordinator, NodeCoordinator}
import com.typesafe.config.ConfigFactory

object Main {

  object RootBehavior {
    def apply(): Behavior[Nothing] = Behaviors.setup[Nothing] { context =>
      context.log.info("Started up dNSG app")
      // TODO get filename from command line
      context.spawn(NodeCoordinator(None), name="Coordinator")
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