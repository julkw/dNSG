package com.github.julkw.dnsg
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}
import com.github.julkw.dnsg.actors.Coordinators.NodeCoordinator
import com.typesafe.config.ConfigFactory

object Main {

  object RootBehavior {
    def apply(): Behavior[Nothing] = Behaviors.setup[Nothing] { context =>
      context.log.info("Started up dNSG app")
      context.spawn(NodeCoordinator(), name="NodeCoordinator")
      Behaviors.empty
    }
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    // Create an Akka system
    val system = ActorSystem[Nothing](RootBehavior(), "dNSG-system", config)
  }
}