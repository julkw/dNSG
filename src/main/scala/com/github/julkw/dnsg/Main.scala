package com.github.julkw.dnsg
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
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
    // Remnants from copied example
    val ports = Seq(25251)

    ports.foreach(startup)
  }

  def startup(port: Int): Unit = {
    // Override the configuration of the port
    val config = ConfigFactory.parseString(s"""
      akka.remote.artery.canonical.port=$port
      """).withFallback(ConfigFactory.load())

    // Create an Akka system
    val system = ActorSystem[Nothing](RootBehavior(), "ClusterSystem", config)
    // TODO add Reaper
  }



}