package com.github.julkw.dnsg
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}
import akka.cluster.typed.{Cluster, Join}
import com.github.julkw.dnsg.actors.Coordinators.NodeCoordinator
import com.typesafe.config.ConfigFactory

object Main {

  object RootBehavior {
    def apply(filename: Option[String]): Behavior[Nothing] = Behaviors.setup[Nothing] { context =>
      context.log.info("Started up dNSG app")
      context.spawn(NodeCoordinator(filename), name="NodeCoordinator")
      Behaviors.empty
    }
  }

  def main(args: Array[String]): Unit = {
    val arguments = parseArgs(args)
    val port = arguments._1
    val filename = arguments._2

    val config = port match {
      case Some(inputPort) =>
        ConfigFactory.parseString(s"""
      akka.remote.artery.canonical.port=$inputPort
      """).withFallback(ConfigFactory.load())
      case None =>
        ConfigFactory.load()
    }

    // Create an Akka system
    val system = ActorSystem[Nothing](RootBehavior(filename), "dNSGSystem", config)
  }

  def parseArgs(args: Array[String]): (Option[Int], Option[String]) = {
    // TODO: maybe add seed-port over arguments as well?
    var port: Option[Int] = None
    var filename: Option[String] = None

    def nextOption(list: List[String]): Unit = {
      list match {
        case Nil =>
        case "--port" :: value :: tail =>
          port = Some(value.toInt)
          nextOption(tail)
        case "--filename" :: value :: tail =>
          filename = Some(value)
          nextOption(tail)
      }
    }

    val argList = args.toList.flatMap(arg => arg.split("="))
    nextOption(argList)
    (port, filename)
  }



}