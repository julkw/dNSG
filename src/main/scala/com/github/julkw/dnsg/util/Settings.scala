package com.github.julkw.dnsg.util

import akka.actor.typed.scaladsl.ActorContext
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator
import com.typesafe.config.Config

case class Settings(config: Config) {

  private val namespace = "com.github.julkw.dnsg"

  val inputFilePath: String = config.getString(s"$namespace.input-file")

  val k: Int = config.getInt(s"$namespace.k")

  val sampleRate: Double = config.getDouble(s"$namespace.sample-rate")

  val maxReverseNeighbors: Int= config.getInt(s"$namespace.max-reverse-neighbors")

  val workers: Int = config.getInt(s"$namespace.workers")

  val nodesExpected: Int = config.getInt(s"$namespace.nodes-expected")

  val cacheSize: Int = config.getInt(s"$namespace.cache-size")

  val dataMessageSize: Int = config.getInt(s"$namespace.data-message-size")

  val graphMessageSize: Int = config.getInt(s"$namespace.graph-message-size")

  val connectMessageSize: Int = config.getInt(s"$namespace.connect-message-size")

  // for testing
  val queryFilePath: String = config.getString(s"$namespace.query-testing.query-file")

  val linesOffset: Int = config.getInt(s"$namespace.query-testing.lines-offset")

  val lines: Int = config.getInt(s"$namespace.query-testing.lines")

  val dimensionOffset: Int = config.getInt(s"$namespace.query-testing.dimension-offset")

  val dimensions: Int = config.getInt(s"$namespace.query-testing.dimensions")

  def printSettings(ctx: ActorContext[ClusterCoordinator.CoordinationEvent]): Unit = {
    ctx.log.info("inputFile: {}", inputFilePath)
    ctx.log.info("k: {}", k)
    ctx.log.info("sampleRate: {}", sampleRate)
    ctx.log.info("workers: {}", workers)
    ctx.log.info("maxReverseNeighbors: {}", maxReverseNeighbors)
    ctx.log.info("lines: {}", lines)
  }
}
