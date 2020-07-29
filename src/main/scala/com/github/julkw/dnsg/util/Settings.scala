package com.github.julkw.dnsg.util

import akka.actor.typed.scaladsl.ActorContext
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator
import com.typesafe.config.Config

case class Settings(config: Config) {

  private val namespace = "com.github.julkw.dnsg"

  val filename: String = config.getString(s"$namespace.input-file")

  val aknngFilePath: String = config.getString(s"$namespace.aknng-file")

  val nsgFilePath: String = config.getString(s"$namespace.nsg-file")

  val k: Int = config.getInt(s"$namespace.k")

  val preNNDescentK: Int = config.getInt(s"$namespace.pre-nndescent-k")

  val maxReverseNeighborsNSG: Int= config.getInt(s"$namespace.max-reverse-neighbors-nsg")

  val maxReverseNeighborsNND: Int= config.getInt(s"$namespace.max-reverse-neighbors-nndescent")

  val maxNeighborCandidates: Int= config.getInt(s"$namespace.max-neighbor-candidates")

  val candidateQueueSizeKnng: Int = config.getInt(s"$namespace.candidate-queue-size-knng")

  val candidateQueueSizeNSG: Int = config.getInt(s"$namespace.candidate-queue-size-nsg")

  val workers: Int = config.getInt(s"$namespace.workers")

  val nodesExpected: Int = config.getInt(s"$namespace.nodes-expected")

  val dataRedistribution: String = config.getString(s"$namespace.data-redistribution")

  val dataReplication: String = config.getString(s"$namespace.data-replication")

  // this is defined over the number of ints/floats that can be send in one message. It does not take into account any Boxing overhead though
  val maxMessageSize: Int = config.getInt(s"$namespace.max-message-size")

  // for testing
  val queryFilePath: String = config.getString(s"$namespace.query-testing.query-file")

  val queryResultFilePath: String = config.getString(s"$namespace.query-testing.query-result-file")

  val lines: Int = config.getInt(s"$namespace.query-testing.lines")

  val candidateQueueIncreaseBy: Int = config.getInt(s"$namespace.query-testing.candidate-queue-increase-by")

  val candidateQueueIncreaseTimes: Int = config.getInt(s"$namespace.query-testing.candidate-queue-increases")

  def printSettings(ctx: ActorContext[ClusterCoordinator.CoordinationEvent]): Unit = {
    ctx.log.info("k: {}", k)
    ctx.log.info("workers: {}", workers)
    ctx.log.info("nodes: {}", nodesExpected)
    ctx.log.info("maxReverseNeighbors: {}", maxReverseNeighborsNSG)
    ctx.log.info("lines: {}", lines)
    ctx.log.info("maxMessageSize: {}", maxMessageSize)
    ctx.log.info("data redistribution option: {}", dataRedistribution)
    ctx.log.info("data replication strategy: {}", dataReplication)
  }
}
