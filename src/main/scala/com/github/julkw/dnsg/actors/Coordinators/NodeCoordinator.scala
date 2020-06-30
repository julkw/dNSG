package com.github.julkw.dnsg.actors.Coordinators

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import akka.cluster.typed.{ClusterSingleton, SingletonActor}
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.{CoordinationEvent, NodeIntroduction}
import com.github.julkw.dnsg.actors.{DataHolder, NodeLocatorHolder}
import com.github.julkw.dnsg.actors.DataHolder.{LoadDataEvent, LoadDataFromFile}
import com.github.julkw.dnsg.actors.NodeLocatorHolder.NodeLocationEvent
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.{GetNSGFrom, InitializeGraph, SearchOnGraphEvent, UpdatedLocalData}
import com.github.julkw.dnsg.actors.createNSG.{NSGMerger, NSGWorker}
import com.github.julkw.dnsg.actors.createNSG.NSGMerger.MergeNSGEvent
import com.github.julkw.dnsg.actors.createNSG.NSGWorker.Responsibility
import com.github.julkw.dnsg.actors.nndescent.KnngWorker
import com.github.julkw.dnsg.actors.nndescent.KnngWorker.{BuildKNNGEvent, MoveGraph}
import com.github.julkw.dnsg.util.Data.LocalData
import com.github.julkw.dnsg.util.{Distance, NodeLocator, Settings, dNSGSerializable}

object NodeCoordinator {

  sealed trait NodeCoordinationEvent extends dNSGSerializable

  final case class StartDistributingData(dataHolders: Set[ActorRef[LoadDataEvent]]) extends NodeCoordinationEvent

  final case class DataRef(dataRef: LocalData[Float], graphSize: Int) extends NodeCoordinationEvent

  final case class StartImprovingGraph(nodeLocator: NodeLocator[SearchOnGraphEvent]) extends NodeCoordinationEvent

  final case object StartSearchOnGraph extends NodeCoordinationEvent

  final case class StartBuildingNSG(navigatingNode: Int, nodeLocator: NodeLocator[SearchOnGraphEvent]) extends NodeCoordinationEvent

  final case class LocalKnngWorker(worker: ActorRef[BuildKNNGEvent]) extends NodeCoordinationEvent

  final case object AllDone extends NodeCoordinationEvent

  def apply(): Behavior[NodeCoordinationEvent] = Behaviors.setup { ctx =>
    val settings = Settings(ctx.system.settings.config)

    // get access to cluster coordinator
    val singletonManager = ClusterSingleton(ctx.system)
    val clusterCoordinator: ActorRef[ClusterCoordinator.CoordinationEvent] = singletonManager.init(
      SingletonActor(Behaviors.supervise(ClusterCoordinator()).onFailure[Exception](SupervisorStrategy.restart), "ClusterCoordinator"))

    val dh = ctx.spawn(DataHolder(ctx.self), name = "DataHolder")
    val nl = ctx.spawn(NodeLocatorHolder(clusterCoordinator, ctx.self, dh, settings.maxMessageSize), name = "NodeLocatorHolder")
    clusterCoordinator ! NodeIntroduction(ctx.self, dh, nl)
    if (settings.filename.nonEmpty) {
      ctx.log.info("Load data from {}", settings.filename)
      new NodeCoordinator(settings, dh, nl, clusterCoordinator, ctx).setUp(settings.filename)
    } else {
      new NodeCoordinator(settings, dh, nl, clusterCoordinator, ctx).waitForData()
    }
  }
}

class NodeCoordinator(settings: Settings,
                      dataHolder: ActorRef[LoadDataEvent],
                      nodeLocatorHolder: ActorRef[NodeLocationEvent],
                      clusterCoordinator: ActorRef[CoordinationEvent],
                      ctx: ActorContext[NodeCoordinator.NodeCoordinationEvent]) extends Distance {
  import NodeCoordinator._

  def setUp(filename: String): Behavior[NodeCoordinationEvent] = Behaviors.receiveMessagePartial {
    case StartDistributingData(dataHolders) =>
      dataHolder ! LoadDataFromFile(filename, settings.lines, dataHolders, clusterCoordinator)
      waitForData()
  }

  def waitForData(): Behavior[NodeCoordinationEvent] = Behaviors.receiveMessagePartial {
    case StartDistributingData(_) =>
      // this node doesn't have the file and so can do nothing but wait
      waitForData()

    case DataRef(dataRef, graphSize) =>
      val data = dataRef
      assert(data.localDataSize > 0)
      ctx.log.info("Successfully loaded data of size: {}", data.localDataSize)
      // distribute data to SearchOnGraphActors
      val responsibilities = data.localIndices.grouped(data.localDataSize / settings.workers)
      val localGraphHolders = responsibilities.zipWithIndex.map { case (responsibility, index) =>
        val sog = ctx.spawn(SearchOnGraphActor(clusterCoordinator, nodeLocatorHolder), name = "SearchOnGraphActor" + index.toString)
        sog ! InitializeGraph(responsibility, graphSize, data)
        sog
      }.toSet
      waitForNavigatingNode(data, localGraphHolders)
  }

  def waitForNavigatingNode(data: LocalData[Float], localGraphHolders: Set[ActorRef[SearchOnGraphEvent]]): Behavior[NodeCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      // In case of data redistribution
      case DataRef(newData, _) =>
        ctx.log.info("Received new data, forwarding to graphHolders")
        localGraphHolders.foreach(graphHolder => graphHolder ! UpdatedLocalData(newData))
        waitForNavigatingNode(newData, localGraphHolders)

      case StartBuildingNSG(navigatingNode, nodeLocator) =>
        startBuildingNSG(data, localGraphHolders, nodeLocator, navigatingNode)
    }

  def startBuildingNSG(data: LocalData[Float],
                       localGraphHolders: Set[ActorRef[SearchOnGraphEvent]],
                       nodeLocator: NodeLocator[SearchOnGraphEvent],
                       navigatingNode: Int): Behavior[NodeCoordinationEvent] = {
    val responsibilityPerGraphHolder = nodeLocator.actorsResponsibilities()
    val mergerResponsibility = localGraphHolders.flatMap(graphHolder => responsibilityPerGraphHolder(graphHolder))
    val nsgMerger = ctx.spawn(NSGMerger(clusterCoordinator, mergerResponsibility.toSeq, settings.nodesExpected, settings.maxMessageSize, nodeLocator), name = "NSGMerger")
    var index = 0
    localGraphHolders.foreach { graphHolder =>
      val nsgWorker = ctx.spawn(NSGWorker(data, navigatingNode, settings.candidateQueueSizeKnng, settings.maxReverseNeighborsNSG, nodeLocator, nsgMerger), name = "NSGWorker" + index.toString)
      index += 1
      // 1 to 1 mapping from searchOnGraphActors to NSGWorkers
      val responsibilities = responsibilityPerGraphHolder(graphHolder)
      nsgWorker ! Responsibility(responsibilities)
    }
    moveNSGToSearchOnGraph(localGraphHolders, nsgMerger)
  }

  // this happens here for mapping to the correct NSGMerger
  def moveNSGToSearchOnGraph(localGraphHolders: Set[ActorRef[SearchOnGraphEvent]],
                             nsgMerger: ActorRef[MergeNSGEvent]): Behavior[NodeCoordinationEvent] =
    Behaviors.receiveMessagePartial{
      case StartSearchOnGraph =>
        localGraphHolders.foreach(graphHolder => graphHolder ! GetNSGFrom(nsgMerger))
        waitForShutdown()
    }

  def waitForShutdown(): Behavior[NodeCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case AllDone =>
        ctx.system.terminate()
        Behaviors.stopped
    }

}
