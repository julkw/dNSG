package com.github.julkw.dnsg.actors

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import akka.cluster.typed.{ClusterSingleton, SingletonActor}
import com.github.julkw.dnsg.actors.ClusterCoordinator.{CoordinationEvent, NodeCoordinatorIntroduction}
import com.github.julkw.dnsg.actors.DataHolder.{LoadDataEvent, LoadPartialDataFromFile, WaitForStreamedData}
import com.github.julkw.dnsg.actors.SearchOnGraph.{GetNSGFrom, SearchOnGraphEvent, SendResponsibleIndicesTo}
import com.github.julkw.dnsg.actors.createNSG.NSGMerger.MergeNSGEvent
import com.github.julkw.dnsg.actors.createNSG.{NSGMerger, NSGWorker}
import com.github.julkw.dnsg.actors.nndescent.KnngWorker
import com.github.julkw.dnsg.actors.nndescent.KnngWorker.{BuildGraphEvent, MoveGraph, ResponsibleFor}
import com.github.julkw.dnsg.util.{Distance, LocalData, NodeLocator, Settings}

object NodeCoordinator {

  sealed trait NodeCoordinationEvent

  final case class DataRef(dataRef: LocalData[Float]) extends NodeCoordinationEvent

  final case object StartSearchOnGraph extends NodeCoordinationEvent

  final case class StartBuildingNSG(navigatingNode: Int, nodeLocator: NodeLocator[SearchOnGraphEvent]) extends NodeCoordinationEvent

  final case class LocalKnngWorker(worker: ActorRef[BuildGraphEvent]) extends NodeCoordinationEvent

  def apply(fileName: Option[String]): Behavior[NodeCoordinationEvent] = Behaviors.setup { ctx =>
    val settings = Settings(ctx.system.settings.config)
    settings.printSettings(ctx)

    // get access to cluster coordinator
    val singletonManager = ClusterSingleton(ctx.system)
    val clusterCoordinator: ActorRef[ClusterCoordinator.CoordinationEvent] = singletonManager.init(
      SingletonActor(Behaviors.supervise(ClusterCoordinator()).onFailure[Exception](SupervisorStrategy.restart), "ClusterCoordinator"))
    clusterCoordinator ! NodeCoordinatorIntroduction(ctx.self)

    val dh = ctx.spawn(DataHolder(), name = "DataHolder")
    fileName match {
      case Some(fName) =>
        // TODO somehow ensure that all others are already registered to receive data
        dh ! LoadPartialDataFromFile(fName, settings.linesOffset, settings.lines, settings.dimensionOffset, settings.dimensions, ctx.self, clusterCoordinator)
      case None =>
        dh ! WaitForStreamedData
    }

    ctx.log.info("start building the approximate graph")
    Behaviors.setup(
      ctx => new NodeCoordinator(settings, dh, clusterCoordinator, ctx).setUp()
    )
  }

}

class NodeCoordinator(settings: Settings,
                      dataHolder: ActorRef[LoadDataEvent],
                      clusterCoordinator: ActorRef[CoordinationEvent],
                      ctx: ActorContext[NodeCoordinator.NodeCoordinationEvent]) extends Distance {
  import NodeCoordinator._

  def setUp(): Behavior[NodeCoordinationEvent] = Behaviors.receiveMessagePartial {
    case DataRef(dataRef) =>
      val data = dataRef
      ctx.log.info("Successfully loaded data")
      // create Actor to start distribution of data
      val maxResponsibilityPerNode: Int = data.localDataSize / settings.workers + data.localDataSize / 10
      val kw = ctx.spawn(KnngWorker(data, maxResponsibilityPerNode, settings.k, settings.sampleRate, clusterCoordinator, ctx.self), name = "KnngWorker")
      kw ! ResponsibleFor(data.localIndices, 0)
      waitForKnng(Set.empty, data)
  }

  def waitForKnng(knngWorkers: Set[ActorRef[BuildGraphEvent]], data: LocalData[Float]): Behavior[NodeCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case LocalKnngWorker(worker) =>
        waitForKnng(knngWorkers + worker, data)
      case StartSearchOnGraph =>
        moveKnngToSearchOnGraph(knngWorkers, data)
    }

  def moveKnngToSearchOnGraph(knngWorkers: Set[ActorRef[BuildGraphEvent]], data: LocalData[Float]): Behavior[NodeCoordinationEvent] = {
    var sogIndex = 0
    val graphHolders = knngWorkers.map { worker =>
      val nsgw = ctx.spawn(SearchOnGraph(clusterCoordinator, data, settings.k), name = "SearchOnGraph" + sogIndex.toString)
      sogIndex += 1
      worker ! MoveGraph(nsgw)
      nsgw
    }
    waitForNavigatingNode(data, graphHolders)
  }

  def waitForNavigatingNode(data: LocalData[Float], graphHolders: Set[ActorRef[SearchOnGraphEvent]]): Behavior[NodeCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case StartBuildingNSG(navigatingNode, nodeLocator) =>
        startBuildingNSG(data, nodeLocator, graphHolders, navigatingNode)
    }


  def startBuildingNSG(data: LocalData[Float],
                       nodeLocator: NodeLocator[SearchOnGraphEvent],
                       graphHolders: Set[ActorRef[SearchOnGraphEvent]],
                       navigatingNode: Int): Behavior[NodeCoordinationEvent] = {
    val nsgMerger = ctx.spawn(NSGMerger(clusterCoordinator, data.localIndices), name = "NSGMerger")
    var index = 0
    graphHolders.foreach { graphHolder =>
      val nsgWorker = ctx.spawn(NSGWorker(clusterCoordinator, data, navigatingNode, settings.maxReverseNeighbors, nodeLocator, nsgMerger), name = "NSGWorker" + index.toString)
      index += 1
      // for now this is the easiest way to distribute responsibility
      graphHolder ! SendResponsibleIndicesTo(nsgWorker)
      nsgWorker
    }
    moveNSGToSearchOnGraph(graphHolders, nsgMerger, data)
  }

  def moveNSGToSearchOnGraph(graphHolders: Set[ActorRef[SearchOnGraphEvent]],
                             nsgMerger: ActorRef[MergeNSGEvent],
                             data: LocalData[Float]): Behavior[NodeCoordinationEvent] =
    Behaviors.receiveMessagePartial{
      case StartSearchOnGraph =>
        graphHolders.foreach(graphHolder => graphHolder ! GetNSGFrom(nsgMerger))
        Behaviors.empty
    }

}
