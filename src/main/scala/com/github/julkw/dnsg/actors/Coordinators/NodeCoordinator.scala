package com.github.julkw.dnsg.actors.Coordinators

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import akka.cluster.typed.{ClusterSingleton, SingletonActor}
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.{CoordinationEvent, NodeCoordinatorIntroduction}
import com.github.julkw.dnsg.actors.DataHolder
import com.github.julkw.dnsg.actors.DataHolder.{LoadDataEvent, LoadPartialDataFromFile}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.{GetNSGFrom, SearchOnGraphEvent, SendResponsibleIndicesTo, UpdatedLocalData}
import com.github.julkw.dnsg.actors.createNSG.{NSGMerger, NSGWorker}
import com.github.julkw.dnsg.actors.createNSG.NSGMerger.MergeNSGEvent
import com.github.julkw.dnsg.actors.nndescent.KnngWorker
import com.github.julkw.dnsg.actors.nndescent.KnngWorker.{BuildGraphEvent, MoveGraph, ResponsibleFor}
import com.github.julkw.dnsg.util.Data.LocalData
import com.github.julkw.dnsg.util.{Distance, NodeLocator, Settings, dNSGSerializable}

object NodeCoordinator {

  sealed trait NodeCoordinationEvent extends dNSGSerializable

  final case object StartDistributingData extends NodeCoordinationEvent

  final case class DataRef(dataRef: LocalData[Float]) extends NodeCoordinationEvent

  final case object StartSearchOnGraph extends NodeCoordinationEvent

  final case class StartBuildingNSG(navigatingNode: Int, nodeLocator: NodeLocator[ActorRef[SearchOnGraphEvent]]) extends NodeCoordinationEvent


  final case class LocalKnngWorker(worker: ActorRef[BuildGraphEvent]) extends NodeCoordinationEvent

  final case object AllDone extends NodeCoordinationEvent

  def apply(fileName: Option[String]): Behavior[NodeCoordinationEvent] = Behaviors.setup { ctx =>
    val settings = Settings(ctx.system.settings.config)

    // get access to cluster coordinator
    val singletonManager = ClusterSingleton(ctx.system)
    val clusterCoordinator: ActorRef[ClusterCoordinator.CoordinationEvent] = singletonManager.init(
      SingletonActor(Behaviors.supervise(ClusterCoordinator()).onFailure[Exception](SupervisorStrategy.restart), "ClusterCoordinator"))
    clusterCoordinator ! NodeCoordinatorIntroduction(ctx.self)

    val dh = ctx.spawn(DataHolder(ctx.self), name = "DataHolder")
    fileName match {
      case Some(fName) =>
        Behaviors.setup(
          ctx => new NodeCoordinator(settings, dh, clusterCoordinator, ctx).setUp(fName)
        )
      case None =>
        Behaviors.setup(
          ctx => new NodeCoordinator(settings, dh, clusterCoordinator, ctx).waitForData()
        )
    }
  }

}

class NodeCoordinator(settings: Settings,
                      dataHolder: ActorRef[LoadDataEvent],
                      clusterCoordinator: ActorRef[CoordinationEvent],
                      ctx: ActorContext[NodeCoordinator.NodeCoordinationEvent]) extends Distance {
  import NodeCoordinator._

  def setUp(filename: String): Behavior[NodeCoordinationEvent] = Behaviors.receiveMessagePartial {
    case StartDistributingData =>
      dataHolder ! LoadPartialDataFromFile(settings.nodesExpected, filename, settings.linesOffset, settings.lines, settings.dimensionOffset, settings.dimensions, ctx.self, clusterCoordinator)
      waitForData()
  }

  def waitForData(): Behavior[NodeCoordinationEvent] = Behaviors.receiveMessagePartial {
    case StartDistributingData =>
      // this node doesn't have the file and so can do nothing but wait
      waitForData()

    case DataRef(dataRef) =>
      val data = dataRef
      assert(data.localDataSize > 0)
      ctx.log.info("Successfully loaded data")
      // create Actor to start distribution of data
      val maxResponsibilityPerNode: Int = data.localDataSize / settings.workers + data.localDataSize / 10
      val kw = ctx.spawn(KnngWorker(data, maxResponsibilityPerNode, clusterCoordinator, ctx.self), name = "KnngWorker")
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
      val nsgw = ctx.spawn(SearchOnGraphActor(clusterCoordinator), name = "SearchOnGraph" + sogIndex.toString)
      sogIndex += 1
      worker ! MoveGraph(nsgw)
      nsgw
    }
    waitForNavigatingNode(data, graphHolders)
  }

  def waitForNavigatingNode(data: LocalData[Float], graphHolders: Set[ActorRef[SearchOnGraphEvent]]): Behavior[NodeCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      // In case of data redistribution
      case DataRef(newData) =>
        graphHolders.foreach(graphHolder=> graphHolder ! UpdatedLocalData(newData))
        waitForNavigatingNode(newData, graphHolders)

      // TODO this was unhandled in one runthrough
      case StartBuildingNSG(navigatingNode, nodeLocator) =>
        startBuildingNSG(data, nodeLocator, graphHolders, navigatingNode)
    }

  def startBuildingNSG(data: LocalData[Float],
                       nodeLocator: NodeLocator[ActorRef[SearchOnGraphEvent]],
                       graphHolders: Set[ActorRef[SearchOnGraphEvent]],
                       navigatingNode: Int): Behavior[NodeCoordinationEvent] = {
    val nsgMerger = ctx.spawn(NSGMerger(clusterCoordinator, data.localIndices, settings.nodesExpected, nodeLocator), name = "NSGMerger")
    var index = 0
    graphHolders.foreach { graphHolder =>
      val nsgWorker = ctx.spawn(NSGWorker(clusterCoordinator, data, navigatingNode, settings.k, settings.maxReverseNeighbors, nodeLocator, nsgMerger), name = "NSGWorker" + index.toString)
      index += 1
      // for now this is the easiest way to distribute responsibility
      graphHolder ! SendResponsibleIndicesTo(nsgWorker)
      nsgWorker
    }
    moveNSGToSearchOnGraph(graphHolders, nsgMerger, data)
  }

  // this happens here for mapping to the correct NSGMerger
  def moveNSGToSearchOnGraph(graphHolders: Set[ActorRef[SearchOnGraphEvent]],
                             nsgMerger: ActorRef[MergeNSGEvent],
                             data: LocalData[Float]): Behavior[NodeCoordinationEvent] =
    Behaviors.receiveMessagePartial{
      case StartSearchOnGraph =>
        graphHolders.foreach(graphHolder => graphHolder ! GetNSGFrom(nsgMerger))
        waitForShutdown()
    }

  def waitForShutdown(): Behavior[NodeCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case AllDone =>
        ctx.system.terminate()
        Behaviors.stopped
    }

}
