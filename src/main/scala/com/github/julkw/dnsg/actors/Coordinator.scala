package com.github.julkw.dnsg.actors

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.DataHolder.{GetAverageValue, LoadDataEvent, LoadSiftDataFromFile}
import com.github.julkw.dnsg.actors.SearchOnGraph.{AddToGraph, FindNearestNeighbors, FindNearestNeighborsStartingFrom, FindUnconnectedNode, GetNSGFrom, GraphDistribution, KNearestNeighbors, SearchOnGraphEvent, SendResponsibleIndicesTo, UpdateConnectivity}
import com.github.julkw.dnsg.actors.createNSG.NSGMerger.{MergeNSGEvent, NSGDistributed}
import com.github.julkw.dnsg.actors.createNSG.{NSGMerger, NSGWorker}
import com.github.julkw.dnsg.actors.nndescent.KnngWorker
import com.github.julkw.dnsg.actors.nndescent.KnngWorker._
import com.github.julkw.dnsg.util.{NodeLocator, PositionTree}

object Coordinator {

  sealed trait CoordinationEvent

  final case class DataRef(dataRef: Seq[Seq[Float]]) extends CoordinationEvent

  final case class AverageValue(average: Seq[Float]) extends CoordinationEvent

  final case class WrappedBuildGraphEvent(event: KnngWorker.BuildGraphEvent) extends CoordinationEvent

  final case class WrappedSearchOnGraphEvent(event: SearchOnGraph.SearchOnGraphEvent) extends CoordinationEvent

  // building the NSG
  final case class InitialNSGDone(nsgHolder: ActorRef[MergeNSGEvent]) extends CoordinationEvent

  final case object UpdatedToNSG extends CoordinationEvent

  final case object FinishedUpdatingConnectivity extends CoordinationEvent

  final case class UnconnectedNode(nodeIndex: Int) extends CoordinationEvent

  final case object AllConnected extends CoordinationEvent


  def apply(): Behavior[CoordinationEvent] = Behaviors.setup { ctx =>
    // TODO turn into input through configuration
    val filename: String = "/home/juliane/code/dNSG/data/siftsmall/siftsmall_base.fvecs"
    val k: Int = 10
    val maxReverseNeighbors: Int = 10
    val numWorkers: Int = 4

    val buildGraphEventAdapter: ActorRef[KnngWorker.BuildGraphEvent] =
      ctx.messageAdapter { event => WrappedBuildGraphEvent(event) }

    val searchOnGraphEventAdapter: ActorRef[SearchOnGraph.SearchOnGraphEvent] =
      ctx.messageAdapter { event => WrappedSearchOnGraphEvent(event)}

    val dh = ctx.spawn(DataHolder(), name = "DataHolder")
    dh ! LoadSiftDataFromFile(filename, ctx.self)

    ctx.log.info("start building the approximate graph")
    Behaviors.setup(
      ctx => new Coordinator(k, numWorkers, maxReverseNeighbors, filename, dh, buildGraphEventAdapter, searchOnGraphEventAdapter, ctx).setUp()
    )
  }

}

class Coordinator(k: Int,
                  numWorkers: Int,
                  maxReverseNeighbors: Int,
                  filename: String,
                  dataHolder: ActorRef[LoadDataEvent],
                  buildGraphEventAdapter: ActorRef[KnngWorker.BuildGraphEvent],
                  searchOnGraphEventAdapter: ActorRef[SearchOnGraph.SearchOnGraphEvent],
                  ctx: ActorContext[Coordinator.CoordinationEvent]) {
  import Coordinator._

  def setUp(): Behavior[CoordinationEvent] = Behaviors.receiveMessagePartial {
    case DataRef(dataRef) =>
      val data = dataRef
      ctx.log.info("Successfully loaded data")
      // create Actor to start distribution of data
      val maxResponsibilityPerNode: Int = data.length / numWorkers + data.length / 10
      val kw = ctx.spawn(KnngWorker(data, maxResponsibilityPerNode, k, buildGraphEventAdapter), name = "KnngWorker")
      val allIndices: Seq[Int] = 0 until data.length
      kw ! ResponsibleFor(allIndices)
      distributeDataForKnng(data)
  }

  def distributeDataForKnng(data: Seq[Seq[Float]]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case wrappedGraphEvent: WrappedBuildGraphEvent =>
        // handle the response from Configuration, which we understand since it was wrapped in a message that is part of
        // the protocol of this actor
        wrappedGraphEvent.event match {
          case DistributionInfo(distInfoRoot, _) =>
            ctx.log.info("Tell all dataholding workers where other data is placed so they can start building the approximate graph")
            val positionTree: PositionTree[BuildGraphEvent] = PositionTree(distInfoRoot)
            val workers = positionTree.allLeafs().map(leaf => leaf.data)
            val nodeLocator: NodeLocator[BuildGraphEvent] = NodeLocator(positionTree)
            workers.foreach(worker => worker ! BuildApproximateGraph(NodeLocator(positionTree)))
            buildKnng(data, workers, nodeLocator)
        }
    }

  def buildKnng(data: Seq[Seq[Float]],
                knngWorkers: Seq[ActorRef[BuildGraphEvent]],
                nodeLocator: NodeLocator[BuildGraphEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case wrappedGraphEvent: WrappedBuildGraphEvent =>
        // handle the response from Configuration, which we understand since it was wrapped in a message that is part of
        // the protocol of this actor
        wrappedGraphEvent.event match {
          case FinishedApproximateGraph =>
            ctx.log.info("Approximate graph has been build")
            knngWorkers.foreach(worker => worker ! StartNNDescent)
            buildKnng(data, knngWorkers, nodeLocator)

          case FinishedNNDescent =>
            ctx.log.info("NNDescent seems to be done")
            // tell workers that the graph is finished and they can move it to the searchOnGraph actors
            knngWorkers.foreach { worker =>
              val i = knngWorkers.indexOf(worker)
              val nsgw = ctx.spawn(SearchOnGraph(ctx.self, data, k), name = "SearchOnGraph" + i.toString)
              worker ! StartSearchOnGraph(nsgw)
            }
            buildKnng(data, knngWorkers, nodeLocator)

          case SOGDistributionInfo(treeNode, _) =>
            ctx.log.info("All graphs not with SearchOnGraph actors")
            val nodeLocator: NodeLocator[SearchOnGraphEvent] = NodeLocator(PositionTree(treeNode))
            nodeLocator.positionTree.allLeafs().foreach(graphHolder => graphHolder.data ! GraphDistribution(nodeLocator))
            dataHolder ! GetAverageValue(ctx.self)
            searchOnKnng(data, nodeLocator)

          case CorrectFinishedNNDescent =>
          // In case of cluster tell Cluster Coordinator, else this hopefully shouldn't happen
            buildKnng(data, knngWorkers, nodeLocator)
        }
    }

  def searchOnKnng(data: Seq[Seq[Float]],
                   nodeLocator: NodeLocator[SearchOnGraphEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case AverageValue(value) =>
        // find navigating Node
        nodeLocator.findResponsibleActor(value) ! FindNearestNeighbors(value, searchOnGraphEventAdapter)
        searchOnKnng(data, nodeLocator)

      case wrappedSearchOnGraphEvent: WrappedSearchOnGraphEvent =>
        wrappedSearchOnGraphEvent.event match {
          case KNearestNeighbors(query, neighbors) =>
            // Right now the only query being asked for is the NavigationNode, so that has been found
            val navigatingNode = neighbors.head
            ctx.log.info("The navigating node has the index: {}", navigatingNode)
            // TODO do some kind of data redistribution with the knowledge of the navigating node, updating the nodeLocator in the process
            startBuildingNSG(data, nodeLocator, navigatingNode)
        }
    }

  def startBuildingNSG(data: Seq[Seq[Float]],
                       nodeLocator: NodeLocator[SearchOnGraphEvent],
                       navigatingNode: Int): Behavior[CoordinationEvent] = {
    val nsgMerger = ctx.spawn(NSGMerger(ctx.self, 0 until data.length), name = "NSGMerger")

    val graphHolders = nodeLocator.positionTree.allLeafs().map(leaf => leaf.data)
      graphHolders.foreach{ graphHolder =>
        val i = graphHolders.indexOf(graphHolder)
        val nsgWorker = ctx.spawn(NSGWorker(ctx.self, data, navigatingNode, maxReverseNeighbors, nodeLocator, nsgMerger), name = "NSGWorker" + i.toString)
        // for now this is the easiest way to distribute responsibility
        graphHolder ! SendResponsibleIndicesTo(nsgWorker)
      }
      buildNSG(data, navigatingNode, nsgMerger, nodeLocator, 0)
    }

  def buildNSG(data: Seq[Seq[Float]],
               navigatingNodeIndex: Int,
               nsgMerger: ActorRef[MergeNSGEvent],
               nodeLocator: NodeLocator[SearchOnGraphEvent],
               awaitingUpdates: Int) : Behavior[CoordinationEvent] = Behaviors.receiveMessagePartial{
    case InitialNSGDone(nsgHolder) =>
      // tell the searchOnGraph Actors to upgrade their knng to NSG
      val graphHolders = nodeLocator.positionTree.allLeafs().map(leaf => leaf.data)
      graphHolders.foreach(graphHolder => graphHolder ! GetNSGFrom(nsgHolder))
      buildNSG(data, navigatingNodeIndex, nsgMerger, nodeLocator, graphHolders.length)

    case UpdatedToNSG =>
      if (awaitingUpdates == 1) {
        // nsg is fully distributed, merger can be shutdown
        nsgMerger ! NSGDistributed
        // check NSG for connectivity
        nodeLocator.findResponsibleActor(data(navigatingNodeIndex)) ! UpdateConnectivity(navigatingNodeIndex)
        connectNSG(data, navigatingNodeIndex, nodeLocator, navigatingNodeIndex)
      } else {
        buildNSG(data, navigatingNodeIndex, nsgMerger, nodeLocator, awaitingUpdates - 1)
      }
  }

  def connectNSG(data: Seq[Seq[Float]],
                 navigatingNodeIndex: Int,
                 nodeLocator: NodeLocator[SearchOnGraphEvent],
                 latestUnconnectedNode: Int) : Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial{
      case FinishedUpdatingConnectivity =>
        // find out if there is still an unconnected node and connect it
        val graphHolders = nodeLocator.positionTree.allLeafs().map(leaf => leaf.data)
        graphHolders.head ! FindUnconnectedNode(ctx.self, graphHolders.slice(1, graphHolders.length).toSet)
        connectNSG(data, navigatingNodeIndex, nodeLocator, latestUnconnectedNode)

      case UnconnectedNode(nodeIndex) =>
        ctx.log.info("found an unconnected node")
        nodeLocator.findResponsibleActor(data(nodeIndex)) !
          FindNearestNeighborsStartingFrom(data(nodeIndex), navigatingNodeIndex, searchOnGraphEventAdapter)
        connectNSG(data, navigatingNodeIndex, nodeLocator, nodeIndex)

      case wrappedSearchOnGraphEvent: WrappedSearchOnGraphEvent =>
        wrappedSearchOnGraphEvent.event match {
          case KNearestNeighbors(query, neighbors) =>
            // Right now the only query being asked for is to connect unconnected nodes
            assert(query == data(latestUnconnectedNode))
            neighbors.foreach(reverseNeighbor =>
              nodeLocator.findResponsibleActor(data(reverseNeighbor)) !
                AddToGraph(reverseNeighbor, latestUnconnectedNode))
            nodeLocator.findResponsibleActor(data(latestUnconnectedNode)) ! UpdateConnectivity(latestUnconnectedNode)
            connectNSG(data, navigatingNodeIndex, nodeLocator, latestUnconnectedNode)
        }

      case AllConnected =>
        ctx.log.info("NSG build seems to be done")
        connectNSG(data, navigatingNodeIndex, nodeLocator, latestUnconnectedNode)
    }

}
