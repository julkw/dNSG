package com.github.julkw.dnsg.actors

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.DataHolder.{GetAverageValue, LoadDataEvent, LoadPartialDataFromFile, LoadSiftDataFromFile, ReadTestQueries}
import com.github.julkw.dnsg.actors.SearchOnGraph.{AddToGraph, FindNearestNeighbors, FindNearestNeighborsStartingFrom, FindUnconnectedNode, GetNSGFrom, GraphDistribution, KNearestNeighbors, SearchOnGraphEvent, SendResponsibleIndicesTo, UpdateConnectivity}
import com.github.julkw.dnsg.actors.createNSG.NSGMerger.{MergeNSGEvent, NSGDistributed}
import com.github.julkw.dnsg.actors.createNSG.{NSGMerger, NSGWorker}
import com.github.julkw.dnsg.actors.nndescent.KnngWorker
import com.github.julkw.dnsg.actors.nndescent.KnngWorker._
import com.github.julkw.dnsg.util.{Distance, NodeLocator, PositionTree, Settings}

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

  // testing the graph

  final case class TestQueries(queries: Seq[(Seq[Float], Seq[Int])]) extends CoordinationEvent


  def apply(): Behavior[CoordinationEvent] = Behaviors.setup { ctx =>
    val buildGraphEventAdapter: ActorRef[KnngWorker.BuildGraphEvent] =
      ctx.messageAdapter { event => WrappedBuildGraphEvent(event) }

    val searchOnGraphEventAdapter: ActorRef[SearchOnGraph.SearchOnGraphEvent] =
      ctx.messageAdapter { event => WrappedSearchOnGraphEvent(event)}

    val settings = Settings(ctx.system.settings.config)

    val dh = ctx.spawn(DataHolder(), name = "DataHolder")
    //dh ! LoadSiftDataFromFile(filename, ctx.self)
    dh ! LoadPartialDataFromFile(settings.inputFilePath, settings.linesOffset, settings.lines, settings.dimensionOffset, settings.dimensions, ctx.self)

    ctx.log.info("start building the approximate graph")
    Behaviors.setup(
      ctx => new Coordinator(settings, dh, buildGraphEventAdapter, searchOnGraphEventAdapter, ctx).setUp()
    )
  }

}

class Coordinator(settings: Settings,
                  dataHolder: ActorRef[LoadDataEvent],
                  buildGraphEventAdapter: ActorRef[KnngWorker.BuildGraphEvent],
                  searchOnGraphEventAdapter: ActorRef[SearchOnGraph.SearchOnGraphEvent],
                  ctx: ActorContext[Coordinator.CoordinationEvent]) extends Distance {
  import Coordinator._

  def setUp(): Behavior[CoordinationEvent] = Behaviors.receiveMessagePartial {
    case DataRef(dataRef) =>
      val data = dataRef
      ctx.log.info("Successfully loaded data")
      // create Actor to start distribution of data
      val maxResponsibilityPerNode: Int = data.length / settings.workers + data.length / 10
      val kw = ctx.spawn(KnngWorker(data, maxResponsibilityPerNode, settings.k, settings.sampleRate, buildGraphEventAdapter), name = "KnngWorker")
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
            workers.foreach(worker => worker ! BuildApproximateGraph(nodeLocator))
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
              val nsgw = ctx.spawn(SearchOnGraph(ctx.self, data, settings.k), name = "SearchOnGraph" + i.toString)
              worker ! StartSearchOnGraph(nsgw)
            }
            buildKnng(data, knngWorkers, nodeLocator)

          case SOGDistributionInfo(treeNode, _) =>
            ctx.log.info("All graphs now with SearchOnGraph actors")
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
        val nsgWorker = ctx.spawn(NSGWorker(ctx.self, data, navigatingNode, settings.maxReverseNeighbors, nodeLocator, nsgMerger), name = "NSGWorker" + i.toString)
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
        dataHolder ! ReadTestQueries(settings.queryFilePath, ctx.self)
        testNSG(data, navigatingNodeIndex, nodeLocator, Map.empty, 0)
    }

    def testNSG(data: Seq[Seq[Float]],
                navigatingNodeIndex: Int,
                nodeLocator: NodeLocator[SearchOnGraphEvent],
                queries: Map[Seq[Float], Seq[Int]],
                sumOfNeighborsFound: Int): Behavior[CoordinationEvent] =
      Behaviors.receiveMessagePartial{
        case TestQueries(testQueries) =>
          testQueries.foreach(query => nodeLocator.findResponsibleActor(query._1) !
              FindNearestNeighborsStartingFrom(query._1, navigatingNodeIndex, searchOnGraphEventAdapter))
          testNSG(data, navigatingNodeIndex, nodeLocator, testQueries.toMap, sumOfNeighborsFound)

        case wrappedSearchOnGraphEvent: WrappedSearchOnGraphEvent =>
          wrappedSearchOnGraphEvent.event match {
            case KNearestNeighbors(query, neighbors) =>
              val correctNeighborIndices = queries(query)
              //val cniWithDist = correctNeighborIndices.map(index => (index, euclideanDist(data(index), query)))
              //val foundNeighborsWithDist = neighbors.map(index => (index, euclideanDist(data(index), query)))
              //ctx.log.info("The correct neighbors would have been: {}", correctNeighborIndices)
              //ctx.log.info("The NSG found: {}", neighbors)
              //ctx.log.info("Found {} of {} nearest neighbors", correctNeighborIndices.intersect(neighbors).length, correctNeighborIndices.length)
              val newSum = sumOfNeighborsFound + correctNeighborIndices.intersect(neighbors).length
              if (queries.size == 1) {
                ctx.log.info("Overall correct neighbors found: {}", newSum)
                ctx.system.terminate()
                Behaviors.stopped
              } else {
                testNSG(data, navigatingNodeIndex, nodeLocator, queries - query, newSum)
              }

          }
      }

}
