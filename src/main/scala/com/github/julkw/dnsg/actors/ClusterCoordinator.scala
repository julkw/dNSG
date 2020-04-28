package com.github.julkw.dnsg.actors

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.DataHolder.{GetAverageValue, LoadDataEvent, LoadPartialDataFromFile, LoadSiftDataFromFile, ReadTestQueries}
import com.github.julkw.dnsg.actors.SearchOnGraph.{AddToGraph, FindNearestNeighbors, FindNearestNeighborsStartingFrom, FindUnconnectedNode, GetNSGFrom, GraphDistribution, KNearestNeighbors, SearchOnGraphEvent, SendResponsibleIndicesTo, UpdateConnectivity}
import com.github.julkw.dnsg.actors.createNSG.NSGMerger.{MergeNSGEvent, NSGDistributed}
import com.github.julkw.dnsg.actors.createNSG.{NSGMerger, NSGWorker}
import com.github.julkw.dnsg.actors.nndescent.KnngWorker
import com.github.julkw.dnsg.actors.nndescent.KnngWorker._
import com.github.julkw.dnsg.util.{Distance, NodeLocator, NodeLocatorBuilder, PositionTree, Settings}

object ClusterCoordinator {

  sealed trait CoordinationEvent

  final case class AverageValue(average: Seq[Float]) extends CoordinationEvent

  final case class KnngDistributionInfo(responsibility: Seq[Int], sender: ActorRef[BuildGraphEvent]) extends CoordinationEvent

  final case class SearchOnGraphDistributionInfo(responsibility: Seq[Int], sender: ActorRef[SearchOnGraphEvent]) extends CoordinationEvent

  final case object FinishedApproximateGraph extends CoordinationEvent

  final case object FinishedNNDescent extends CoordinationEvent

  final case object CorrectFinishedNNDescent extends CoordinationEvent

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
    ctx.log.info("start building the approximate graph")
    Behaviors.setup(
      ctx => new ClusterCoordinator(settings, buildGraphEventAdapter, searchOnGraphEventAdapter, ctx).setUp()
    )
  }

}

class ClusterCoordinator(settings: Settings,
                         buildGraphEventAdapter: ActorRef[KnngWorker.BuildGraphEvent],
                         searchOnGraphEventAdapter: ActorRef[SearchOnGraph.SearchOnGraphEvent],
                         ctx: ActorContext[ClusterCoordinator.CoordinationEvent]) extends Distance {
  import ClusterCoordinator._

  def setUp(): Behavior[CoordinationEvent] = Behaviors.receiveMessagePartial {
    // TODO wait on data length from DataHolder and call next step
    case _ =>
      Behaviors.same
  }

  def distributeDataForKnng(nodeLocatorBuilder: NodeLocatorBuilder[BuildGraphEvent], knngWorkers: Set[ActorRef[BuildGraphEvent]]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
        case KnngDistributionInfo(responsibility, worker) =>
          val updatedKnngWorkers = knngWorkers + worker
          nodeLocatorBuilder.addLocations(responsibility, worker) match {
            case Some(nodeLocator) =>
              ctx.log.info("Data distribution done, start building approximate graph")
              updatedKnngWorkers.foreach{ worker =>
                worker ! BuildApproximateGraph(nodeLocator)
              }
              buildApproximageKnng(knngWorkers, 0, nodeLocator.graphSize)
            case None =>
              // not yet collected all distributionInfo
              distributeDataForKnng(nodeLocatorBuilder, updatedKnngWorkers)
          }
    }

  def buildApproximageKnng(knngWorkers: Set[ActorRef[BuildGraphEvent]], workersDone: Int, graphSize: Int): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case FinishedApproximateGraph =>
        val updatedWorkersDone = workersDone + 1
        if (updatedWorkersDone == knngWorkers.size) {
          ctx.log.info("Approximate graph has been build. Start NNDescent")
          knngWorkers.foreach(worker => worker ! StartNNDescent)
          waitForNnDescent(knngWorkers, 0, graphSize)
        } else {
          buildApproximageKnng(knngWorkers, updatedWorkersDone, graphSize)
        }
    }

  def waitForNnDescent(knngWorkers: Set[ActorRef[BuildGraphEvent]], workersDone: Int, graphSize: Int): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case FinishedNNDescent =>
        val updatedWorkersDone = workersDone + 1
        if (updatedWorkersDone == knngWorkers.size) {
          ctx.log.info("NNDescent seems to be done")
          // TODO tell NodeCoordinators to spawn SearchOnGraphActors
          waitOnSearchOnGraphDistributionInfo(NodeLocatorBuilder(graphSize), Set.empty)
        } else {
          waitForNnDescent(knngWorkers, updatedWorkersDone, graphSize)
        }

      case CorrectFinishedNNDescent =>
        waitForNnDescent(knngWorkers, workersDone - 1, graphSize)
    }

  def waitOnSearchOnGraphDistributionInfo(nodeLocatorBuilder: NodeLocatorBuilder[SearchOnGraphEvent], sogActors: Set[ActorRef[SearchOnGraphEvent]]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case SearchOnGraphDistributionInfo(indices, actorRef) =>
        val updatedSogActors = sogActors + actorRef
        nodeLocatorBuilder.addLocations(indices, actorRef) match {
          case Some (nodeLocator) =>
            // TODO: Find Navigating Node -> How to get Average from DataHolder at this point?
            ctx.log.info("All graphs now with SearchOnGraph actors")
            updatedSogActors.foreach(sogActor => sogActor ! GraphDistribution(nodeLocator))
            searchOnKnng(nodeLocator)
          case None =>
            waitOnSearchOnGraphDistributionInfo(nodeLocatorBuilder, updatedSogActors)
        }
    }

  def searchOnKnng(nodeLocator: NodeLocator[SearchOnGraphEvent]): Behavior[CoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case AverageValue(value) =>
        // find navigating Node
        nodeLocator.findResponsibleActor(value) ! FindNearestNeighbors(value, searchOnGraphEventAdapter)
        searchOnKnng(nodeLocator)

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
