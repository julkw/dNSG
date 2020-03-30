package com.github.julkw.dnsg.actors

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.DataHolder.{GetAverageValue, LoadDataEvent, LoadSiftDataFromFile}
import com.github.julkw.dnsg.actors.SearchOnGraph.{FindNearestNeighbors, GraphDistribution, KNearestNeighbors, SearchOnGraphEvent}
import com.github.julkw.dnsg.actors.createNSG.NSGWorker
import com.github.julkw.dnsg.actors.createNSG.NSGWorker.BuildNSGEvent
import com.github.julkw.dnsg.actors.nndescent.KnngWorker
import com.github.julkw.dnsg.actors.nndescent.KnngWorker._
import com.github.julkw.dnsg.util.{NodeLocator, PositionTree}

object Coordinator {

  sealed trait CoordinationEvent

  final case class DataRef(dataRef: Seq[Seq[Float]]) extends CoordinationEvent

  final case class AverageValue(average: Seq[Float]) extends CoordinationEvent

  final case class WrappedBuildGraphEvent(event: KnngWorker.BuildGraphEvent) extends CoordinationEvent

  final case class WrappedSearchOnGraphEvent(event: SearchOnGraph.SearchOnGraphEvent) extends CoordinationEvent

  final case class GraphReceived(nsgWorker: ActorRef[BuildNSGEvent], knngWorker: ActorRef[BuildGraphEvent]) extends CoordinationEvent

  def apply(): Behavior[CoordinationEvent] = Behaviors.setup { ctx =>
    // TODO turn into input through configuration
    val filename: String = "/home/juliane/code/dNSG/data/siftsmall/siftsmall_base.fvecs"
    val k = 10
    val numWorkers = 4

    val buildGraphEventAdapter: ActorRef[KnngWorker.BuildGraphEvent] =
      ctx.messageAdapter { event => WrappedBuildGraphEvent(event) }

    val searchOnGraphEventAdapter: ActorRef[SearchOnGraph.SearchOnGraphEvent] =
      ctx.messageAdapter { event => WrappedSearchOnGraphEvent(event)}

    val dh = ctx.spawn(DataHolder(), name = "DataHolder")
    dh ! LoadSiftDataFromFile(filename, ctx.self)

    ctx.log.info("start building the approximate graph")
    Behaviors.setup(
      ctx => new Coordinator(k, numWorkers, filename, dh, buildGraphEventAdapter, searchOnGraphEventAdapter, ctx).setUp()
    )
  }

}

class Coordinator(k: Int,
                  numWorkers: Int,
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
              val nsgw = ctx.spawn(SearchOnGraph(ctx.self, data), name = "SearchOnGraph" + i.toString)
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
        nodeLocator.findResponsibleActor(value) ! FindNearestNeighbors(value, 1, searchOnGraphEventAdapter)
        searchOnKnng(data, nodeLocator)

      case wrappedSearchOnGraphEvent: WrappedSearchOnGraphEvent =>
        wrappedSearchOnGraphEvent.event match {
          case KNearestNeighbors(query, neighbors) =>
            // Right now the only query being asked for is the NavigationNode, so that has been found
            val navigatingNode = neighbors.head
            ctx.log.info("The navigating node has the index: {}", navigatingNode)
            // TODO do some kind of data redistribution with the knowledge of the navigating node, updating the nodeLocator in the process
            buildNSG(navigatingNode)
        }
    }

  def buildNSG(navigatingNodeIndex: Int) : Behavior[CoordinationEvent] = Behaviors.receiveMessagePartial{
    // TODO Create NSGWorkers (or let them be created by SearchOnGraph Actors?
    // each nsgWorker is responsible for finding the NSG edges for each of the g_nodes held by the accompanying SearchOnGraph actor
    // for this we need a modified SearchOnGraph that returns all looked at nodes (does not cut off the returned neighbors)
    // how do I do this with little code duplication?
    case _ =>
      Behaviors.same
  }
}
