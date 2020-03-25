package com.github.julkw.dnsg.actors

import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.DataHolder.{GetAverageValue, LoadDataEvent, LoadSiftDataFromFile}
import com.github.julkw.dnsg.actors.nndescent.KnngWorker
import com.github.julkw.dnsg.actors.nndescent.KnngWorker._
import com.github.julkw.dnsg.util.PositionTree

object Coordinator {

  sealed trait CoordinationEvent

  final case class DataRef(dataRef: Seq[Seq[Float]]) extends CoordinationEvent

  final case class AverageValue(average: Seq[Float]) extends CoordinationEvent

  final case class WrappedBuildGraphEvent(event: KnngWorker.BuildGraphEvent) extends CoordinationEvent

  final case class ListingResponse(listing: Receptionist.Listing) extends CoordinationEvent

  // TODO turn into input through configurations
  val k = 10
  val numWorkers = 4
  var data: Seq[Seq[Float]] = Seq.empty
  var knngWorkers: Set[ActorRef[BuildGraphEvent]] = Set.empty
  var distributionTree: Option[PositionTree] = None

  def apply(): Behavior[CoordinationEvent] = Behaviors.setup { ctx =>
    val filename: String = "/home/juliane/code/dNSG/data/siftsmall/siftsmall_base.fvecs"

    val buildGraphEventAdapter: ActorRef[KnngWorker.BuildGraphEvent] =
      ctx.messageAdapter { event => WrappedBuildGraphEvent(event) }

    // Stay updated on the actors building and holding the graph
    val listingResponseAdapter = ctx.messageAdapter[Receptionist.Listing](ListingResponse)
    ctx.system.receptionist ! Receptionist.Subscribe(KnngWorker.knngServiceKey, listingResponseAdapter)

    def buildApproximateGraph(dh: ActorRef[LoadDataEvent]): Behavior[CoordinationEvent] =
      Behaviors.receiveMessagePartial {
        case DataRef(dataRef) =>
          data = dataRef
          ctx.log.info("Successfully loaded data")
          // TODO start KnngWorkers
          // create Actor to start distribution of data
          val maxResponsibilityPerNode: Int = data.length / numWorkers + data.length / 10
          val kw = ctx.spawn(KnngWorker(data, maxResponsibilityPerNode, k, buildGraphEventAdapter), name = "KnngWorker")
          val allIndices: Seq[Int] = 0 until data.length
          kw ! ResponsibleFor(allIndices)
          buildApproximateGraph(dh)

        case AverageValue(value) =>
          // TODO safe distribution tree and actually use it to send query to the most suited worker
          knngWorkers.head ! Query(value, buildGraphEventAdapter)
          buildApproximateGraph(dh)

        case wrappedListing: ListingResponse =>
          wrappedListing.listing match {
            case KnngWorker.knngServiceKey.Listing(listings) =>
              // if there already is a distributionTree, the new actors need to be told
              ctx.log.info("Received new listing. Number of dataholding actors {}", listings.size)
              if (distributionTree.isDefined) {
                (listings -- knngWorkers).foreach(actor => actor ! DistributionTree(distributionTree.get))
              }
              knngWorkers = listings
          }
          buildApproximateGraph(dh)

        case wrappedGraphEvent: WrappedBuildGraphEvent =>
          // handle the response from Configuration, which we understand since it was wrapped in a message that is part of
          // the protocol of this actor
          wrappedGraphEvent.event match {
            case DistributionInfo(distInfoRoot, _) =>
              ctx.log.info("Tell all dataholding workers where other data is placed so they can start building the approximate graph")
              val positionTree: PositionTree = PositionTree(distInfoRoot)
              distributionTree = Option(positionTree)
              knngWorkers.foreach(worker => worker ! DistributionTree(positionTree))

            case FinishedApproximateGraph =>
              ctx.log.info("Approximate graph has been build")
              knngWorkers.foreach(worker => worker ! StartNNDescent)

            case FinishedNNDescent =>
              ctx.log.info("NNDescent seems to be done")
              knngWorkers.foreach(worker => worker ! StartSearchOnGraph)
              dh ! GetAverageValue(ctx.self)

            case KNearestNeighbors(query, neighbors) =>
              ctx.log.info("Received an answer to my query")
              // Right now the only query being asked for is the NavigationNode, so that has been found
              val navigatingNode = neighbors.head
              // TODO safe graph to file with information of navigating Node included

            case CorrectFinishedNNDescent =>
              // In case of cluster tell Cluster Coordinator, else this hopefully shouldn't happen

          }
          buildApproximateGraph(dh)
      }

    val dh = ctx.spawn(DataHolder(), name = "DataHolder")
    // TODO specify file and type through configurations
    dh ! LoadSiftDataFromFile(filename, ctx.self)

    ctx.log.info("start building the approximate graph")
    buildApproximateGraph(dh)
  }

}
