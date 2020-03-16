package com.github.julkw.dnsg.actors

import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.DataHolder.LoadSiftDataFromFile
import com.github.julkw.dnsg.actors.nndescent.KnngWorker
import com.github.julkw.dnsg.actors.nndescent.KnngWorker._
import com.github.julkw.dnsg.util.PositionTree

object Coordinator {

  sealed trait CoordinationEvent

  final case class DataRef(dataRef: Seq[Seq[Float]]) extends CoordinationEvent

  final case class WrappedBuildGraphEvent(event: KnngWorker.BuildGraphEvent) extends CoordinationEvent

  final case class ListingResponse(listing: Receptionist.Listing) extends CoordinationEvent

  // TODO turn into input through configurations
  val k = 10
  val numWorkers = 2
  var data: Seq[Seq[Float]] = Seq.empty
  var knngWorkers: Set[ActorRef[BuildGraphEvent]] = Set.empty

  def apply(): Behavior[CoordinationEvent] = Behaviors.setup { ctx =>
    val filename: String = "/home/juliane/code/dNSG/data/siftsmall/siftsmall_base.fvecs"

    val buildGraphEventAdapter: ActorRef[KnngWorker.BuildGraphEvent] =
      ctx.messageAdapter { event => WrappedBuildGraphEvent(event) }

    // Stay updated on the actors building and holding the graph
    val listingResponseAdapter = ctx.messageAdapter[Receptionist.Listing](ListingResponse)
    ctx.system.receptionist ! Receptionist.Subscribe(KnngWorker.knngServiceKey, listingResponseAdapter)

    def buildApproximateGraph(): Behavior[CoordinationEvent] = {
      ctx.log.info("start building the approximate graph")
      Behaviors.receiveMessagePartial {
        case DataRef(dataRef) =>
          data = dataRef
          ctx.log.info("Successfully loaded data")
          // TODO start KnngWorkers
          // create Actor to start distribution of data
          val maxResponsibilityPerNode: Int = data.length / numWorkers
          val kw = ctx.spawn(KnngWorker(data, maxResponsibilityPerNode, k, buildGraphEventAdapter), name = "KnngWorker")
          val allIndices: Seq[Int] = 0 until data.length
          kw ! ResponsibleFor(allIndices)
          Behaviors.same

        case wrapped: WrappedBuildGraphEvent =>
          // handle the response from Configuration, which we understand since it was wrapped in a message that is part of
          // the protocol of this actor
          wrapped.event match {
            case DistributionInfo(distInfoRoot, _) =>
              ctx.log.info("Tell all dataholding workers where other data is placed so they can start building the approximate graph")
              val positionTree: PositionTree = PositionTree(distInfoRoot)
              knngWorkers.foreach(worker => worker ! DistributionTree(positionTree))
              Behaviors.same

            case FinishedApproximateGraph =>
              // TODO start NNDescent Phase
              // is this needed or do the workers just switch by themselves?
              Behaviors.same

            case KnngWorker.knngServiceKey.Listing(listings) =>
              knngWorkers = listings
              Behaviors.same
          }
      }
    }

    val dh = ctx.spawn(DataHolder(), name = "DataHolder")
    // TODO specify file and type through configurations
    dh ! LoadSiftDataFromFile(filename, ctx.self)

    buildApproximateGraph()
  }

}
