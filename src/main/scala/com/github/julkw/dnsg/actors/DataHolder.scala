package com.github.julkw.dnsg.actors

import java.io.{BufferedInputStream, FileInputStream}
import java.nio
import java.nio.ByteBuffer
import java.nio.ByteOrder

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.julkw.dnsg.actors.ClusterCoordinator.{AverageValue, CoordinationEvent, DataSize, TestQueries}
import com.github.julkw.dnsg.actors.NodeCoordinator.{DataRef, NodeCoordinationEvent}
import com.github.julkw.dnsg.actors.SearchOnGraph.{GetGraph, Graph, SearchOnGraphEvent}
import com.github.julkw.dnsg.util.LocalData

import scala.language.postfixOps

object DataHolder {

  sealed trait LoadDataEvent

  final case class LoadSiftDataFromFile(filename: String, replyTo: ActorRef[NodeCoordinationEvent], clusterCoordinator: ActorRef[CoordinationEvent]) extends LoadDataEvent

  final case class LoadPartialDataFromFile(filename: String, lineOffset: Int, linesUsed: Int, dimensionsOffset: Int, dimensionsUsed: Int, replyTo: ActorRef[NodeCoordinationEvent], clusterCoordinator: ActorRef[CoordinationEvent]) extends LoadDataEvent

  final case object WaitForStreamedData extends LoadDataEvent

  final case class GetAverageValue(replyTo: ActorRef[CoordinationEvent]) extends LoadDataEvent

  final case class ReadTestQueries(filename: String, replyTo: ActorRef[CoordinationEvent]) extends LoadDataEvent

  // TODO this message is not really being handled at the moment
  final case class SaveGraphToFile(filename: String, graphHolders: Set[ActorRef[SearchOnGraphEvent]], k: Int) extends LoadDataEvent

  final case class WrappedSearchOnGraphEvent(event: SearchOnGraph.SearchOnGraphEvent) extends LoadDataEvent

  val dataHolderServiceKey: ServiceKey[DataHolder.LoadDataEvent] = ServiceKey[LoadDataEvent]("dataService")

  private case class ListingResponse(listing: Receptionist.Listing) extends LoadDataEvent

  def apply(): Behavior[LoadDataEvent] = Behaviors.setup { ctx =>
    ctx.log.info("Started up DataHolder")

    val listingResponseAdapter = ctx.messageAdapter[Receptionist.Listing](ListingResponse)

    new DataHolder(ctx, listingResponseAdapter).setup()
  }
}

class DataHolder(ctx: ActorContext[DataHolder.LoadDataEvent], listingAdapter: ActorRef[Receptionist.Listing]) {
  import DataHolder._

  def setup(): Behavior[LoadDataEvent] = {
    ctx.system.receptionist ! Receptionist.Register(dataHolderServiceKey, ctx.self)
    ctx.system.receptionist ! Receptionist.Subscribe(dataHolderServiceKey, listingAdapter)
    ctx.system.receptionist ! Receptionist.Find(dataHolderServiceKey, listingAdapter)
    loadData(Set.empty)
  }

  def loadData(dataHolders: Set[ActorRef[LoadDataEvent]]): Behavior[LoadDataEvent] =
    Behaviors.receiveMessagePartial {
      case ListingResponse(dataHolderServiceKey.Listing(listings)) =>
        ctx.log.info("{} dataHolders found", listings.size)
        loadData(dataHolders ++ listings)

      case WaitForStreamedData =>
        ctx.log.info("Not supplied with file, waiting for other node to send data")
        loadData(dataHolders)

      case LoadSiftDataFromFile(filename, replyTo, clusterCoordinator) =>
        ctx.log.info("Asked to load SIFT data from {}", filename)
        val data = readData(filename)
        clusterCoordinator ! DataSize(data.length, ctx.self)
        // TODO distribute the data
        val localData = LocalData(data, 0)
        replyTo ! DataRef(localData)
        holdData(localData, dataHolders)

      // only return part of the data for testing (number of lines and dimensions need to be known beforehand for this)
      case LoadPartialDataFromFile(filename, lineOffset, linesUsed, dimensionsOffset, dimensionsUsed, replyTo, clusterCoordinator) =>
        val data = readData(filename).slice(lineOffset, lineOffset + linesUsed).map(vector =>
          vector.slice(dimensionsOffset, dimensionsOffset + dimensionsUsed))
        clusterCoordinator ! DataSize(data.length, ctx.self)
        // TODO distribute the data
        val localData = LocalData(data, 0)
        replyTo ! DataRef(localData)
        holdData(localData, dataHolders)
    }

  def holdData(data: LocalData[Float], dataHolders: Set[ActorRef[LoadDataEvent]]): Behavior[LoadDataEvent] =
    Behaviors.receiveMessagePartial {
      case ListingResponse(dataHolderServiceKey.Listing(listings)) =>
        ctx.log.info("{} dataHolders found (too late)", listings.size)
        holdData(data, dataHolders ++ listings)

      case GetAverageValue(replyTo) =>
        // TODO calculate average while loading data (as when in the cluster not all the data will be held here)
        val averageValue: Seq[Float] = (0 until data.dimension).map{ index =>
          data.data.map(value => value(index)).sum / data.localDataSize
        }
        replyTo ! AverageValue(averageValue)
        holdData(data, dataHolders)

      case ReadTestQueries(filename, replyTo) =>
        val queries = readQueries(filename)
        replyTo ! TestQueries(queries)
        loadData(dataHolders)

      case SaveGraphToFile(filename, graphHolders, k) =>
        // TODO create file so it can be appended with the graph
        val searchOnGraphEventAdapter: ActorRef[SearchOnGraph.SearchOnGraphEvent] =
          ctx.messageAdapter { event => WrappedSearchOnGraphEvent(event)}
        graphHolders.foreach(gh => gh ! GetGraph(searchOnGraphEventAdapter))
        saveToFile(filename, graphHolders, k, searchOnGraphEventAdapter, data, dataHolders)
    }

  def saveToFile(filename: String,
                 remainingGraphHolders: Set[ActorRef[SearchOnGraphEvent]],
                 k: Int,
                 searchOnGraphEventAdapter: ActorRef[SearchOnGraph.SearchOnGraphEvent],
                 data: LocalData[Float],
                 dataHolders: Set[ActorRef[LoadDataEvent]]): Behavior[LoadDataEvent] =
    Behaviors.receiveMessage {
      case WrappedSearchOnGraphEvent(event) =>
        event match {
          case Graph(graph, sender) =>
            // TODO open and append file with graph
            // Each g_node one line
            val updatedGraphHolders = remainingGraphHolders - sender
            if (updatedGraphHolders.isEmpty) {
              holdData(data, dataHolders)
            } else {
              saveToFile(filename, updatedGraphHolders, k, searchOnGraphEventAdapter, data, dataHolders)
            }

        }
    }


  // utility functions
  def readData(filename: String): Seq[Seq[Float]] = {
    // read dimensions for proper grouping
    val bis = new BufferedInputStream(new FileInputStream(filename))
    bis.mark(0)
    val dimArray: Array[Byte] = Array.fill(4){0}
    bis.read(dimArray)
    val dimensions = byteArrayToLittleEndianInt(dimArray)
    bis.reset()

    val data = LazyList.continually(bis.read).takeWhile(-1 !=).map(_.toByte).grouped(4).grouped(dimensions + 1).map{
      byteValues =>
        byteValues.slice(1, byteValues.length).map(value => byteArrayToLittleEndianFloat(value.toArray))
    }.toSeq
    data
  }

  // read Queries and indices of nearest neighbors
  def readQueries(filename: String): Seq[(Seq[Float], Seq[Int])] = {
    val bis = new BufferedInputStream(new FileInputStream(filename))
    val byteValues = LazyList.continually(bis.read).takeWhile(-1 !=).map(_.toByte).grouped(4).toSeq
    val dimensions = byteArrayToLittleEndianInt(byteValues.head.toArray)
    val k = byteArrayToLittleEndianInt(byteValues(dimensions + 1).toArray)
    val querySize = dimensions + 1 + k + 1
    val queries = byteValues.grouped(querySize).map{ byteQuery =>
      val query = byteQuery.slice(1, dimensions + 1).map(byteValue => byteArrayToLittleEndianFloat(byteValue.toArray))
      val neighbors = byteQuery.slice(dimensions + 2, byteQuery.length).map(byteValue => byteArrayToLittleEndianInt(byteValue.toArray))
      (query, neighbors)
    }
    queries.toSeq
  }

  def byteArrayToLittleEndianInt(bArray: Array[Byte]) : Int = {
    val bb: nio.ByteBuffer = ByteBuffer.wrap(bArray)
    bb.order(ByteOrder.LITTLE_ENDIAN)
    bb.getInt()
  }

  def byteArrayToLittleEndianFloat(bArray: Array[Byte]) : Float = {
    val bb: nio.ByteBuffer = ByteBuffer.wrap(bArray)
    bb.order(ByteOrder.LITTLE_ENDIAN)
    bb.getFloat()
  }

}
