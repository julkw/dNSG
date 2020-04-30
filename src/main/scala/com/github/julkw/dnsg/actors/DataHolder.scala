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
import com.github.julkw.dnsg.util.{LocalData, dNSGSerializable}

import scala.language.postfixOps

object DataHolder {

  sealed trait LoadDataEvent extends dNSGSerializable

  // load data
  final case class LoadSiftDataFromFile(filename: String, replyTo: ActorRef[NodeCoordinationEvent], clusterCoordinator: ActorRef[CoordinationEvent]) extends LoadDataEvent

  final case class LoadPartialDataFromFile(filename: String, lineOffset: Int, linesUsed: Int, dimensionsOffset: Int, dimensionsUsed: Int, replyTo: ActorRef[NodeCoordinationEvent], clusterCoordinator: ActorRef[CoordinationEvent]) extends LoadDataEvent

  // share data
  final case class PrepareForData(dataSize: Int, offset: Int, replyTo: ActorRef[LoadDataEvent]) extends LoadDataEvent

  final case class ReadyForData(sendDataTo: ActorRef[LoadDataEvent]) extends LoadDataEvent

  final case class PartialData(partialData: Seq[Seq[Float]], startIndex: Int) extends LoadDataEvent

  // other stuff
  final case class GetAverageValue(replyTo: ActorRef[CoordinationEvent]) extends LoadDataEvent

  final case class ReadTestQueries(filename: String, replyTo: ActorRef[CoordinationEvent]) extends LoadDataEvent

  // TODO this message is not really being handled at the moment
  final case class SaveGraphToFile(filename: String, graphHolders: Set[ActorRef[SearchOnGraphEvent]], k: Int) extends LoadDataEvent

  final case class WrappedSearchOnGraphEvent(event: SearchOnGraph.SearchOnGraphEvent) extends LoadDataEvent

  val dataHolderServiceKey: ServiceKey[DataHolder.LoadDataEvent] = ServiceKey[LoadDataEvent]("dataService")

  private case class ListingResponse(listing: Receptionist.Listing) extends LoadDataEvent

  val dataMessageSize = 100

  def apply(nodeCoordinator: ActorRef[NodeCoordinationEvent]): Behavior[LoadDataEvent] = Behaviors.setup { ctx =>
    ctx.log.info("Started up DataHolder")

    val listingResponseAdapter = ctx.messageAdapter[Receptionist.Listing](ListingResponse)

    new DataHolder(nodeCoordinator, ctx, listingResponseAdapter).setup()
  }
}

class DataHolder(nodeCoordinator: ActorRef[NodeCoordinationEvent], ctx: ActorContext[DataHolder.LoadDataEvent], listingAdapter: ActorRef[Receptionist.Listing]) {
  import DataHolder._

  def setup(): Behavior[LoadDataEvent] = {
    ctx.system.receptionist ! Receptionist.Register(dataHolderServiceKey, ctx.self)
    ctx.system.receptionist ! Receptionist.Subscribe(dataHolderServiceKey, listingAdapter)
    loadData(Set.empty)
  }

  def loadData(dataHolders: Set[ActorRef[LoadDataEvent]]): Behavior[LoadDataEvent] =
    Behaviors.receiveMessagePartial {
      case ListingResponse(dataHolderServiceKey.Listing(listings)) =>
        ctx.log.info("{} dataHolders found", listings.size)
        loadData(listings)

      case LoadSiftDataFromFile(filename, replyTo, clusterCoordinator) =>
        ctx.log.info("Asked to load SIFT data from {}", filename)
        // TODO potentially send while reading for really big dataSets
        val data = readData(filename)
        clusterCoordinator ! DataSize(data.length, ctx.self)
        if (dataHolders.size <= 1) {
          // I am the only node in the cluster
          val localData = LocalData(data, 0)
          nodeCoordinator ! DataRef(localData)
          holdData(localData, dataHolders)
        } else {
          val partitionInfo = calculatePartitionInfo(data.length, dataHolders)
          partitionInfo.foreach { case (dataHolder, pInfo) if dataHolder != ctx.self =>
            dataHolder ! PrepareForData(pInfo._2, pInfo._1, ctx.self)
          }
          shareData(data, partitionInfo, dataHolders)
        }

      // only return part of the data for testing (number of lines and dimensions need to be known beforehand for this)
      case LoadPartialDataFromFile(filename, lineOffset, linesUsed, dimensionsOffset, dimensionsUsed, replyTo, clusterCoordinator) =>
        val data = readData(filename).slice(lineOffset, lineOffset + linesUsed).map(vector =>
          vector.slice(dimensionsOffset, dimensionsOffset + dimensionsUsed))
        clusterCoordinator ! DataSize(data.length, ctx.self)
        if (dataHolders.size <= 1) {
          // I am the only node in the cluster
          ctx.log.info("Only node in cluster, not sharing data")
          val localData = LocalData(data, 0)
          nodeCoordinator ! DataRef(localData)
          holdData(localData, dataHolders)
        } else {
          val partitionInfo = calculatePartitionInfo(data.length, dataHolders)
          partitionInfo.foreach { case (dataHolder, pInfo) if dataHolder != ctx.self =>
            dataHolder ! PrepareForData(pInfo._2, pInfo._1, ctx.self)
          }
          shareData(data, partitionInfo, dataHolders)
        }

      case PrepareForData(dataSize, offset, replyTo) =>
        ctx.log.info("Receiving data from other dataHolder")
        replyTo ! ReadyForData(ctx.self)
        receiveData(Seq.empty, dataSize, offset, 0, dataHolders)
    }

  def shareData(data: Seq[Seq[Float]],
                partitionInfo: Map[ActorRef[LoadDataEvent], (Int, Int)],
                dataHolders: Set[ActorRef[LoadDataEvent]]): Behavior[LoadDataEvent] =
    Behaviors.receiveMessagePartial {
      case ReadyForData(dataHolder) =>
        val offset = partitionInfo(dataHolder)._1
        val dataSize = partitionInfo(dataHolder)._2
        var pdIndex = 0
        // TODO can I send them all at once like this or do I have to do an extra step of the other asking for more after dealing with a single message?
        data.slice(offset, dataSize).grouped(dataMessageSize).foreach { partialData =>
          val partialDataOffset = offset + dataMessageSize * pdIndex
          pdIndex += 1
          dataHolder ! PartialData(partialData, partialDataOffset)
        }
        val remainingPartitionInfo = partitionInfo - dataHolder
        if (remainingPartitionInfo == 1) {
          // only myself left
          val localOffset = remainingPartitionInfo(ctx.self)._1
          val localDataSize = remainingPartitionInfo(ctx.self)._2
          val localData = LocalData(data.slice(localOffset, localDataSize), localOffset)
          nodeCoordinator ! DataRef(localData)
          holdData(localData, dataHolders)
        } else {
          shareData(data, partitionInfo - dataHolder, dataHolders)
        }
    }

  def receiveData(data: Seq[(Int, Seq[Seq[Float]])],
                  expectedDataSize: Int,
                  localOffset: Int,
                  pointsReceived: Int,
                  dataHolders: Set[ActorRef[LoadDataEvent]]): Behavior[LoadDataEvent] =
    Behaviors.receiveMessagePartial{
      case PartialData(partialData, partialDataOffset) =>
        val updatedData = data :+ (partialDataOffset, partialData)
        val updatedPointsReceived = pointsReceived + partialData.length
        if (updatedPointsReceived == expectedDataSize) {
          // all data received
          val combinedData = data.sortBy(_._1).map(_._2).reduce(_ ++ _)
          val localData = LocalData(combinedData, localOffset)
          nodeCoordinator ! DataRef(localData)
          ctx.log.info("got all my data")
          holdData(localData, dataHolders)
        } else {
          receiveData(updatedData, expectedDataSize, localOffset, updatedPointsReceived, dataHolders)
        }
    }

  def holdData(data: LocalData[Float], dataHolders: Set[ActorRef[LoadDataEvent]]): Behavior[LoadDataEvent] =
    Behaviors.receiveMessagePartial {
      case ListingResponse(dataHolderServiceKey.Listing(listings)) =>
        // TODO prevent this from happening
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

  def calculatePartitionInfo(dataSize: Int, dataHolders: Set[ActorRef[LoadDataEvent]]): Map[ActorRef[LoadDataEvent], (Int, Int)] = {
    val dataPerNode = math.ceil(dataSize / dataHolders.size).toInt
    var index = 0
    val partitionInfo = dataHolders.map { dataHolder =>
      val offset = index * dataPerNode
      index += 1
      val partitionDataSize = math.min(dataPerNode, dataSize - offset)
      dataHolder -> (offset, partitionDataSize)
    }.toMap
    partitionInfo
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
