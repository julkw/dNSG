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
import com.github.julkw.dnsg.actors.SearchOnGraphActor.{GetGraph, Graph, SearchOnGraphEvent}
import com.github.julkw.dnsg.util.{LocalData, dNSGSerializable}

import scala.language.postfixOps

object DataHolder {

  sealed trait LoadDataEvent extends dNSGSerializable

  // load data
  final case class LoadSiftDataFromFile(expectedNodes: Int, filename: String, replyTo: ActorRef[NodeCoordinationEvent], clusterCoordinator: ActorRef[CoordinationEvent]) extends LoadDataEvent

  final case class LoadPartialDataFromFile(expectedNodes: Int, filename: String, lineOffset: Int, linesUsed: Int, dimensionsOffset: Int, dimensionsUsed: Int, replyTo: ActorRef[NodeCoordinationEvent], clusterCoordinator: ActorRef[CoordinationEvent]) extends LoadDataEvent

  // share data
  final case class PrepareForData(dataSize: Int, offset: Int, replyTo: ActorRef[LoadDataEvent]) extends LoadDataEvent

  final case class PartialData(partialData: Seq[Seq[Float]], dataHolder: ActorRef[LoadDataEvent]) extends LoadDataEvent

  final case class GetNext(alreadyReceived: Int, dataHolder: ActorRef[LoadDataEvent]) extends LoadDataEvent

  // other stuff
  final case class GetAverageValue(replyTo: ActorRef[CoordinationEvent]) extends LoadDataEvent

  final case class GetLocalAverage(replyTo: ActorRef[LoadDataEvent]) extends LoadDataEvent

  final case class LocalAverage(value: Seq[Float], elementsUsed: Int) extends LoadDataEvent

  final case class ReadTestQueries(filename: String, replyTo: ActorRef[CoordinationEvent]) extends LoadDataEvent

  // TODO this message is not really being handled at the moment
  final case class SaveGraphToFile(filename: String, graphHolders: Set[ActorRef[SearchOnGraphEvent]], k: Int) extends LoadDataEvent

  final case class WrappedSearchOnGraphEvent(event: SearchOnGraphActor.SearchOnGraphEvent) extends LoadDataEvent

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

      case LoadSiftDataFromFile(expectedNodes, filename, replyTo, clusterCoordinator) =>
        ctx.log.info("Asked to load SIFT data from {}", filename)
        // TODO potentially send while reading for really big dataSets
        val data = readData(filename)
        clusterCoordinator ! DataSize(data.length, ctx.self)
        if (expectedNodes == 1) {
          // I am the only node in the cluster
          val localData = LocalData(data, 0)
          nodeCoordinator ! DataRef(localData)
          holdData(localData, dataHolders)
        } else if (dataHolders.size < expectedNodes) {
          ctx.log.info("Waiting on other nodes before distributing data")
          waitForDataHolders(data, expectedNodes, dataHolders)
        } else {
          startDistributingData(data, dataHolders)
        }

      // only return part of the data for testing (number of lines and dimensions need to be known beforehand for this)
      case LoadPartialDataFromFile(expectedNodes, filename, lineOffset, linesUsed, dimensionsOffset, dimensionsUsed, replyTo, clusterCoordinator) =>
        val data = readData(filename).slice(lineOffset, lineOffset + linesUsed).map(vector =>
          vector.slice(dimensionsOffset, dimensionsOffset + dimensionsUsed))
        clusterCoordinator ! DataSize(data.length, ctx.self)
        if (expectedNodes == 1) {
          // I am the only node in the cluster
          val localData = LocalData(data, 0)
          nodeCoordinator ! DataRef(localData)
          holdData(localData, dataHolders)
        } else if (dataHolders.size < expectedNodes) {
          ctx.log.info("Waiting on other nodes before distributing data")
          waitForDataHolders(data, expectedNodes, dataHolders)
        } else {
          startDistributingData(data, dataHolders)
        }

      case PrepareForData(dataSize, offset, replyTo) =>
        ctx.log.info("Receiving data from other dataHolder")
        replyTo ! GetNext(0, ctx.self)
        receiveData(Seq.empty, dataSize, offset, 0, dataHolders)
    }

  def waitForDataHolders(data: Seq[Seq[Float]], expectedNodes: Int, dataHolders: Set[ActorRef[LoadDataEvent]]): Behavior[LoadDataEvent] =
    Behaviors.receiveMessagePartial {
      case ListingResponse(dataHolderServiceKey.Listing(listings)) =>
        ctx.log.info("{} dataHolders found", listings.size)
        if (expectedNodes < listings.size) {
          waitForDataHolders(data, expectedNodes, listings)
        } else {
          startDistributingData(data, listings)
        }
    }

  def startDistributingData(data: Seq[Seq[Float]], dataHolders: Set[ActorRef[LoadDataEvent]]): Behavior[LoadDataEvent] = {
    val partitionInfo = calculatePartitionInfo(data.length, dataHolders)
    partitionInfo.foreach { case (dataHolder, pInfo) =>
      if (dataHolder != ctx.self) {
        dataHolder ! PrepareForData(pInfo._2, pInfo._1, ctx.self)
      }
    }
    shareData(data, partitionInfo, dataHolders)
  }

  def shareData(data: Seq[Seq[Float]],
                partitionInfo: Map[ActorRef[LoadDataEvent], (Int, Int)],
                dataHolders: Set[ActorRef[LoadDataEvent]]): Behavior[LoadDataEvent] =
    Behaviors.receiveMessagePartial {
      case GetNext(alreadyReceived, dataHolder) =>
        val offset = partitionInfo(dataHolder)._1 + alreadyReceived
        val partitionDataSize = partitionInfo(dataHolder)._2
        val amountToSend = math.min(partitionDataSize - alreadyReceived, dataMessageSize)
        val dataToSend = data.slice(offset, offset + amountToSend)
        dataHolder ! PartialData(dataToSend, ctx.self)
        // check if done with this (and maybe all) actor(s)
        if (alreadyReceived + amountToSend == partitionDataSize) {
          // all data has been sent to this actor
          val remainingPartitionInfo = partitionInfo - dataHolder
          if (remainingPartitionInfo.size == 1) {
            ctx.log.info("Done distributing data")
            val localOffset = remainingPartitionInfo(ctx.self)._1
            val localDataSize = remainingPartitionInfo(ctx.self)._2
            val localData = LocalData(data.slice(localOffset, localDataSize), localOffset)
            nodeCoordinator ! DataRef(localData)
            holdData(localData, dataHolders)
          } else {
            shareData(data, remainingPartitionInfo, dataHolders)
          }
        } else {
          shareData(data, partitionInfo, dataHolders)
        }
    }

  def receiveData(data: Seq[Seq[Float]],
                  expectedDataSize: Int,
                  localOffset: Int,
                  pointsReceived: Int,
                  dataHolders: Set[ActorRef[LoadDataEvent]]): Behavior[LoadDataEvent] =
    Behaviors.receiveMessagePartial{
      case PartialData(partialData, dataHolder) =>
        val updatedData: Seq[Seq[Float]] = data ++ partialData

        val updatedPointsReceived = pointsReceived + partialData.length
        if (updatedPointsReceived == expectedDataSize) {
          // all data received
          val localData: LocalData[Float] = LocalData(updatedData, localOffset)
          nodeCoordinator ! DataRef(localData)
          ctx.log.info("got all my data")
          holdData(localData, dataHolders)
        } else {
          dataHolder ! GetNext(updatedPointsReceived, ctx.self)
          receiveData(updatedData, expectedDataSize, localOffset, updatedPointsReceived, dataHolders)
        }
    }

  def holdData(data: LocalData[Float], dataHolders: Set[ActorRef[LoadDataEvent]]): Behavior[LoadDataEvent] =
    Behaviors.receiveMessagePartial {
      case GetAverageValue(replyTo) =>
        dataHolders.foreach { dataHolder =>
          if (dataHolder != ctx.self) {
            dataHolder ! GetLocalAverage(ctx.self)
          }
        }
        val averageValue: Seq[Float] = (0 until data.dimension).map{ index =>
          data.data.map(value => value(index)).sum / data.localDataSize
        }
        if (dataHolders.size > 1) {
          calculateAverages(data, dataHolders, averageValue, data.localDataSize, dataHolders.size - 1, replyTo)
        } else {
          replyTo ! AverageValue(averageValue)
          holdData(data, dataHolders)
        }

      case GetLocalAverage(replyTo) =>
        val averageValue: Seq[Float] = (0 until data.dimension).map{ index =>
          data.data.map(value => value(index)).sum / data.localDataSize
        }
        replyTo ! LocalAverage(averageValue, data.localDataSize)
        holdData(data, dataHolders)

      case ReadTestQueries(filename, replyTo) =>
        val queries = readQueries(filename)
        replyTo ! TestQueries(queries)
        loadData(dataHolders)

      case SaveGraphToFile(filename, graphHolders, k) =>
        // TODO create file so it can be appended with the graph
        val searchOnGraphEventAdapter: ActorRef[SearchOnGraphActor.SearchOnGraphEvent] =
          ctx.messageAdapter { event => WrappedSearchOnGraphEvent(event)}
        graphHolders.foreach(gh => gh ! GetGraph(searchOnGraphEventAdapter))
        saveToFile(filename, graphHolders, k, searchOnGraphEventAdapter, data, dataHolders)
    }

  def calculateAverages(data: LocalData[Float],
                        dataHolders: Set[ActorRef[LoadDataEvent]],
                        currentAverage: Seq[Float],
                        currentElements: Int,
                        awaitingAnswers: Int,
                        sendResultTo: ActorRef[CoordinationEvent]): Behavior[LoadDataEvent] =
    Behaviors.receiveMessagePartial {
      case LocalAverage(value, elementsUsed) =>
        val updatedElements = currentElements + elementsUsed
        val oldMultiplier = currentElements.toFloat / updatedElements
        val newMultiplier = elementsUsed.toFloat / updatedElements
        val updatedAverage = currentAverage.zip(value).map { case (oldValue, newValue) =>
          oldValue * oldMultiplier + newValue * newMultiplier
        }
        if (awaitingAnswers == 1) {
          sendResultTo ! AverageValue(updatedAverage)
          holdData(data, dataHolders)
        } else {
          calculateAverages(data, dataHolders, updatedAverage, updatedElements, awaitingAnswers - 1, sendResultTo)
        }
    }

  def saveToFile(filename: String,
                 remainingGraphHolders: Set[ActorRef[SearchOnGraphEvent]],
                 k: Int,
                 searchOnGraphEventAdapter: ActorRef[SearchOnGraphActor.SearchOnGraphEvent],
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
