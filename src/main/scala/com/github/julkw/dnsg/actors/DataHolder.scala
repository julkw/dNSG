package com.github.julkw.dnsg.actors

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.{AverageValue, CoordinationEvent, DataSize, GraphWrittenToFile, TestQueries}
import com.github.julkw.dnsg.actors.Coordinators.NodeCoordinator.{DataRef, NodeCoordinationEvent}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.{SearchOnGraphEvent, SendGraphForFile}
import com.github.julkw.dnsg.util.Data.{LocalData, LocalSequentialData, LocalUnorderedData}
import com.github.julkw.dnsg.util.{FileInteractor, LocalityCheck, NodeLocator, Settings, dNSGSerializable}

import scala.language.postfixOps

object DataHolder {

  sealed trait LoadDataEvent extends dNSGSerializable

  // load data
  // expects data in the .fvec format
  final case class LoadDataFromFile(filename: String, lines: Int, dataHolders: Set[ActorRef[LoadDataEvent]], clusterCoordinator: ActorRef[CoordinationEvent]) extends LoadDataEvent

  // share data
  final case class PrepareForData(dataSize: Int, offset: Int, allDataSize: Int, replyTo: ActorRef[LoadDataEvent], dataHolders: Set[ActorRef[LoadDataEvent]]) extends LoadDataEvent

  final case class PartialData(partialData: Seq[Seq[Float]], dataHolder: ActorRef[LoadDataEvent]) extends LoadDataEvent

  final case class GetNext(alreadyReceived: Int, dataHolder: ActorRef[LoadDataEvent]) extends LoadDataEvent

  final case class RedistributeData(primaryAssignments: NodeLocator[SearchOnGraphEvent], secondaryAssignments: Map[Int, Set[ActorRef[SearchOnGraphEvent]]]) extends LoadDataEvent

  final case class PrepareForRedistributedData(sender: ActorRef[LoadDataEvent]) extends LoadDataEvent

  final case class GetRedistributedData(sender: ActorRef[LoadDataEvent]) extends LoadDataEvent

  final case class PartialRedistributedData(data: Map[Int, Seq[Float]], sender: ActorRef[LoadDataEvent]) extends LoadDataEvent

  // other stuff
  final case class GetAverageValue(replyTo: ActorRef[CoordinationEvent]) extends LoadDataEvent

  final case class GetLocalAverage(replyTo: ActorRef[LoadDataEvent]) extends LoadDataEvent

  final case class LocalAverage(value: Seq[Float], elementsUsed: Int) extends LoadDataEvent

  final case class ReadTestQueries(queryFile: String, resultFile: String, replyTo: ActorRef[CoordinationEvent]) extends LoadDataEvent

  final case class SaveGraphToFile(filename: String, graphHolders: Set[ActorRef[SearchOnGraphEvent]], graphSize: Int, navigatingNode: Option[Int], replyTo: ActorRef[CoordinationEvent]) extends LoadDataEvent

  final case class GetGraphFromFile(filename: String) extends LoadDataEvent

  final case class GraphForFile(graph: Seq[(Int, Seq[Int])], sender: ActorRef[SearchOnGraphEvent], moreToSend: Boolean) extends LoadDataEvent


  def apply(nodeCoordinator: ActorRef[NodeCoordinationEvent]): Behavior[LoadDataEvent] = Behaviors.setup { ctx =>
    val maxMessageSize = Settings(ctx.system.settings.config).maxMessageSize
    new DataHolder(nodeCoordinator, maxMessageSize, ctx).loadData()
  }
}

class DataHolder(nodeCoordinator: ActorRef[NodeCoordinationEvent], maxMessageSize: Int, ctx: ActorContext[DataHolder.LoadDataEvent]) extends FileInteractor with LocalityCheck {
  import DataHolder._

  def loadData(): Behavior[LoadDataEvent] =
    Behaviors.receiveMessagePartial {
      case LoadDataFromFile(filename, lines, dataHolders, clusterCoordinator) =>
        val data = readDataFloat(filename, lines)
        clusterCoordinator ! DataSize(data.length, filename, ctx.self)
        if (dataHolders.size == 1) {
          // I am the only node in the cluster
          val localData = LocalSequentialData(data, 0)
          nodeCoordinator ! DataRef(localData, localData.localDataSize)
          holdData(localData, dataHolders)
        } else {
          startDistributingData(data, dataHolders)
        }

      case PrepareForData(dataSize, offset, allDataSize, replyTo, dataHolders) =>
        replyTo ! GetNext(0, ctx.self)
        receiveData(Seq.empty, dataSize, offset, allDataSize, pointsReceived = 0, dataHolders)
    }

  def startDistributingData(data: Seq[Seq[Float]], dataHolders: Set[ActorRef[LoadDataEvent]]): Behavior[LoadDataEvent] = {
    val partitionInfo = calculatePartitionInfo(data.length, dataHolders)
    partitionInfo.foreach { case (dataHolder, pInfo) =>
      if (dataHolder != ctx.self) {
        dataHolder ! PrepareForData(pInfo._2, pInfo._1, data.length, ctx.self, dataHolders)
      }
    }
    // max number of datapoints to send per message
    val dataMessageSize = maxMessageSize / data.head.length
    shareData(data, partitionInfo, dataMessageSize, dataHolders)
  }

  def shareData(data: Seq[Seq[Float]],
                partitionInfo: Map[ActorRef[LoadDataEvent], (Int, Int)],
                dataMessageSize: Int,
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
            val localOffset = remainingPartitionInfo(ctx.self)._1
            val localDataSize = remainingPartitionInfo(ctx.self)._2
            val localData = LocalSequentialData(data.slice(localOffset, localOffset + localDataSize), localOffset)
            nodeCoordinator ! DataRef(localData, data.length)
            holdData(localData, dataHolders)
          } else {
            shareData(data, remainingPartitionInfo, dataMessageSize, dataHolders)
          }
        } else {
          shareData(data, partitionInfo, dataMessageSize, dataHolders)
        }
    }

  def receiveData(data: Seq[Seq[Float]],
                  expectedDataSize: Int,
                  localOffset: Int,
                  allDataSize: Int,
                  pointsReceived: Int,
                  dataHolders: Set[ActorRef[LoadDataEvent]]): Behavior[LoadDataEvent] =
    Behaviors.receiveMessagePartial{
      case PartialData(partialData, dataHolder) =>
        val updatedData: Seq[Seq[Float]] = data ++ partialData

        val updatedPointsReceived = pointsReceived + partialData.length
        if (updatedPointsReceived == expectedDataSize) {
          // all data received
          val localData: LocalData[Float] = LocalSequentialData(updatedData, localOffset)
          nodeCoordinator ! DataRef(localData, allDataSize)
          holdData(localData, dataHolders)
        } else {
          dataHolder ! GetNext(updatedPointsReceived, ctx.self)
          receiveData(updatedData, expectedDataSize, localOffset, allDataSize, updatedPointsReceived, dataHolders)
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
          data.rawData.map(value => value(index)).sum / data.localDataSize
        }
        if (dataHolders.size > 1) {
          calculateAverages(data, dataHolders, averageValue, data.localDataSize, dataHolders.size - 1, replyTo)
        } else {
          replyTo ! AverageValue(averageValue)
          holdData(data, dataHolders)
        }

      case GetLocalAverage(replyTo) =>
        val averageValue: Seq[Float] = (0 until data.dimension).map{ index =>
          data.rawData.map(value => value(index)).sum / data.localDataSize
        }
        replyTo ! LocalAverage(averageValue, data.localDataSize)
        holdData(data, dataHolders)

      case RedistributeData(primaryAssignments, secondaryAssignments) =>
        val expectedDataSize = (0 until primaryAssignments.graphSize).count { nodeIndex =>
          isLocal(primaryAssignments.findResponsibleActor(nodeIndex)) ||
            secondaryAssignments.getOrElse(nodeIndex, Set.empty).exists(worker => isLocal(worker))
        }
        val toSend = data.localIndices.groupBy(index => primaryAssignments.findResponsibleActor(index))
        val sendToDHs = dataHolders.map { dataHolder =>
          val correspondingWorkers = toSend.keys.filter(actor => actor.path.root == dataHolder.path.root)
          val sendToDH = correspondingWorkers.flatMap { worker =>
            toSend(worker) ++ secondaryAssignments.keys.filter ( index => data.isLocal(index) && secondaryAssignments(index).contains(worker))
          }
          dataHolder -> sendToDH.toSeq
        }.toMap
        sendToDHs.keys.foreach(dataHolder => dataHolder ! PrepareForRedistributedData(ctx.self))
        val dataMessageSize = maxMessageSize / data.dimension
        redistributeData(data, Map.empty, expectedDataSize, sendToDHs, primaryAssignments.graphSize, dataMessageSize, dataHolders)

      case PrepareForRedistributedData(sender) =>
        ctx.self ! PrepareForRedistributedData(sender)
        holdData(data, dataHolders)

      case ReadTestQueries(queryFile, resultFile, replyTo) =>
        val queries = readQueries(queryFile, resultFile)
        replyTo ! TestQueries(queries)
        holdData(data, dataHolders)

      case SaveGraphToFile(filename, graphHolders, graphSize, navigatingNode, replyTo) =>
        graphHolders.foreach(gh => gh ! SendGraphForFile(ctx.self))
        prepareGraphFile(filename, navigatingNode)
        saveToFile(filename, graphSize, data, dataHolders, replyTo)

      case GetGraphFromFile(filename) =>
        readGraphFromFile(filename)
        // TODO distribute Graph (and potentially data?) for further testing
        holdData(data, dataHolders)
    }

  def redistributeData(oldData: LocalData[Float],
                       newData: Map[Int, Seq[Float]],
                       expectedNewData: Int,
                       toSend: Map[ActorRef[LoadDataEvent], Seq[Int]],
                       allDataSize: Int,
                       dataMessageSize: Int,
                       dataHolders: Set[ActorRef[LoadDataEvent]]): Behavior[LoadDataEvent] =
    Behaviors.receiveMessagePartial {
      case PrepareForRedistributedData(sender) =>
        sender ! GetRedistributedData(ctx.self)
        redistributeData(oldData, newData, expectedNewData, toSend, allDataSize, dataMessageSize, dataHolders)

      case GetRedistributedData(sender) =>
        if (toSend.contains(sender)) {
          val indicesToSend = toSend(sender)
          if (indicesToSend.nonEmpty) {
            val dataToSend = indicesToSend.slice(0, dataMessageSize).map(index => index -> oldData.get(index)).toMap
            sender ! PartialRedistributedData(dataToSend, ctx.self)
            redistributeData(oldData, newData, expectedNewData, toSend + (sender -> indicesToSend.slice(dataMessageSize, indicesToSend.length)), allDataSize, dataMessageSize, dataHolders)
          } else if (toSend.size == 1 && newData.size == expectedNewData) {
            val localData = LocalUnorderedData(newData)
            nodeCoordinator ! DataRef(localData, 0)
            holdData(localData, dataHolders)
          } else {
            redistributeData(oldData, newData, expectedNewData, toSend - sender, allDataSize, dataMessageSize, dataHolders)
          }
        } else {
          sender ! PartialRedistributedData(Map.empty, ctx.self)
          redistributeData(oldData, newData, expectedNewData, toSend, allDataSize, dataMessageSize, dataHolders)
        }

      case PartialRedistributedData(data, sender) =>
        sender ! GetRedistributedData(ctx.self)
        val updatedData = newData ++ data
        if (updatedData.size == expectedNewData && toSend.isEmpty) {
          val localData = LocalUnorderedData(updatedData)
          nodeCoordinator ! DataRef(localData, allDataSize)
          holdData(localData, dataHolders)
        } else {
          redistributeData(oldData, updatedData, expectedNewData, toSend, allDataSize, dataMessageSize, dataHolders)
        }
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
                 waitingForGraphNodes: Int,
                 data: LocalData[Float],
                 dataHolders: Set[ActorRef[LoadDataEvent]],
                 replyTo: ActorRef[CoordinationEvent]): Behavior[LoadDataEvent] =
    Behaviors.receiveMessagePartial {
      case GraphForFile(graph, sender, moreToSend) =>
        ctx.log.info("Received {} nodes of graph to write into file", graph.length)
        writeGraphToFile(filename, graph)
        val updatedWaitingForNodes = waitingForGraphNodes - graph.length
        if (updatedWaitingForNodes == 0) {
          replyTo ! GraphWrittenToFile
          holdData(data, dataHolders)
        } else {
          if (moreToSend) {
            sender ! SendGraphForFile(ctx.self)
          }
          saveToFile(filename, updatedWaitingForNodes, data, dataHolders, replyTo)
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
}
