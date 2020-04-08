package com.github.julkw.dnsg.actors

import java.io.{BufferedInputStream, FileInputStream}
import java.nio
import java.nio.ByteBuffer
import java.nio.ByteOrder

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.github.julkw.dnsg.actors.Coordinator.{AverageValue, CoordinationEvent, DataRef, TestQueries}
import com.github.julkw.dnsg.actors.SearchOnGraph.{GetGraph, Graph, SearchOnGraphEvent}

import scala.language.postfixOps

object DataHolder {

  sealed trait LoadDataEvent

  final case class LoadSiftDataFromFile(filename: String, replyTo: ActorRef[CoordinationEvent]) extends LoadDataEvent

  final case class LoadPartialDataFromFile(filename: String, lineOffset: Int, linesUsed: Int, dimensionsOffset: Int, dimensionsUsed: Int, replyTo: ActorRef[CoordinationEvent]) extends LoadDataEvent

  final case class GetAverageValue(replyTo: ActorRef[CoordinationEvent]) extends LoadDataEvent

  final case class ReadTestQueries(filename: String, replyTo: ActorRef[CoordinationEvent]) extends LoadDataEvent

  final case class SaveGraphToFile(filename: String, graphHolders: Set[ActorRef[SearchOnGraphEvent]], k: Int) extends LoadDataEvent
  // TODO in a cluster the indices would not be enough, so maybe one message per node including its location
  // unless the graph and the data remain in two different files

  final case class WrappedSearchOnGraphEvent(event: SearchOnGraph.SearchOnGraphEvent) extends LoadDataEvent

  var data : Seq[Seq[Float]] = Seq.empty[Seq[Float]]


  def apply(): Behavior[LoadDataEvent] = Behaviors.setup { ctx =>
    ctx.log.info("Started up DataHolder")
    Behaviors.receiveMessage {
      case LoadSiftDataFromFile(filename, replyTo) =>
        ctx.log.info("Asked to load SIFT data from {}", filename)
        readData(filename)
        replyTo ! DataRef(data)
        Behaviors.same

      // only return part of the data for testing (number of lines and dimensions need to be known beforehand for this)
      case LoadPartialDataFromFile(filename, lineOffset, linesUsed, dimensionsOffset, dimensionsUsed, replyTo) =>
        readData(filename)
        data = data.slice(lineOffset, lineOffset + linesUsed).map(vector =>
          vector.slice(dimensionsOffset, dimensionsOffset + dimensionsUsed))
        replyTo ! DataRef(data)
        Behaviors.same

      case GetAverageValue(replyTo) =>
        // TODO calculate average while loading data (as when in the cluster not all the data will be held here)
        val averageValue: Seq[Float] = (0 until data(0).length).map{ index =>
          data.map(value => value(index)).sum / data.length
        }
        replyTo ! AverageValue(averageValue)
        Behaviors.same

      case ReadTestQueries(filename, replyTo) =>
        val queries = readQueries(filename)
        replyTo ! TestQueries(queries)
        Behaviors.same

      case SaveGraphToFile(filename, graphHolders, k) =>
        // TODO create file so it can be appended with the graph
        val searchOnGraphEventAdapter: ActorRef[SearchOnGraph.SearchOnGraphEvent] =
          ctx.messageAdapter { event => WrappedSearchOnGraphEvent(event)}
        graphHolders.foreach(gh => gh ! GetGraph(searchOnGraphEventAdapter))
        saveToFile(filename, graphHolders, k, searchOnGraphEventAdapter)
    }
  }

  def saveToFile(filename: String,
                 remainingGraphHolders: Set[ActorRef[SearchOnGraphEvent]],
                 k: Int,
                 searchOnGraphEventAdapter: ActorRef[SearchOnGraph.SearchOnGraphEvent]): Behavior[LoadDataEvent] =
    Behaviors.receiveMessage {
      case WrappedSearchOnGraphEvent(event) =>
        event match {
          case Graph(graph, sender) =>
            // TODO open and append file with graph
            // Each g_node one line
            val updatedGraphHolders = remainingGraphHolders - sender
            saveToFile(filename, updatedGraphHolders, k, searchOnGraphEventAdapter)
        }
    }

  def readData(filename: String): Unit = {
    // read dimensions for proper grouping
    val bis = new BufferedInputStream(new FileInputStream(filename))
    bis.mark(0)
    val dimArray: Array[Byte] = Array.fill(4){0}
    bis.read(dimArray)
    val dimensions = byteArrayToLittleEndianInt(dimArray)
    bis.reset()

    data = LazyList.continually(bis.read).takeWhile(-1 !=).map(_.toByte).grouped(4).grouped(dimensions + 1).map{
      byteValues =>
        byteValues.slice(1, byteValues.length).map(value => byteArrayToLittleEndianFloat(value.toArray))
    }.toSeq
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
