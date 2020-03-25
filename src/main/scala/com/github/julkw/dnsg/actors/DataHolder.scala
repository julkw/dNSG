package com.github.julkw.dnsg.actors

import java.io.{BufferedInputStream, FileInputStream}
import java.nio
import java.nio.ByteBuffer
import java.nio.ByteOrder

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.github.julkw.dnsg.actors.Coordinator.{AverageValue, CoordinationEvent, DataRef}

import scala.language.postfixOps

object DataHolder {

  sealed trait LoadDataEvent
  final case class LoadSiftDataFromFile(filename: String, replyTo: ActorRef[CoordinationEvent]) extends LoadDataEvent
  final case class GetAverageValue(replyTo: ActorRef[CoordinationEvent]) extends LoadDataEvent

  var data : Seq[Seq[Float]] = Seq.empty[Seq[Float]]


  def apply(): Behavior[LoadDataEvent] = Behaviors.setup { ctx =>
    ctx.log.info("Started up DataHolder")
    Behaviors.receiveMessage { message =>
      message match {
        case LoadSiftDataFromFile(filename, replyTo) =>
          ctx.log.info("Asked to load SIFT data from {}", filename)

          val bis = new BufferedInputStream(new FileInputStream(filename))
          val bArray = LazyList.continually(bis.read).takeWhile(-1 !=).map(_.toByte).toArray

          val dimensions = byteArrayToLittleEndianInt(bArray.slice(0, 4))
          ctx.log.info("Dimensions: {}", dimensions)

          val vectorSize = (dimensions + 1) * 4
          val vectors = bArray.length / vectorSize

          ctx.log.info("The number of vectors is: {}", vectors)
          for (vector <- 0 until vectors) {
            val vectorStart = vector * vectorSize + 4
            var valueVec : Seq[Float] = Seq.empty
            for (dim <- 0 until dimensions) {
              val valueStart = vectorStart + dim * 4
              val value = byteArrayToLittleEndianFloat(bArray.slice(valueStart, valueStart + 4))
              valueVec = valueVec :+ value
            }
            data = data :+ valueVec
          }
          // TODO remove again, this is just for faster debugging
          data = data.slice(0, 1000)
          replyTo ! DataRef(data)

        case GetAverageValue(replyTo) =>
          // TODO calculate average while loading data (as when in the cluster not all the data will be held here)
          val averageValue: Seq[Float] = (0 until data(0).length).map{ index =>
            data.map(value => value(index)).sum / data.length
          }
          replyTo ! AverageValue(averageValue)
      }
      Behaviors.same
    }
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
