package com.github.julkw.dnsg.util

import java.io.{BufferedInputStream, BufferedOutputStream, File, FileInputStream, FileOutputStream, PrintWriter}
import java.nio
import java.nio.{ByteBuffer, ByteOrder}

import scala.language.postfixOps

trait FileInteractor {
  def readDataFloat(filename: String, lines: Int): Array[Array[Float]] = {
    // read dimensions for proper grouping
    val bis = new BufferedInputStream(new FileInputStream(filename))
    bis.mark(0)
    val dimArray: Array[Byte] = Array.fill(4){0}
    bis.read(dimArray)
    val dimensions = byteArrayToLittleEndianInt(dimArray)
    bis.reset()

    val maxLines = if (lines > 0) { lines } else { 1 }
    val limitLines = if (lines > 0) { 1 } else { 0 }
    var currentLine = 0
    val data = LazyList.continually(bis.read).takeWhile(input => currentLine < maxLines && input != -1).map(_.toByte).grouped(4).grouped(dimensions + 1).map{
      byteValues =>
        val vector = byteValues.slice(1, byteValues.length).map(value => byteArrayToLittleEndianFloat(value.toArray))
        currentLine += limitLines
        vector.toArray
    }.toArray
    data
  }

  def readDataInt(filename: String): Seq[Seq[Int]] = {
    // read dimensions for proper grouping
    val bis = new BufferedInputStream(new FileInputStream(filename))
    bis.mark(0)
    val dimArray: Array[Byte] = Array.fill(4){0}
    bis.read(dimArray)
    val dimensions = byteArrayToLittleEndianInt(dimArray)
    bis.reset()

    val data = LazyList.continually(bis.read).takeWhile(-1 !=).map(_.toByte).grouped(4).grouped(dimensions + 1).map{
      byteValues =>
        byteValues.slice(1, byteValues.length).map(value => byteArrayToLittleEndianInt(value.toArray))
    }.toSeq
    data
  }

  def readGraphFromFile(filename: String): (Map[Int, Seq[Int]], Option[Int]) = {
    val bis = new BufferedInputStream(new FileInputStream(filename))
    val data = LazyList.continually(bis.read).takeWhile(-1 !=).map(_.toByte).grouped(4).map { bytes =>
      byteArrayToLittleEndianInt(bytes.toArray)
    }.toSeq
    val navNode = data.head
    var graph: Map[Int, Seq[Int]] = Map.empty
    var currentNode = -1
    var neighborsStart = 0
    var neighborsEnd = 0
    var currentIndex = 0
    data.foreach { number =>
      if (currentIndex == neighborsStart - 1) {
        neighborsEnd = neighborsStart + number - 1
        graph += (currentNode -> data.slice(neighborsStart, neighborsEnd))
      } else if (currentIndex > neighborsEnd) {
        currentNode = number
        neighborsStart = currentIndex + 2
      }
      currentIndex += 1
    }
    val actualNavNode = if (navNode < 0) {None} else {Some(navNode)}
    (graph, actualNavNode)
  }

  def prepareGraphFile(filename: String, navigatingNode: Option[Int]): Unit = {
    val graphFile = new File(filename)
    if (!graphFile.createNewFile()) {
      val writer = new PrintWriter(filename)
      writer.print("")
      writer.close()
    }
    // start graph with navigating node. If it isn't an NSG put -1
    val navNode = if (navigatingNode.nonEmpty) {navigatingNode.get} else {-1}
    val writer = new FileOutputStream(filename)
    writer.write(intToLittleEndianArray(navNode))
    writer.close()
  }

  def writeGraphToFile(filename: String, graph: Seq[(Int, Seq[Int])]): Unit = {
    val bw = new BufferedOutputStream(new FileOutputStream(filename, true))
    graph.foreach { case (index, neighbors) =>
      bw.write(intToLittleEndianArray(index))
      bw.write(intToLittleEndianArray(neighbors.length))
      neighbors.foreach(neighbor => bw.write(intToLittleEndianArray(neighbor)))
    }
    bw.close()
  }

  // read Queries and indices of nearest neighbors
  def readQueries(queryFile: String, resultFile: String): Seq[(Array[Float], Seq[Int])] = {
    val queries = readDataFloat(queryFile, lines = 0)
    val queryResults = readDataInt(resultFile)
    queries.zip(queryResults)
  }

  def intToLittleEndianArray(i: Int): Array[Byte] = {
    val buffer: ByteBuffer = ByteBuffer.allocate(4)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    buffer.putInt(i)
    buffer.array()
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
