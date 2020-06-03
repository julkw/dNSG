package com.github.julkw.dnsg.util.Data

trait LocalData[T] {
  def get(globalIndex: Int): Seq[T]
  def isLocal(globalIndex: Int): Boolean
  def localDataSize: Int
  def dimension: Int
  // TODO this returns the global indices for the local data. Maybe rename as this might get confusing
  def localIndices: Seq[Int]
  def rawData: Seq[Seq[T]]
}


// this class let's the local data pretend to be global, so the workers can continue working with global indices
case class LocalSequentialData[T] (data: Seq[Seq[T]], localOffset: Int) extends LocalData[T] {
  protected val permanentDataSize: Int = data.length

  def get(globalIndex: Int): Seq[T] = {
    // this assumes, that whoever called this method checked isLocal first
    val index = localIndex(globalIndex)
    data(index)
  }

  def rawData: Seq[Seq[T]] = {
    data
  }

  def isLocal(globalIndex: Int): Boolean = {
    val index = localIndex(globalIndex)
    0 <= index && index < permanentDataSize
  }

  def localDataSize: Int = {
    permanentDataSize
  }

  def dimension: Int = {
    data(0).length
  }

  def localIndices: Seq[Int] = {
    localOffset until localOffset + permanentDataSize
  }

  protected def localIndex(globalIndex: Int): Int = {
    globalIndex - localOffset
  }
}

case class LocalUnorderedData[T] (data: Map[Int, Seq[T]]) extends LocalData[T] {
  protected val permanentDataSize: Int = data.size

  def get(globalIndex: Int): Seq[T] = {
    // this assumes, that whoever called this method checked isLocal first
    data(globalIndex)
  }

  def rawData: Seq[Seq[T]] = {
    data.values.toSeq
  }

  def isLocal(globalIndex: Int): Boolean = {
    data.contains(globalIndex)
  }

  def localDataSize: Int = {
    permanentDataSize
  }

  def dimension: Int = {
    data.head._2.length
  }

  def localIndices: Seq[Int] = {
    data.keys.toSeq
  }
}
