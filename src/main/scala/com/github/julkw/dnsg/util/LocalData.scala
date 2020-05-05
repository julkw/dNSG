package com.github.julkw.dnsg.util

// this class let's the local data pretend to be global, so the workers can continue working with global indices
case class LocalData[T] (data: Seq[Seq[T]], localOffset: Int) {

  def at(globalIndex: Int): Option[Seq[T]] = {
    if (isLocal(globalIndex)) {
      val index = localIndex(globalIndex)
      Some(data(index))
    } else {
      None
    }
  }

  def isLocal(globalIndex: Int): Boolean = {
    val index = localIndex(globalIndex)
    0 <= index && index < data.length
  }

  def localDataSize: Int = {
    data.length
  }

  def dimension: Int = {
    data(0).length
  }

  // TODO this return the global indices for the local data. Maybe rename as this might get confusing
  def localIndices: Seq[Int] = {
    localOffset until localOffset + data.length
  }

  def localIndex(globalIndex: Int): Int = {
    globalIndex - localOffset
  }

}
