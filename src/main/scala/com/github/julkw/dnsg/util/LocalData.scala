package com.github.julkw.dnsg.util

// this class let's the local data pretend to be global, so the workers can continue working with global indices
case class LocalData[T] (data: Seq[Seq[T]], localOffset: Int) {

  def at(globalIndex: Int): Option[Seq[T]] = {
    val index = localIndex(globalIndex)
    if (0 <= index && index < data.length) {
      Some(data(index))
    } else {
      None
    }
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
