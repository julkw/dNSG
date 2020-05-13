package com.github.julkw.dnsg.util.Data

// this class let's the local data pretend to be global, so the workers can continue working with global indices
case class LocalData[T] (data: Seq[Seq[T]], localOffset: Int) {
  val cacheSize: Int = 0
  protected val cache: NodeCacheLRU[T] = NodeCacheLRU(cacheSize)

  def get(globalIndex: Int): Seq[T] = {
    // this assumes, that whoever called this method checked isLocal first
    val index = localIndex(globalIndex)
    data(index)
  }

  def isPermanentlyLocal(globalIndex: Int): Boolean = {
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

  def currentCacheSize(): Int = {
    cache.currentSize()
  }

}
