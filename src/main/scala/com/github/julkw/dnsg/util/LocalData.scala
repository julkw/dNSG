package com.github.julkw.dnsg.util

// this class let's the local data pretend to be global, so the workers can continue working with global indices
case class LocalData[T] (data: Seq[Seq[T]], localOffset: Int) {
  val cacheSize: Int = 100
  protected val cache: NodeCacheLRU[T] = NodeCacheLRU(cacheSize)

  def get(globalIndex: Int): Seq[T] = {
    // this assumes, that whoever called this method checked isLocal first
    val index = localIndex(globalIndex)
    if (0 <= index && index < data.length) {
      val index = localIndex(globalIndex)
      data(index)
    } else {
      cache.get(globalIndex)
    }
  }

  def isPermanentlyLocal(globalIndex: Int): Boolean = {
    val index = localIndex(globalIndex)
    0 <= index && index < data.length
  }

  def isLocal(globalIndex: Int): Boolean = {
    isPermanentlyLocal(globalIndex) || cache.inCache(globalIndex)
  }

  def add(nodeIndex: Int, nodeData: Seq[T]): Unit = {
    cache.insert(nodeIndex, nodeData)
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
