package com.github.julkw.dnsg.util.Data

case class CacheData[T](cacheSize: Int, data: LocalData[T]) {
  val cache = NodeCacheLRU[T](cacheSize)

  def get(globalIndex: Int): Seq[T] = {
    // this assumes, that whoever called this method checked isLocal first
    if (data.isLocal(globalIndex)) {
      data.get(globalIndex)
    } else {
      cache.get(globalIndex)
    }
  }

  def isPermanentlyLocal(globalIndex: Int): Boolean = {
    data.isLocal(globalIndex)
  }

  def isLocal(globalIndex: Int): Boolean = {
    data.isLocal(globalIndex) || cache.inCache(globalIndex)
  }

  def add(nodeIndex: Int, nodeData: Seq[T]): Unit = {
    if (!isPermanentlyLocal(nodeIndex)) {
      cache.insert(nodeIndex, nodeData)
    }
  }

}
