package com.github.julkw.dnsg.util

trait NodeCache[T] {
  def insert(index: Int, data: Seq[T])
  def inCache(index: Int): Boolean
  def get(index: Int): Seq[T]
  def currentSize(): Int
  def empty(): Unit
}

case class NodeCacheLRU[T](size: Int) extends NodeCache[T] {
  case class CachedNode(data: Seq[T], var age: Int = 0)
  protected var nodes: Map[Int, CachedNode] = Map.empty

  def insert(index: Int, data: Seq[T]): Unit = {
    if (nodes.contains(index)) {
      nodes(index).age = 0
    } else {
      nodes = nodes + (index -> CachedNode(data))
    }
    update()
  }

  def inCache(index: Int): Boolean = {
    nodes.contains(index)
  }

  def get(index: Int): Seq[T] = {
    // this assumes, that the asker has checked if the index is in cache
    nodes(index).age = 0
    update()
    nodes(index).data
  }

  def currentSize(): Int = {
    nodes.size
  }

  def update(): Unit = {
    if (nodes.size > size) {
      var oldest = (nodes.head._1, nodes.head._2.age)
      nodes.foreach { case (index, node) =>
        node.age += 1
        if (oldest._2 < node.age) {
          oldest = (index, node.age)
        }
      }
      nodes = nodes - oldest._1
    }
    else {
      // only age
      nodes.values.foreach(node => node.age += 1)
    }
  }

  def empty(): Unit = {
    nodes = Map.empty
  }
}

case class NodeCacheRandom[T](size: Int) extends NodeCache[T] {
  protected var nodes: Map[Int, Seq[T]] = Map.empty

  def insert(index: Int, data: Seq[T]): Unit = {
    if (!nodes.contains(index)) {
      nodes = nodes + (index -> data)
      update()
    }
  }

  def inCache(index: Int): Boolean = {
    nodes.contains(index)
  }

  def get(index: Int): Seq[T] = {
    // this assumes, that the asker has checked if the index is in cache
    nodes(index)
  }

  def currentSize: Int = {
    nodes.size
  }

  def update(): Unit = {
    if (nodes.size > size) {
      // remove a random element
      val r = scala.util.Random.nextInt(nodes.size)
      // TODO no idea if this is in any way fast
      val indexToRemove = nodes.iterator.drop(r).next()._1
      nodes = nodes - indexToRemove
    }
  }

  def empty(): Unit = {
    nodes = Map.empty
  }
}
