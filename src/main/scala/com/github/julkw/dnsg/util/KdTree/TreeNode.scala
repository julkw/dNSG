package com.github.julkw.dnsg.util.KdTree

trait TreeNode[dataType] {
  def queryLeaf(query: Seq[Float]): LeafNode[dataType]
  def queryChild(query: Seq[Float]): TreeNode[dataType]
  def inverseQueryChild(query: Seq[Float]): TreeNode[dataType]
  def data: dataType
}
case class LeafNode[T](data: T) extends TreeNode[T] {
  def queryLeaf(query: Seq[Float]): LeafNode[T] = {
    this
  }
  def queryChild(query: Seq[Float]): TreeNode[T] = {
    this
  }
  def inverseQueryChild(query: Seq[Float]): TreeNode[T] = {
    this
  }
}

case class SplitNode[T](var left: TreeNode[T], var right: TreeNode[T], dimension: Int, border: Float) extends TreeNode[T] {
  def data: T = {
    // TODO add warning, as this shouldn't be called
    left.data
  }

  def queryLeaf(query: Seq[Float]): LeafNode[T] = {
    if (query(dimension) < border) {
      left.queryLeaf(query)
    } else {
      right.queryLeaf(query)
    }
  }

  def queryChild(query: Seq[Float]): TreeNode[T] = {
    if(query(dimension) < border) {
      left
    } else {
      right
    }
  }

  def inverseQueryChild(query: Seq[Float]): TreeNode[T] = {
    if(query(dimension) < border) {
      right
    } else {
      left
    }
  }
}

