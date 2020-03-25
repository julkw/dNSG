package com.github.julkw.dnsg.util

import scala.collection.mutable
import scala.collection.mutable.Queue

case class TreeBuilder (data: Seq[Seq[Float]], k: Int) {

  def construct(indices:Seq[Int]): IndexTree = {
    val node: LeafNode[Seq[Int]] = LeafNode(indices)
    if (indices.length <= k) {
      IndexTree(node)
    } else {
      val toSplit: mutable.Queue[SplitNode[Seq[Int]]] = Queue.empty
      val root = oneLevelSplit(indices)
      toSplit += root
      while (toSplit.nonEmpty) {
        val currentNode: SplitNode[Seq[Int]] = toSplit.dequeue()
        val newLeft = oneLevelSplit(currentNode.left.data)
        val newRight = oneLevelSplit(currentNode.right.data)
        currentNode.left = newLeft
        currentNode.right = newRight
        if (newLeft.data.length > k)
          toSplit += newLeft
        if (newRight.data.length > k)
          toSplit += newRight
      }
      IndexTree(root)
    }
  }

  def oneLevelSplit(indices: Seq[Int]): SplitNode[Seq[Int]] = {
    // TODO this function throws an out of bounds error sometimes and I don't know why
    if(indices.length < 5) {
      val debug = 4
    }
    val r = scala.util.Random
    val maxDimension = data(0).length
    val splittingDimension = r.nextInt(maxDimension)
    // TODO replace with better median implementation
    // https://stackoverflow.com/questions/4662292/scala-median-implementation
    val sortedValues: Seq[Float] = indices.map(index => data(index)(splittingDimension)).sorted
    if (sortedValues.length < 5) {
      val debug = 4
    }
    val median: Float = sortedValues(indices.length / 2)

    val leftIndices = indices.filter(index => data(index)(splittingDimension) < median)
    val rightIndices = indices.filter(index => data(index)(splittingDimension) >= median)

    val leftNode: LeafNode[Seq[Int]] = LeafNode[Seq[Int]](leftIndices)
    val rightNode: LeafNode[Seq[Int]] = LeafNode[Seq[Int]](rightIndices)

    val node  = SplitNode(leftNode, rightNode, splittingDimension, median)
    node
  }

}
