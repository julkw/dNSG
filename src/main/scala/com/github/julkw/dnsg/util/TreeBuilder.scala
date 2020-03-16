package com.github.julkw.dnsg.util

import scala.collection.mutable
import scala.collection.mutable.Queue

case class TreeBuilder (data: Seq[Seq[Float]], k: Int) {

  def constructIteratively(indices:Seq[Int]): TreeNode[Seq[Int]] = {
    val node: LeafNode[Seq[Int]] = LeafNode(indices)
    if (indices.length <= k) {
      node
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
        // TODO Check references etc. I do not believe this works
      }
      root
    }
  }

  def constructRecursively(indices: Seq[Int]): TreeNode[Seq[Int]] = {
    val node: LeafNode[Seq[Int]] = LeafNode(indices)
    if (indices.length <= k) {
      node
    } else {
      val sNode: SplitNode[Seq[Int]] = oneLevelSplit(indices)
      sNode.left = constructRecursively(sNode.left.data)
      sNode.right = constructRecursively(sNode.right.data)
      sNode
    }
  }

  def oneLevelSplit(indices: Seq[Int]): SplitNode[Seq[Int]] = {
    val maxDimension = data(0).length
    val r = scala.util.Random
    val splittingDimension = r.nextInt(maxDimension)
    // TODO replace with better median implementation
    // https://stackoverflow.com/questions/4662292/scala-median-implementation
    // TODO this breaks the program right now!!!
    val median = indices.map(index => data(index)(splittingDimension)).sorted.drop(data.length / 2).head
    val leftIndices = indices.filter(index => data(index)(splittingDimension) < median)
    val rightIndices = indices.filter(index => data(index)(splittingDimension) >= median)

    val leftNode: LeafNode[Seq[Int]] = LeafNode[Seq[Int]](leftIndices)
    val rightNode: LeafNode[Seq[Int]] = LeafNode[Seq[Int]](rightIndices)

    val node  = new SplitNode(leftNode, rightNode, splittingDimension, median)
    node
  }

}
