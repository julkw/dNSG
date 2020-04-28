package com.github.julkw.dnsg.util

import scala.collection.mutable
import scala.collection.mutable.Queue

case class TreeBuilder (data: LocalData[Float], k: Int) {

  def construct(indices: Seq[Int]): IndexTree = {
    val node: LeafNode[Seq[Int]] = LeafNode(indices)
    if (indices.length <= k) {
      IndexTree(node)
    } else {
      val root = oneLevelSplit(indices)
      val updateChildren: mutable.Queue[SplitNode[Seq[Int]]] = Queue.empty
      updateChildren += root
      while (updateChildren.nonEmpty) {
        val currentNode: SplitNode[Seq[Int]] = updateChildren.dequeue()
        // check if children need to be split
        if (currentNode.left.data.length > k) {
          val newLeft = oneLevelSplit(currentNode.left.data)
          currentNode.left = newLeft
          updateChildren += newLeft
        }
        if (currentNode.right.data.length > k) {
          val newRight = oneLevelSplit(currentNode.right.data)
          currentNode.right = newRight
          updateChildren += newRight
        }
      }
      IndexTree(root)
    }
  }

  def oneLevelSplit(indices: Seq[Int]): SplitNode[Seq[Int]] = {
    val r = scala.util.Random
    val maxDimension = data.dimension
    val splittingDimension = r.nextInt(maxDimension)
    // TODO replace with better median implementation
    // https://stackoverflow.com/questions/4662292/scala-median-implementation
    val sortedValues: Seq[Float] = indices.map(index => data.at(index).get(splittingDimension)).sorted
    val median: Float = sortedValues(indices.length / 2)

    val leftIndices = indices.filter(index => data.at(index).get(splittingDimension) < median)
    val rightIndices = indices.filter(index => data.at(index).get(splittingDimension) >= median)

    val leftNode: LeafNode[Seq[Int]] = LeafNode[Seq[Int]](leftIndices)
    val rightNode: LeafNode[Seq[Int]] = LeafNode[Seq[Int]](rightIndices)

    val node  = SplitNode(leftNode, rightNode, splittingDimension, median)
    node
  }

}
