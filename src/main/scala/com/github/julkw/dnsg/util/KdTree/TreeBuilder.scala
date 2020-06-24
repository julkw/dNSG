package com.github.julkw.dnsg.util.KdTree

import com.github.julkw.dnsg.util.Data.LocalData

import scala.collection.mutable
import scala.collection.mutable.Queue

case class TreeBuilder (data: LocalData[Float], k: Int) {

  def construct(indices: Seq[Int]): IndexTree = {
    val node: LeafNode[Seq[Int]] = LeafNode(indices)
    // split each node in half
    val splitPoint = 0.5f
    if (indices.length <= k) {
      IndexTree(node)
    } else {
      val root = oneLevelSplit(indices, splitPoint)
      val updateChildren: mutable.Queue[SplitNode[Seq[Int]]] = mutable.Queue.empty
      updateChildren += root
      while (updateChildren.nonEmpty) {
        val currentNode: SplitNode[Seq[Int]] = updateChildren.dequeue()
        // check if children need to be split
        if (currentNode.left.data.length > k) {
          val newLeft = oneLevelSplit(currentNode.left.data, splitPoint)
          currentNode.left = newLeft
          updateChildren += newLeft
        }
        if (currentNode.right.data.length > k) {
          val newRight = oneLevelSplit(currentNode.right.data, splitPoint)
          currentNode.right = newRight
          updateChildren += newRight
        }
      }
      IndexTree(root)
    }
  }

  def oneLevelSplit(indices: Seq[Int], splitPoint: Float): SplitNode[Seq[Int]] = {
    assert(splitPoint > 0)
    val r = scala.util.Random
    val maxDimension = data.dimension
    val splittingDimension = r.nextInt(maxDimension)

    val sortedValues = indices.map(index => (index, data.get(index)(splittingDimension))).sortBy(_._2)
    val medianIndex = (indices.length * splitPoint).toInt + 1 //round up
    val median = sortedValues(medianIndex)._2

    // to ensure a balanced tree, nodes that are directly on the border are placed into a random child
    // this is possible here because we only ever look for groups of nodes and not specific nodes using the tree
    val leftIndices = sortedValues.slice(0, medianIndex).map(_._1)
    val rightIndices = sortedValues.slice(medianIndex, sortedValues.length).map(_._1)

    val leftNode: LeafNode[Seq[Int]] = LeafNode[Seq[Int]](leftIndices)
    val rightNode: LeafNode[Seq[Int]] = LeafNode[Seq[Int]](rightIndices)

    val node  = SplitNode(leftNode, rightNode, splittingDimension, median)
    node
  }

}
