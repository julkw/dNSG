package com.github.julkw.dnsg.util.KdTree

import akka.actor.typed.ActorRef

import scala.collection.mutable
import scala.language.postfixOps

trait KdTree[T] {
  val root: TreeNode[T]

  def allLeafs(): Seq[LeafNode[T]] = {
    val toProcess: mutable.Queue[TreeNode[T]] = mutable.Queue(root)
    var allLeafs: Seq[LeafNode[T]] = Seq.empty
    while (toProcess.nonEmpty) {
      val nextNode = toProcess.dequeue()
      nextNode match {
        case leaf: LeafNode[T] =>
          allLeafs =  allLeafs :+ leaf
        case split: SplitNode[T] =>
          toProcess.enqueue(split.right)
          toProcess.enqueue(split.left)
      }
    }
    allLeafs
  }
}

case class PositionTree[T](root: TreeNode[ActorRef[T]]) extends KdTree[ActorRef[T]]

case class IndexTree(root: TreeNode[Seq[Int]]) extends KdTree[Seq[Int]]




