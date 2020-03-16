package com.github.julkw.dnsg.util

import akka.actor.typed.ActorRef
import com.github.julkw.dnsg.actors.nndescent.KnngWorker.BuildGraphEvent

import scala.language.postfixOps

trait KdTree[T] {
  val root: TreeNode[T]
}

case class PositionTree (rootNode: TreeNode[ActorRef[BuildGraphEvent]]) extends KdTree[ActorRef[BuildGraphEvent]] {
  val root: TreeNode[ActorRef[BuildGraphEvent]] = rootNode

  def findResponsibleActor (query: Seq[Float]): ActorRef[_] = {
    root.queryLeaf(query).data
  }
}

case class IndexTree(root: TreeNode[Seq[Int]]) extends KdTree[Seq[Int]]




