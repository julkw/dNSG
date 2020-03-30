package com.github.julkw.dnsg.util

import akka.actor.typed.ActorRef

case class NodeLocator[T](positionTree: PositionTree[T]) {
  def findResponsibleActor (query: Seq[Float]): ActorRef[T] = {
    positionTree.root.queryLeaf(query).data
  }
  // TODO hold variable number of Position Trees
  // Can map index to correct Position Tree (later for Clustering)
  // change between local and global index
}
