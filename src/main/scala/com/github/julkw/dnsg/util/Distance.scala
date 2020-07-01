package com.github.julkw.dnsg.util

import com.github.julkw.dnsg.actors.nndescent.KnngWorker.NeighborWithDist

import scala.math.{pow, sqrt}

trait Distance {
  def euclideanDist(pointX: Seq[Float], pointY: Seq[Float]): Double = {
    sqrt((pointX zip pointY).map { case (x,y) => pow(y - x, 2) }.sum)
  }

  def averageGraphDist(graph: Map[Int, Seq[NeighborWithDist]]): Double = {
    val allLocalDists = graph.toSeq.flatMap { case (node, neighbors) =>
      neighbors.map{_.distance}
    }
    allLocalDists.sum / allLocalDists.length
  }
}
