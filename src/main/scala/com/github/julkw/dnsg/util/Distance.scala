package com.github.julkw.dnsg.util

import scala.math.{pow, sqrt}

trait Distance {
  def euclideanDist(pointX: Seq[Float], pointY: Seq[Float]): Double = {
    sqrt((pointX zip pointY).map { case (x,y) => pow(y - x, 2) }.sum)
  }

  def averageGraphDist(graph: Map[Int, Seq[(Int, Double)]], k: Int): Double = {
    val allLocalDists = graph.toSeq.flatMap{case (node, neighbors) =>
      neighbors.map{_._2}
    }.sum
    allLocalDists / (graph.size * k)
  }
}
