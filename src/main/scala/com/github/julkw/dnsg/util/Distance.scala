package com.github.julkw.dnsg.util

import scala.math.{pow, sqrt}

trait Distance {
  def euclideanDist(pointX: Array[Float], pointY: Array[Float]): Double = {
    var sum = 0.0d
    pointX.indices.foreach { index =>
      sum += pow(pointX(index).toDouble - pointY(index).toDouble, 2)
    }
    val result = sqrt(sum)
    result
    //sqrt((pointX zip pointY).map { case (x,y) => pow(y - x, 2) }.sum)
  }
}
