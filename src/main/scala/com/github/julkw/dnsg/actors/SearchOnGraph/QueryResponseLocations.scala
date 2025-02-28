package com.github.julkw.dnsg.actors.SearchOnGraph

import com.github.julkw.dnsg.util.Data.LocalData

case class QueryResponseLocations[T](data: LocalData[T]) {
  protected case class ResponseLocation(location: Array[T], var usedBy: Int)
  private var responseLocations: Map[Int, ResponseLocation] = Map.empty

  def hasLocation(index: Int): Boolean = {
    data.isLocal(index) || responseLocations.contains(index)
  }

  def size(): Int = {
    responseLocations.size
  }

  def location(index: Int): Array[T] = {
    if (data.isLocal(index)) {
      data.get(index)
    } else {
      responseLocations(index).location
    }
  }

  def addedToCandidateList(index: Int, location: Array[T]): Unit = {
    //data.add(index, location)
    if(responseLocations.contains(index)) {
      responseLocations(index).usedBy += 1
    } else if (!data.isLocal(index)) {
      // if permanently local, it doesn't need to be added to response locations
      responseLocations = responseLocations + (index-> ResponseLocation(location, 1))
    }
  }

  def removedFromCandidateList(index: Int): Unit = {
    if (!data.isLocal(index)) {
      responseLocations(index).usedBy -= 1
      if (responseLocations(index).usedBy == 0) {
        responseLocations = responseLocations - index
      }
    }
  }
}
