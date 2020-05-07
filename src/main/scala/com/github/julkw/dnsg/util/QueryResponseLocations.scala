package com.github.julkw.dnsg.util

case class QueryResponseLocations[T](data: LocalData[T]) {
  protected case class ResponseLocation(location: Seq[T], var usedBy: Int)
  private var responseLocations: Map[Int, ResponseLocation] = Map.empty

  def hasLocation(index: Int): Boolean = {
    data.isLocal(index) || responseLocations.contains(index)
  }

  def location(index: Int): Seq[T] = {
    if (data.isLocal(index)) {
      data.get(index)
    } else {
      responseLocations(index).location
    }
  }

  def addedToCandidateList(index: Int, location: Seq[T]): Unit = {
    data.add(index, location)
    if(responseLocations.contains(index)) {
      responseLocations(index).usedBy += 1
    } else if (!data.isPermanentlyLocal(index)) {
      // if permanently local, it doesn't need to be added to response locations
      responseLocations = responseLocations + (index-> ResponseLocation(location, 1))
    }
  }

  def removedFromCandidateList(index: Int): Unit = {
    if (!data.isPermanentlyLocal(index)) {
      responseLocations(index).usedBy -= 1
      if (responseLocations(index).usedBy == 0) {
        responseLocations = responseLocations - index
      }
    }
  }
}
