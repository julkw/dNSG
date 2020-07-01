package com.github.julkw.dnsg.util

class WaitingOnLocation[T] {
  private var waitingOnLocation: Map[Int, Set[T]] = Map.empty

  def alreadyIn(locationIndex: Int, needsLocationIndex: T): Boolean = {
    if (waitingOnLocation.contains(locationIndex)) {
      waitingOnLocation(locationIndex).contains(needsLocationIndex)
    } else {
      false
    }
  }

  // return whether the location still needs to be asked for
  def insert(locationIndex: Int, needsLocationIndex: T): Boolean = {
    if (waitingOnLocation.contains(locationIndex)) {
      waitingOnLocation = waitingOnLocation + (locationIndex -> (waitingOnLocation(locationIndex) + needsLocationIndex))
      false
    } else {
      waitingOnLocation = waitingOnLocation + (locationIndex -> Set(needsLocationIndex))
      true
    }
  }

  def insertMultiple(locationIndex: Int, needLocationIndex: Set[T]): Boolean = {
    if (waitingOnLocation.contains(locationIndex)) {
      waitingOnLocation = waitingOnLocation + (locationIndex -> (waitingOnLocation(locationIndex) ++ needLocationIndex))
      false
    } else {
      waitingOnLocation = waitingOnLocation + (locationIndex -> needLocationIndex)
      true
    }
  }

  def received(locationIndex: Int): Set[T] = {
    if (waitingOnLocation.contains(locationIndex)) {
      val stuffToProcess = waitingOnLocation(locationIndex)
      waitingOnLocation = waitingOnLocation - locationIndex
      stuffToProcess
    } else {
      Set.empty
    }
  }
}
