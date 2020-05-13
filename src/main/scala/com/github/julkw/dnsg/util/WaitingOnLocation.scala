package com.github.julkw.dnsg.util

class WaitingOnLocation {
  private var waitingOnLocation: Map[Int, Set[Int]] = Map.empty

  def alreadyIn(locationIndex: Int, needsLocationIndex: Int): Boolean = {
    if (waitingOnLocation.contains(locationIndex)) {
      waitingOnLocation(locationIndex).contains(needsLocationIndex)
    } else {
      false
    }
  }

  // return whether the location still needs to be asked for
  def insert(locationIndex: Int, needsLocationIndex: Int): Boolean = {
    if (waitingOnLocation.contains(locationIndex)) {
      waitingOnLocation = waitingOnLocation + (locationIndex -> (waitingOnLocation(locationIndex) + needsLocationIndex))
      false
    } else {
      waitingOnLocation = waitingOnLocation + (locationIndex -> Set(needsLocationIndex))
      true
    }
  }

  def received(locationIndex: Int): Set[Int] = {
    if (waitingOnLocation.contains(locationIndex)) {
      val stuffToProcess = waitingOnLocation(locationIndex)
      waitingOnLocation = waitingOnLocation - locationIndex
      stuffToProcess
    } else {
      Set.empty
    }
  }
}
