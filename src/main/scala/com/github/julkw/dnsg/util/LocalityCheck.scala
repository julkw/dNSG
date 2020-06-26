package com.github.julkw.dnsg.util

import akka.actor.typed.ActorRef

trait LocalityCheck {
  // this does not work for clusterSingletons (the ClusterCoordinator)
  def isLocal[T](actor: ActorRef[T]): Boolean = {
    actor.path.address.host.isEmpty
  }
}
