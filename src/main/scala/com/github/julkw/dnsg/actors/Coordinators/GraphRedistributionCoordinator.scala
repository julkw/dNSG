package com.github.julkw.dnsg.actors.Coordinators

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.{ConnectionAchieved, ConnectorsCleanedUp, CoordinationEvent, RedistributionFinished}
import com.github.julkw.dnsg.actors.Coordinators.GraphConnectorCoordinator.{CleanUpConnectors, ConnectionCoordinationEvent, StartGraphRedistribution}
import com.github.julkw.dnsg.actors.DataHolder.{LoadDataEvent, StartRedistributingData}
import com.github.julkw.dnsg.actors.GraphRedistributer.{AssignWithParents, DistributeData, RedistributionEvent, SendSecondaryAssignments}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.{RedistributeGraph, SearchOnGraphEvent}
import com.github.julkw.dnsg.util.{NodeLocator, NodeLocatorBuilder, dNSGSerializable}

object GraphRedistributionCoordinator {

  trait RedistributionCoordinationEvent extends dNSGSerializable

  // redistributing the graph
  final case class RedistributerDistributionInfo(responsibility: Seq[Int], sender: ActorRef[RedistributionEvent]) extends RedistributionCoordinationEvent

  final case class PrimaryNodeAssignments(nodeAssignments: Map[Int, ActorRef[SearchOnGraphEvent]]) extends RedistributionCoordinationEvent

  final case class PrimaryAssignmentParents(assignedWorker: ActorRef[SearchOnGraphEvent], treeNodes: Seq[Int]) extends RedistributionCoordinationEvent

  final case object AssignWithParentsDone extends RedistributionCoordinationEvent

  final case class SecondaryNodeAssignments(nodeAssignments: Map[Int, Set[ActorRef[SearchOnGraphEvent]]]) extends RedistributionCoordinationEvent

  final case class WrappedCoordinationEvent(event: CoordinationEvent) extends RedistributionCoordinationEvent

  final case object DoneWithRedistribution extends RedistributionCoordinationEvent

  trait DataReplicationModel

  case object NoReplication extends DataReplicationModel

  case object OnlyParentsReplication extends DataReplicationModel

  case object AllSharedReplication extends DataReplicationModel

  def apply(navigatingNodeIndex: Int,
            replicationModel: DataReplicationModel,
            graphNodeLocator: NodeLocator[SearchOnGraphEvent],
            dataHolder: ActorRef[LoadDataEvent],
            clusterCoordinator: ActorRef[CoordinationEvent]): Behavior[RedistributionCoordinationEvent] =
    Behaviors.setup { ctx =>
      val coordinationEventAdapter: ActorRef[CoordinationEvent] =
        ctx.messageAdapter { event => WrappedCoordinationEvent(event)}
      new GraphRedistributionCoordinator(navigatingNodeIndex, replicationModel, graphNodeLocator, dataHolder, clusterCoordinator, coordinationEventAdapter, ctx).setup()
    }
}

class GraphRedistributionCoordinator(navigatingNodeIndex: Int,
                                     replicationModel: GraphRedistributionCoordinator.DataReplicationModel,
                                     graphNodeLocator: NodeLocator[SearchOnGraphEvent],
                                     dataHolder: ActorRef[LoadDataEvent],
                                     clusterCoordinator: ActorRef[CoordinationEvent],
                                     coordinationEventAdapter: ActorRef[CoordinationEvent],
                                     ctx: ActorContext[GraphRedistributionCoordinator.RedistributionCoordinationEvent]) {
  import GraphRedistributionCoordinator._

  def setup(): Behavior[RedistributionCoordinationEvent] = {
    ctx.log.info("Now connecting graph for redistribution")
    val connectorCoordinator = ctx.spawn(GraphConnectorCoordinator(navigatingNodeIndex, graphNodeLocator, coordinationEventAdapter), name="GraphConnectorCoordinator")
    connectGraphForRedistribution(connectorCoordinator)
  }

  def connectGraphForRedistribution(connectorCoordinator: ActorRef[ConnectionCoordinationEvent]): Behavior[RedistributionCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case WrappedCoordinationEvent(event) =>
        event match {
          case ConnectionAchieved =>
            ctx.log.info("All Search on Graph actors updated to connected graph, can start redistribution now")
            connectorCoordinator ! StartGraphRedistribution(ctx.self)
            waitOnRedistributionDistributionInfo(connectorCoordinator, NodeLocatorBuilder(graphNodeLocator.graphSize))
        }
    }

  def waitOnRedistributionDistributionInfo(connectorCoordinator: ActorRef[ConnectionCoordinationEvent],
                                           redistributionLocations: NodeLocatorBuilder[RedistributionEvent]):
  Behavior[RedistributionCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case RedistributerDistributionInfo(responsibilities, redistributer) =>
        redistributionLocations.addLocation(responsibilities, redistributer) match {
          case Some(redistributionLocator) =>
            ctx.log.info("All redistributers started")
            redistributionLocator.allActors.foreach(graphRedistributer => graphRedistributer ! DistributeData(redistributionLocator))
            waitOnInitialRedistributionAssignments(connectorCoordinator, NodeLocatorBuilder(graphNodeLocator.graphSize), redistributionLocator, Map.empty)
          case None =>
            waitOnRedistributionDistributionInfo(connectorCoordinator, redistributionLocations)
        }
    }

  def waitOnInitialRedistributionAssignments(connectorCoordinator: ActorRef[ConnectionCoordinationEvent],
                                             redistributionAssignments: NodeLocatorBuilder[SearchOnGraphEvent],
                                             redistributerLocator: NodeLocator[RedistributionEvent],
                                             primaryAssignmentRoots: Map[ActorRef[SearchOnGraphEvent], Seq[Int]]): Behavior[RedistributionCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case PrimaryNodeAssignments(assignments) =>
        redistributionAssignments.addFromMap(assignments) match {
          case Some(redistributionAssignments) =>
            ctx.log.info("Calculated primary redistribution assignments")
            replicationModel match {
              case NoReplication =>
                startRedistribution(connectorCoordinator, redistributionAssignments, Map.empty)
              case OnlyParentsReplication | AllSharedReplication =>
                if (primaryAssignmentRoots.size == redistributionAssignments.allActors.size) {
                  startAssigningParents(connectorCoordinator, redistributerLocator, redistributionAssignments, primaryAssignmentRoots)
                } else {
                  waitForPrimaryAssignmentRoots(connectorCoordinator, redistributerLocator, redistributionAssignments, primaryAssignmentRoots)
                }
            }
          case None =>
            waitOnInitialRedistributionAssignments(connectorCoordinator, redistributionAssignments, redistributerLocator, primaryAssignmentRoots)
        }

      case PrimaryAssignmentParents(worker, treeNodes) =>
        ctx.log.info("{} treenodes and their children assigned to worker", treeNodes.length)
        waitOnInitialRedistributionAssignments(connectorCoordinator, redistributionAssignments, redistributerLocator, primaryAssignmentRoots + (worker -> treeNodes))
    }

  def waitForPrimaryAssignmentRoots(connectorCoordinator: ActorRef[ConnectionCoordinationEvent],
                                    redistributerLocator: NodeLocator[RedistributionEvent],
                                    redistributionAssignments: NodeLocator[SearchOnGraphEvent],
                                    primaryAssignmentRoots: Map[ActorRef[SearchOnGraphEvent], Seq[Int]]): Behavior[RedistributionCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case PrimaryAssignmentParents(assignedWorker, treeNodes) =>
        val updatedPrimaryAssignmentRoots = primaryAssignmentRoots + (assignedWorker -> treeNodes)
        if (updatedPrimaryAssignmentRoots.size == redistributionAssignments.allActors.size) {
          startAssigningParents(connectorCoordinator, redistributerLocator, redistributionAssignments, updatedPrimaryAssignmentRoots)
        } else {
          waitForPrimaryAssignmentRoots(connectorCoordinator, redistributerLocator, redistributionAssignments, primaryAssignmentRoots)
        }
    }

  def startAssigningParents(connectorCoordinator: ActorRef[ConnectionCoordinationEvent],
                            redistributerLocator: NodeLocator[RedistributionEvent],
                            redistributionAssignments: NodeLocator[SearchOnGraphEvent],
                            primaryAssignmentRoots: Map[ActorRef[SearchOnGraphEvent], Seq[Int]]): Behavior[RedistributionCoordinationEvent] = {
    ctx.log.info("Find secondary assignments")
    var waitingOnAnswers = 0
    primaryAssignmentRoots.foreach { case (worker, treeNodes) =>
      treeNodes.foreach { assignmentRoot =>
        redistributerLocator.findResponsibleActor(assignmentRoot) ! AssignWithParents(assignmentRoot, worker)
        waitingOnAnswers += 1
      }
    }
    waitForParentsToBeAssigned(connectorCoordinator, waitingOnAnswers, redistributerLocator, redistributionAssignments)
  }

  def waitForParentsToBeAssigned(connectorCoordinator: ActorRef[ConnectionCoordinationEvent],
                                 waitingOn: Int,
                                 redistributerLocator: NodeLocator[RedistributionEvent],
                                 redistributionAssignments: NodeLocator[SearchOnGraphEvent]): Behavior[RedistributionCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case AssignWithParentsDone =>
        if (waitingOn == 1) {
          redistributerLocator.allActors.foreach ( redistributer => redistributer ! SendSecondaryAssignments)
          waitForSecondaryAssignments(connectorCoordinator, redistributerLocator.allActors.size, Map.empty, redistributionAssignments)
        } else {
          waitForParentsToBeAssigned(connectorCoordinator, waitingOn - 1, redistributerLocator, redistributionAssignments)
        }
    }

  def waitForSecondaryAssignments(connectorCoordinator: ActorRef[ConnectionCoordinationEvent],
                                  waitingOn: Int,
                                  secondaryAssignments: Map[Int, Set[ActorRef[SearchOnGraphEvent]]],
                                  redistributionAssignments: NodeLocator[SearchOnGraphEvent]): Behavior[RedistributionCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case SecondaryNodeAssignments(nodeAssignments) =>
        if (waitingOn == 1) {
          if (replicationModel == AllSharedReplication) {
            val updatedSecondaryAssignments = (secondaryAssignments ++ nodeAssignments).transform((node, assignees) => redistributionAssignments.allActors - redistributionAssignments.findResponsibleActor(node))
            startRedistribution(connectorCoordinator, redistributionAssignments, updatedSecondaryAssignments)
          } else {
            startRedistribution(connectorCoordinator, redistributionAssignments, secondaryAssignments ++ nodeAssignments)
          }
        } else {
          waitForSecondaryAssignments(connectorCoordinator, waitingOn - 1, secondaryAssignments ++ nodeAssignments, redistributionAssignments)
        }
    }

  def startRedistribution(connectorCoordinator: ActorRef[ConnectionCoordinationEvent],
                          primaryAssignments: NodeLocator[SearchOnGraphEvent],
                          secondaryAssignments: Map[Int, Set[ActorRef[SearchOnGraphEvent]]]): Behavior[RedistributionCoordinationEvent] = {
    ctx.log.info("All graph nodes now assigned to workers, start redistribution")
    graphNodeLocator.allActors.foreach(graphHolder => graphHolder ! RedistributeGraph(primaryAssignments, secondaryAssignments, ctx.self))
    dataHolder ! StartRedistributingData(primaryAssignments, secondaryAssignments)
    waitForRedistribution(connectorCoordinator, finishedWorkers = 0, primaryAssignments)
  }

  def waitForRedistribution(connectorCoordinator: ActorRef[ConnectionCoordinationEvent],
                            finishedWorkers: Int,
                            nodeLocator: NodeLocator[SearchOnGraphEvent]): Behavior[RedistributionCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case DoneWithRedistribution =>
        if (finishedWorkers + 1 == nodeLocator.allActors.size) {
          // shutdown the connection establishing workers
          connectorCoordinator ! CleanUpConnectors
          cleanup(nodeLocator, coordinatorShutdown = false)
        } else {
          waitForRedistribution(connectorCoordinator, finishedWorkers + 1, nodeLocator)
        }
    }

  def cleanup(newDistribution: NodeLocator[SearchOnGraphEvent], coordinatorShutdown: Boolean): Behavior[RedistributionCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case WrappedCoordinationEvent(event) =>
        event match {
          case ConnectorsCleanedUp =>
            ctx.log.info("Done with data redistribution")
            clusterCoordinator ! RedistributionFinished(newDistribution)
            Behaviors.stopped
        }
    }
}
