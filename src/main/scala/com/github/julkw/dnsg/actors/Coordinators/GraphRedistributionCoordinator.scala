package com.github.julkw.dnsg.actors.Coordinators

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.{ConnectionAchieved, ConnectorsCleanedUp, CoordinationEvent}
import com.github.julkw.dnsg.actors.Coordinators.GraphConnectorCoordinator.{CleanUpConnectors, ConnectionCoordinationEvent, StartGraphRedistribution}
import com.github.julkw.dnsg.actors.DataHolder.LoadDataEvent
import com.github.julkw.dnsg.actors.GraphRedistributer.{AssignWithParents, RedistributionEvent, SendSecondaryAssignments}
import com.github.julkw.dnsg.actors.NodeLocatorHolder.{BuildRedistributionNodeLocator, NodeLocationEvent, SendNodeLocatorToClusterCoordinator, ShareNodeLocator, ShareRedistributionAssignments}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.SearchOnGraphEvent
import com.github.julkw.dnsg.util.{LocalityCheck, NodeLocator, dNSGSerializable}

object GraphRedistributionCoordinator {

  trait RedistributionCoordinationEvent extends dNSGSerializable

  final case object FinishedRedistributerNodeLocator extends RedistributionCoordinationEvent

  final case class RedistributerNodeLocator(nodeLocator: NodeLocator[RedistributionEvent]) extends RedistributionCoordinationEvent

  final case class PrimaryAssignmentParents(assignedWorker: ActorRef[SearchOnGraphEvent], treeNodes: Seq[Int]) extends RedistributionCoordinationEvent

  final case object PrimaryAssignmentsDone extends RedistributionCoordinationEvent

  final case class PrimaryAssignmentsNodeLocator(nodeLocator: NodeLocator[RedistributionEvent]) extends RedistributionCoordinationEvent

  final case object AssignWithParentsDone extends RedistributionCoordinationEvent

  final case object SecondaryAssignmentsDone extends RedistributionCoordinationEvent

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
            nodeLocatorHolders: Set[ActorRef[NodeLocationEvent]],
            clusterCoordinator: ActorRef[CoordinationEvent]): Behavior[RedistributionCoordinationEvent] =
    Behaviors.setup { ctx =>
      val coordinationEventAdapter: ActorRef[CoordinationEvent] =
        ctx.messageAdapter { event => WrappedCoordinationEvent(event)}
      new GraphRedistributionCoordinator(navigatingNodeIndex, replicationModel, graphNodeLocator, dataHolder, nodeLocatorHolders, clusterCoordinator, coordinationEventAdapter, ctx).setup()
    }
}

class GraphRedistributionCoordinator(navigatingNodeIndex: Int,
                                     replicationModel: GraphRedistributionCoordinator.DataReplicationModel,
                                     graphNodeLocator: NodeLocator[SearchOnGraphEvent],
                                     dataHolder: ActorRef[LoadDataEvent],
                                     nodeLocatorHolders: Set[ActorRef[NodeLocationEvent]],
                                     clusterCoordinator: ActorRef[CoordinationEvent],
                                     coordinationEventAdapter: ActorRef[CoordinationEvent],
                                     ctx: ActorContext[GraphRedistributionCoordinator.RedistributionCoordinationEvent]) extends LocalityCheck {
  import GraphRedistributionCoordinator._

  def setup(): Behavior[RedistributionCoordinationEvent] = {
    ctx.log.info("Now connecting graph for redistribution")
    val connectorCoordinator = ctx.spawn(GraphConnectorCoordinator(navigatingNodeIndex, graphNodeLocator, nodeLocatorHolders, coordinationEventAdapter), name="GraphConnectorCoordinator")
    connectGraphForRedistribution(connectorCoordinator)
  }

  def connectGraphForRedistribution(connectorCoordinator: ActorRef[ConnectionCoordinationEvent]): Behavior[RedistributionCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case WrappedCoordinationEvent(event) =>
        event match {
          case ConnectionAchieved =>
            ctx.log.info("All Search on Graph actors updated to connected graph, can start redistribution now")
            nodeLocatorHolders.foreach(nodeLocatorHolder => nodeLocatorHolder ! BuildRedistributionNodeLocator(ctx.self))
            connectorCoordinator ! StartGraphRedistribution(ctx.self)
            waitOnRedistributerNodeLocator(nodeLocatorHolders.size, connectorCoordinator)
        }
    }

  def waitOnRedistributerNodeLocator(waitingOnNodeLocatorHolders: Int, connectorCoordinator: ActorRef[ConnectionCoordinationEvent]): Behavior[RedistributionCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case FinishedRedistributerNodeLocator =>
        if (waitingOnNodeLocatorHolders == 1) {
          ctx.log.info("All redistributers started")
          nodeLocatorHolders.foreach(nodeLocatorHolder => nodeLocatorHolder ! ShareNodeLocator(isLocal(nodeLocatorHolder)))
        }
        waitOnRedistributerNodeLocator(waitingOnNodeLocatorHolders - 1, connectorCoordinator)

      case RedistributerNodeLocator(nodeLocator) =>
        waitForPrimaryAssignments(connectorCoordinator, nodeLocator, Map.empty, nodeLocatorHolders.size)

      case PrimaryAssignmentsDone =>
        ctx.self ! PrimaryAssignmentsDone
        waitOnRedistributerNodeLocator(waitingOnNodeLocatorHolders, connectorCoordinator)
    }

  def waitForPrimaryAssignments(connectorCoordinator: ActorRef[ConnectionCoordinationEvent],
                                redistributerLocator: NodeLocator[RedistributionEvent],
                                primaryAssignmentRoots: Map[ActorRef[SearchOnGraphEvent], Seq[Int]],
                                waitingOnNodeLocatorHolders: Int): Behavior[RedistributionCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case PrimaryAssignmentsDone =>
        if (waitingOnNodeLocatorHolders == 1) {
          ctx.log.info("Calculated primary redistribution assignments")
          replicationModel match {
            case NoReplication =>
              nodeLocatorHolders.foreach(nodeLocatorHolder => nodeLocatorHolder ! ShareRedistributionAssignments(replicationModel))
              waitForRedistribution(connectorCoordinator, finishedWorkers = 0)
            case OnlyParentsReplication | AllSharedReplication =>
              if (primaryAssignmentRoots.size == graphNodeLocator.allActors.size) {
                startAssigningParents(connectorCoordinator, redistributerLocator, primaryAssignmentRoots)
              } else {
                waitForPrimaryAssignmentRoots(connectorCoordinator, redistributerLocator, primaryAssignmentRoots)
              }
          }
        } else {
          waitForPrimaryAssignments(connectorCoordinator, redistributerLocator, primaryAssignmentRoots, waitingOnNodeLocatorHolders - 1)
        }

      case PrimaryAssignmentParents(worker, treeNodes) =>
        waitForPrimaryAssignments(connectorCoordinator, redistributerLocator, primaryAssignmentRoots + (worker -> treeNodes), waitingOnNodeLocatorHolders)
    }

  def waitForPrimaryAssignmentRoots(connectorCoordinator: ActorRef[ConnectionCoordinationEvent],
                                    redistributerLocator: NodeLocator[RedistributionEvent],
                                    primaryAssignmentRoots: Map[ActorRef[SearchOnGraphEvent], Seq[Int]]): Behavior[RedistributionCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case PrimaryAssignmentParents(assignedWorker, treeNodes) =>
        val updatedPrimaryAssignmentRoots = primaryAssignmentRoots + (assignedWorker -> treeNodes)
        if (updatedPrimaryAssignmentRoots.size == graphNodeLocator.allActors.size) {
          startAssigningParents(connectorCoordinator, redistributerLocator, updatedPrimaryAssignmentRoots)
        } else {
          waitForPrimaryAssignmentRoots(connectorCoordinator, redistributerLocator, primaryAssignmentRoots)
        }
    }

  def startAssigningParents(connectorCoordinator: ActorRef[ConnectionCoordinationEvent],
                            redistributerLocator: NodeLocator[RedistributionEvent],
                            primaryAssignmentRoots: Map[ActorRef[SearchOnGraphEvent], Seq[Int]]): Behavior[RedistributionCoordinationEvent] = {
    ctx.log.info("Find secondary assignments")
    var waitingOnAnswers = 0
    primaryAssignmentRoots.foreach { case (worker, treeNodes) =>
      treeNodes.foreach { assignmentRoot =>
        redistributerLocator.findResponsibleActor(assignmentRoot) ! AssignWithParents(assignmentRoot, worker)
        waitingOnAnswers += 1
      }
    }
    waitForParentsToBeAssigned(connectorCoordinator, waitingOnAnswers, redistributerLocator)
  }

  def waitForParentsToBeAssigned(connectorCoordinator: ActorRef[ConnectionCoordinationEvent],
                                 waitingOn: Int,
                                 redistributerLocator: NodeLocator[RedistributionEvent]): Behavior[RedistributionCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case AssignWithParentsDone =>
        if (waitingOn == 1) {
          ctx.log.info("Done with assigning parents, now collecting secondary assignments")
          redistributerLocator.allActors.foreach ( redistributer => redistributer ! SendSecondaryAssignments)
          waitForSecondaryAssignments(connectorCoordinator, nodeLocatorHolders.size)
        } else {
          waitForParentsToBeAssigned(connectorCoordinator, waitingOn - 1, redistributerLocator)
        }
    }

  def waitForSecondaryAssignments(connectorCoordinator: ActorRef[ConnectionCoordinationEvent],
                                  waitingOn: Int): Behavior[RedistributionCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case SecondaryAssignmentsDone =>
        if (waitingOn == 1) {
          ctx.log.info("Calculated secondary redistribution assignments")
          nodeLocatorHolders.foreach(nodeLocatorHolder => nodeLocatorHolder ! ShareRedistributionAssignments(replicationModel))
          waitForRedistribution(connectorCoordinator, finishedWorkers = 0)
        } else {
          waitForSecondaryAssignments(connectorCoordinator, waitingOn - 1)
        }
    }

  def waitForRedistribution(connectorCoordinator: ActorRef[ConnectionCoordinationEvent],
                            finishedWorkers: Int): Behavior[RedistributionCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case DoneWithRedistribution =>
        if (finishedWorkers + 1 == graphNodeLocator.allActors.size) {
          // shutdown the connection establishing workers
          connectorCoordinator ! CleanUpConnectors
          cleanup()
        } else {
          waitForRedistribution(connectorCoordinator, finishedWorkers + 1)
        }
    }

  def cleanup(): Behavior[RedistributionCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case WrappedCoordinationEvent(event) =>
        event match {
          case ConnectorsCleanedUp =>
            ctx.log.info("Done with data redistribution")
            val localHolders = nodeLocatorHolders.filter(nodeLocatorHolder => nodeLocatorHolder.path.address.host.isEmpty)
            assert(localHolders.size == 1)
            localHolders.head ! SendNodeLocatorToClusterCoordinator
            Behaviors.stopped
        }
    }
}
