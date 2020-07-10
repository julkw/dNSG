package com.github.julkw.dnsg.actors.Coordinators

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.{ConnectionAchieved, ConnectorsCleanedUp, CoordinationEvent}
import com.github.julkw.dnsg.actors.Coordinators.GraphConnectorCoordinator.{CleanUpConnectors, ConnectionCoordinationEvent, StartGraphRedistribution}
import com.github.julkw.dnsg.actors.GraphRedistributer.{InitialAssignWithParents, RedistributionEvent, SendSecondaryAssignments}
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

  final case class GetMoreInitialAssignments(sender: ActorRef[RedistributionEvent]) extends RedistributionCoordinationEvent

  final case class SecondaryAssignmentsDone(replicatedNodes: Int, replications: Int) extends RedistributionCoordinationEvent

  final case class WrappedCoordinationEvent(event: CoordinationEvent) extends RedistributionCoordinationEvent

  final case object DoneWithRedistribution extends RedistributionCoordinationEvent

  trait DataReplicationModel

  case object NoReplication extends DataReplicationModel

  case object OnlyParentsReplication extends DataReplicationModel

  case object AllSharedReplication extends DataReplicationModel

  def apply(navigatingNodeIndex: Int,
            replicationModel: DataReplicationModel,
            graphNodeLocator: NodeLocator[SearchOnGraphEvent],
            nodeLocatorHolders: Set[ActorRef[NodeLocationEvent]],
            maxMessageSize: Int): Behavior[RedistributionCoordinationEvent] =
    Behaviors.setup { ctx =>
      val coordinationEventAdapter: ActorRef[CoordinationEvent] =
        ctx.messageAdapter { event => WrappedCoordinationEvent(event)}
      new GraphRedistributionCoordinator(navigatingNodeIndex, replicationModel, graphNodeLocator, nodeLocatorHolders, coordinationEventAdapter, maxMessageSize, ctx).setup()
    }
}

class GraphRedistributionCoordinator(navigatingNodeIndex: Int,
                                     replicationModel: GraphRedistributionCoordinator.DataReplicationModel,
                                     graphNodeLocator: NodeLocator[SearchOnGraphEvent],
                                     nodeLocatorHolders: Set[ActorRef[NodeLocationEvent]],
                                     coordinationEventAdapter: ActorRef[CoordinationEvent],
                                     maxMessageSize: Int,
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
    val toSend = primaryAssignmentRoots.keys.flatMap { worker =>
      primaryAssignmentRoots(worker).map { node =>
        waitingOnAnswers += 1
        (worker, node)
      }
    }.groupBy { assignment =>
      redistributerLocator.findResponsibleActor(assignment._2)
    }.transform { (redistributer, assignments) =>
      val groupedAssignments = assignments.groupBy(_._1).transform((_, assignment) => assignment.map(_._2).toSeq)
      sendInitialAssignments(redistributer, groupedAssignments)
    }
    waitForParentsToBeAssigned(connectorCoordinator, toSend, waitingOnAnswers, primaryAssignmentRoots.size, redistributerLocator)
  }

  def waitForParentsToBeAssigned(connectorCoordinator: ActorRef[ConnectionCoordinationEvent],
                                 initialAssignmentsToSend: Map[ActorRef[RedistributionEvent], Map[ActorRef[SearchOnGraphEvent], Seq[Int]]],
                                 waitingOn: Int,
                                 numWorkers: Int,
                                 redistributerLocator: NodeLocator[RedistributionEvent]): Behavior[RedistributionCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case GetMoreInitialAssignments(sender) =>
        val updatedAssignmentsToSend = initialAssignmentsToSend + (sender -> sendInitialAssignments(sender, initialAssignmentsToSend(sender)))
        waitForParentsToBeAssigned(connectorCoordinator, updatedAssignmentsToSend, waitingOn, numWorkers, redistributerLocator)

      case AssignWithParentsDone =>
        if (waitingOn == 1) {
          ctx.log.info("Done with assigning parents, now collecting secondary assignments")
          redistributerLocator.allActors.foreach ( redistributer => redistributer ! SendSecondaryAssignments)
          waitForSecondaryAssignments(connectorCoordinator, numWorkers, nodeLocatorHolders.size)
        } else {
          waitForParentsToBeAssigned(connectorCoordinator, initialAssignmentsToSend, waitingOn - 1, numWorkers, redistributerLocator)
        }
    }

  def waitForSecondaryAssignments(connectorCoordinator: ActorRef[ConnectionCoordinationEvent],
                                  numWorkers: Int,
                                  waitingOn: Int): Behavior[RedistributionCoordinationEvent] =
    Behaviors.receiveMessagePartial {
      case SecondaryAssignmentsDone(replicatedNodes, replications) =>
        ctx.log.info("Node locator collected secondary assignments for {} nodes, still waiting on {} nls", replicatedNodes, waitingOn - 1)
        if (waitingOn == 1) {
          ctx.log.info("Calculated secondary redistribution assignments")
          val updatedReplications = if (replicationModel == AllSharedReplication) {
            replicatedNodes * numWorkers
          } else { replications }
          ctx.log.info("{} nodes replicated, overall {} replications", replicatedNodes, updatedReplications)
          // TODO on cluster with onlyParentsRedistribution not all nodes got the new data and with AllSharedRedistribution they all got the data but did not continue working
          // TODO figure our why
          nodeLocatorHolders.foreach(nodeLocatorHolder => nodeLocatorHolder ! ShareRedistributionAssignments(replicationModel))
          waitForRedistribution(connectorCoordinator, finishedWorkers = 0)
        } else {
          waitForSecondaryAssignments(connectorCoordinator, numWorkers, waitingOn - 1)
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

  def sendInitialAssignments(redistributer: ActorRef[RedistributionEvent], assignments: Map[ActorRef[SearchOnGraphEvent], Seq[Int]]): Map[ActorRef[SearchOnGraphEvent], Seq[Int]] = {
    var messageSize = 0
    val sendAssignments = assignments.takeWhile { assignment =>
      messageSize += assignment._2.length
      messageSize <= maxMessageSize
    }
    val assignmentsLeftToSend = assignments -- sendAssignments.keys
    if (messageSize < maxMessageSize && assignmentsLeftToSend.nonEmpty) {
      val splitAssignment = assignmentsLeftToSend.head
      val leftOverSpaceToSend = maxMessageSize - messageSize
      val splitAssignmentSendNow = splitAssignment._2.slice(0, leftOverSpaceToSend)
      redistributer ! InitialAssignWithParents(sendAssignments + (splitAssignment._1 -> splitAssignmentSendNow), true)
      val splitAssignmentSendLater = splitAssignment._2.slice(leftOverSpaceToSend, splitAssignment._2.length)
      sendAssignments + (splitAssignment._1 -> splitAssignmentSendLater)
    } else {
      redistributer ! InitialAssignWithParents(sendAssignments, assignmentsLeftToSend.nonEmpty)
      assignmentsLeftToSend
    }
  }
}
