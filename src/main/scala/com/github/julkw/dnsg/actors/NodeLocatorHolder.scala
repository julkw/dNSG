package com.github.julkw.dnsg.actors

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.julkw.dnsg.actors.Coordinators.ClusterCoordinator.{CoordinationEvent, FinishedKnngNodeLocator, FinishedSearchOnGraphNodeLocator, SearchOnGraphNodeLocator}
import com.github.julkw.dnsg.actors.Coordinators.GraphConnectorCoordinator.{ConnectionCoordinationEvent, FinishedGraphConnectorNodeLocator, GraphConnectorNodeLocator}
import com.github.julkw.dnsg.actors.Coordinators.GraphRedistributionCoordinator.{AllSharedReplication, DataReplicationModel, FinishedRedistributerNodeLocator, PrimaryAssignmentsDone, RedistributerNodeLocator, RedistributionCoordinationEvent, SecondaryAssignmentsDone}
import com.github.julkw.dnsg.actors.Coordinators.NodeCoordinator.{NodeCoordinationEvent, StartBuildingNSG}
import com.github.julkw.dnsg.actors.DataHolder.{LoadDataEvent, RedistributeData}
import com.github.julkw.dnsg.actors.GraphConnector.{ConnectGraphEvent, ConnectorDistributionInfo}
import com.github.julkw.dnsg.actors.GraphRedistributer.{DistributeData, RedistributionEvent}
import com.github.julkw.dnsg.actors.SearchOnGraph.SearchOnGraphActor.{GraphDistribution, RedistributeGraph, SearchOnGraphEvent}
import com.github.julkw.dnsg.actors.nndescent.KnngWorker.{BuildKNNGEvent, KnngWorkerNodeLocator}
import com.github.julkw.dnsg.util.{LocalityCheck, NodeLocator, NodeLocatorBuilder, dNSGSerializable}

object NodeLocatorHolder {
  trait NodeLocationEvent extends dNSGSerializable

  final case class AllNodeLocatorHolders(nodeLocatorHolders: Set[ActorRef[NodeLocationEvent]], graphSize: Int) extends NodeLocationEvent

  final case class LocalSOGDistributionInfo(responsibility: Seq[Int], sender: ActorRef[SearchOnGraphEvent]) extends NodeLocationEvent

  final case class SOGDistInfoBatch(indices: Seq[Int], responsibleActor: ActorRef[SearchOnGraphEvent], batchNumber: Int, sender: ActorRef[NodeLocationEvent]) extends NodeLocationEvent

  final case class GetNextBatch(nextBatchNumber: Int, sender: ActorRef[NodeLocationEvent]) extends NodeLocationEvent

  final case class ShareNodeLocator(sendToCoordinator: Boolean) extends NodeLocationEvent

  final case object SendNodeLocatorToClusterCoordinator extends NodeLocationEvent

  final case class KnngWorkerGotGraphFrom(searchOnGraphActor: ActorRef[SearchOnGraphEvent], knngActor: ActorRef[BuildKNNGEvent]) extends NodeLocationEvent

  final case class BuildConnectorNodeLocator(connectorCoordinator: ActorRef[ConnectionCoordinationEvent]) extends NodeLocationEvent

  final case class GraphConnectorGotGraphFrom(searchOnGraphActor: ActorRef[SearchOnGraphEvent], graphConnector: ActorRef[ConnectGraphEvent]) extends NodeLocationEvent

  final case class BuildRedistributionNodeLocator(connectorCoordinator: ActorRef[RedistributionCoordinationEvent]) extends NodeLocationEvent

  final case class GraphRedistributerGotGraphFrom(graphConnector: ActorRef[ConnectGraphEvent], graphRedistributer: ActorRef[RedistributionEvent]) extends NodeLocationEvent

  final case class LocalPrimaryNodeAssignments(nodeAssignments: Map[Int, ActorRef[SearchOnGraphEvent]]) extends NodeLocationEvent

  final case class PrimaryAssignmentBatch(indices: Seq[Int], responsibleActor: ActorRef[SearchOnGraphEvent], batchNumber: Int, sender: ActorRef[NodeLocationEvent]) extends NodeLocationEvent

  final case class LocalSecondaryNodeAssignments(nodeAssignments: Map[Int, Set[ActorRef[SearchOnGraphEvent]]]) extends NodeLocationEvent

  final case class SecondaryAssignmentBatch(indices: Seq[Int], responsibleActors: Set[ActorRef[SearchOnGraphEvent]], batchNumber: Int, lastBatch: Boolean, sender: ActorRef[NodeLocationEvent]) extends NodeLocationEvent

  final case class ShareRedistributionAssignments(replicationModel: DataReplicationModel) extends NodeLocationEvent

  final case class SendNodeLocatorToNodeCoordinator(navigatingNode: Int) extends NodeLocationEvent

  def apply (clusterCoordinator: ActorRef[CoordinationEvent],
             nodeCoordinator: ActorRef[NodeCoordinationEvent],
             localDataHolder: ActorRef[LoadDataEvent],
             maxMessageSize: Int): Behavior[NodeLocationEvent] = {
    Behaviors.setup( ctx => new NodeLocatorHolder(clusterCoordinator, nodeCoordinator, localDataHolder, maxMessageSize, ctx).setup())
  }
}

class NodeLocatorHolder(clusterCoordinator: ActorRef[CoordinationEvent],
                        nodeCoordinator: ActorRef[NodeCoordinationEvent],
                        localDataHolder: ActorRef[LoadDataEvent],
                        maxMessageSize: Int,
                        ctx: ActorContext[NodeLocatorHolder.NodeLocationEvent]) extends LocalityCheck {
  import NodeLocatorHolder._

  def setup(): Behavior[NodeLocationEvent] =
  Behaviors.receiveMessagePartial {
    case AllNodeLocatorHolders(nodeLocatorHolders, graphSize) =>
      val otherNodeLocatorHolders = (nodeLocatorHolders - ctx.self).map(actor => actor -> true).toMap
      gatherAndShareSOGWorkerDistInfo(otherNodeLocatorHolders, Seq.empty, Set.empty, new NodeLocatorBuilder[SearchOnGraphEvent](graphSize), None)

    case LocalSOGDistributionInfo(responsibility, sender) =>
      ctx.self ! LocalSOGDistributionInfo(responsibility, sender)
      setup()

    case SOGDistInfoBatch(indices, responsibleActor, batchNumber, sender) =>
      ctx.self ! SOGDistInfoBatch(indices, responsibleActor, batchNumber, sender)
      setup()
  }

  def gatherAndShareSOGWorkerDistInfo(otherNodeLocatorHolders: Map[ActorRef[NodeLocationEvent], Boolean],
                                      distInfoBatches: Seq[(ActorRef[SearchOnGraphEvent], Seq[Int])],
                                      localGraphHolders: Set[ActorRef[SearchOnGraphEvent]],
                                      nodeLocatorBuilder: NodeLocatorBuilder[SearchOnGraphEvent],
                                      finalNodeLocator: Option[NodeLocator[SearchOnGraphEvent]]): Behavior[NodeLocationEvent] =
    Behaviors.receiveMessagePartial {
      case LocalSOGDistributionInfo(responsibility, sender) =>
        val batches = responsibility.grouped(maxMessageSize).map(indices => (sender, indices)).toSeq
        val batchNumber = distInfoBatches.length
        // send new info immediately to those actors who have already asked for more
        val updatedNodeLocatorHolders = otherNodeLocatorHolders.transform { (nodeLocatorHolder, sendImmediately) =>
          if (sendImmediately) {
            nodeLocatorHolder ! SOGDistInfoBatch(batches.head._2, batches.head._1, batchNumber, ctx.self)
          }
          false
        }
        val nodeLocator = nodeLocatorBuilder.addLocation(responsibility, sender)
        if (nodeLocator.isDefined) {
          clusterCoordinator ! FinishedSearchOnGraphNodeLocator
        }
        gatherAndShareSOGWorkerDistInfo(updatedNodeLocatorHolders, distInfoBatches ++ batches, localGraphHolders + sender, nodeLocatorBuilder, nodeLocator)

      case SOGDistInfoBatch(indices, responsibleActor, batchNumber, sender) =>
        val nodeLocator = nodeLocatorBuilder.addLocation(indices, responsibleActor)
        if (nodeLocator.isDefined) {
          clusterCoordinator ! FinishedSearchOnGraphNodeLocator
        } else {
          sender ! GetNextBatch(batchNumber + 1, ctx.self)
        }
        gatherAndShareSOGWorkerDistInfo(otherNodeLocatorHolders, distInfoBatches, localGraphHolders, nodeLocatorBuilder, nodeLocator)

      case GetNextBatch(nextBatchNumber, sender) =>
        if (nextBatchNumber < distInfoBatches.length) {
          val nextBatch = distInfoBatches(nextBatchNumber)
          sender ! SOGDistInfoBatch(nextBatch._2, nextBatch._1, nextBatchNumber, ctx.self)
          gatherAndShareSOGWorkerDistInfo(otherNodeLocatorHolders, distInfoBatches, localGraphHolders, nodeLocatorBuilder, finalNodeLocator)
        } else {
          gatherAndShareSOGWorkerDistInfo(otherNodeLocatorHolders + (sender -> true), distInfoBatches, localGraphHolders, nodeLocatorBuilder, finalNodeLocator)
        }

      case ShareNodeLocator(sendToCoordinator) =>
        val nodeLocator = finalNodeLocator.get
        localGraphHolders.foreach(graphHolder => graphHolder ! GraphDistribution(nodeLocator))
          //.foreach(worker => worker ! BuildApproximateGraph(nodeLocator))
        if (sendToCoordinator) {
          clusterCoordinator ! SearchOnGraphNodeLocator(nodeLocator)
        }
        gatherKNNGDistInfo(otherNodeLocatorHolders.keys.toSet, nodeLocator, Map.empty)

      case KnngWorkerGotGraphFrom(knngActor, searchOnGraphActor) =>
        ctx.self ! KnngWorkerGotGraphFrom(knngActor, searchOnGraphActor)
        gatherAndShareSOGWorkerDistInfo(otherNodeLocatorHolders, distInfoBatches, localGraphHolders, nodeLocatorBuilder, finalNodeLocator)
  }

  def gatherKNNGDistInfo(otherNodeLocatorHolders: Set[ActorRef[NodeLocationEvent]],
                         lastNodeLocator: NodeLocator[SearchOnGraphEvent],
                         actorMapping: Map[ActorRef[SearchOnGraphEvent], ActorRef[BuildKNNGEvent]]): Behavior[NodeLocationEvent] =
    Behaviors.receiveMessagePartial {
      case KnngWorkerGotGraphFrom(searchOnGraphActor, knngActor) =>
        if (isLocal(searchOnGraphActor)) {
          // share info with other nodeLocatorHolders
          otherNodeLocatorHolders.foreach(nodeLocatorHolder => nodeLocatorHolder ! KnngWorkerGotGraphFrom(searchOnGraphActor, knngActor))
        }
        val updatedMapping = actorMapping + (searchOnGraphActor -> knngActor)
        if (updatedMapping.size == lastNodeLocator.allActors.size) {
          clusterCoordinator ! FinishedKnngNodeLocator
        }
        gatherKNNGDistInfo(otherNodeLocatorHolders, lastNodeLocator, updatedMapping)

      case ShareNodeLocator(_) =>
        val worker = actorMapping.values.toSet
        val nodeLocator = NodeLocator(lastNodeLocator.locationData, lastNodeLocator.actors.map(knngActor => actorMapping(knngActor)))
        val localKNNGWorkers = worker.filter(knngWorker => isLocal(knngWorker))
        localKNNGWorkers.foreach(knngWorker => knngWorker ! KnngWorkerNodeLocator(nodeLocator))
        holdSOGNodeLocator(otherNodeLocatorHolders, lastNodeLocator)
  }

  def holdSOGNodeLocator(otherNodeLocatorHolders: Set[ActorRef[NodeLocationEvent]],
                         nodeLocator: NodeLocator[SearchOnGraphEvent]): Behavior[NodeLocationEvent] =
    Behaviors.receiveMessagePartial {
      case BuildConnectorNodeLocator(connectorCoordinator) =>
        gatherConnectorDistInfo(otherNodeLocatorHolders, nodeLocator, Map.empty, connectorCoordinator)

      case SendNodeLocatorToNodeCoordinator(navigatingNode) =>
        nodeCoordinator ! StartBuildingNSG(navigatingNode, nodeLocator)
        holdSOGNodeLocator(otherNodeLocatorHolders, nodeLocator)

      case SendNodeLocatorToClusterCoordinator =>
        clusterCoordinator ! SearchOnGraphNodeLocator(nodeLocator)
        holdSOGNodeLocator(otherNodeLocatorHolders, nodeLocator)

      case GraphConnectorGotGraphFrom(searchOnGraphActor, graphConnector) =>
        ctx.self ! GraphConnectorGotGraphFrom(searchOnGraphActor, graphConnector)
        holdSOGNodeLocator(otherNodeLocatorHolders, nodeLocator)
    }

  def gatherConnectorDistInfo(otherNodeLocatorHolders: Set[ActorRef[NodeLocationEvent]],
                              lastNodeLocator: NodeLocator[SearchOnGraphEvent],
                              actorMapping: Map[ActorRef[SearchOnGraphEvent], ActorRef[ConnectGraphEvent]],
                              connectorCoordinator: ActorRef[ConnectionCoordinationEvent]): Behavior[NodeLocationEvent] =
    Behaviors.receiveMessagePartial {
      case GraphConnectorGotGraphFrom(searchOnGraphActor, graphConnector) =>
        if (isLocal(graphConnector)) {
          // share info with other nodeLocatorHolders
          otherNodeLocatorHolders.foreach(nodeLocatorHolder => nodeLocatorHolder ! GraphConnectorGotGraphFrom(searchOnGraphActor, graphConnector))
        }
        val updatedMapping = actorMapping + (searchOnGraphActor -> graphConnector)
        if (updatedMapping.size == lastNodeLocator.allActors.size) {
          connectorCoordinator ! FinishedGraphConnectorNodeLocator
        }
        gatherConnectorDistInfo(otherNodeLocatorHolders, lastNodeLocator, updatedMapping, connectorCoordinator)

      case ShareNodeLocator(sendToCoordinator) =>
        val graphConnectors = actorMapping.values.toSet
        val nodeLocator = NodeLocator(lastNodeLocator.locationData, lastNodeLocator.actors.map(sogActor => actorMapping(sogActor)))
        if (sendToCoordinator) {
          connectorCoordinator ! GraphConnectorNodeLocator(nodeLocator)
        }
        val localConnectors = graphConnectors.filter(connector => isLocal(connector))
        localConnectors.foreach(connector => connector ! ConnectorDistributionInfo(nodeLocator))
        holdConnectorDistInfo(otherNodeLocatorHolders, nodeLocator)
    }

  def holdConnectorDistInfo(otherNodeLocatorHolders: Set[ActorRef[NodeLocationEvent]],
                            nodeLocator: NodeLocator[ConnectGraphEvent]): Behavior[NodeLocationEvent] =
    Behaviors.receiveMessagePartial {
      case BuildRedistributionNodeLocator(redistributionCoordinator) =>
        gatherRedistributerDistInfo(otherNodeLocatorHolders, nodeLocator, Map.empty, redistributionCoordinator)

      case GraphRedistributerGotGraphFrom(graphConnector, redistributer) =>
        ctx.self ! GraphRedistributerGotGraphFrom(graphConnector, redistributer)
        holdConnectorDistInfo(otherNodeLocatorHolders, nodeLocator)
    }

  def gatherRedistributerDistInfo(otherNodeLocatorHolders: Set[ActorRef[NodeLocationEvent]],
                                  lastNodeLocator: NodeLocator[ConnectGraphEvent],
                                  actorMapping: Map[ActorRef[ConnectGraphEvent], ActorRef[RedistributionEvent]],
                                  redistributionCoordinator: ActorRef[RedistributionCoordinationEvent]): Behavior[NodeLocationEvent] =
    Behaviors.receiveMessagePartial {
      case GraphRedistributerGotGraphFrom(graphConnector, redistributer) =>
        if (isLocal(redistributer)) {
          // share info with other nodeLocatorHolders
          otherNodeLocatorHolders.foreach(nodeLocatorHolder => nodeLocatorHolder ! GraphRedistributerGotGraphFrom(graphConnector, redistributer))
        }
        val updatedMapping = actorMapping + (graphConnector -> redistributer)
        if (updatedMapping.size == lastNodeLocator.allActors.size) {
          redistributionCoordinator ! FinishedRedistributerNodeLocator
        }
        gatherRedistributerDistInfo(otherNodeLocatorHolders, lastNodeLocator, updatedMapping, redistributionCoordinator)

      case ShareNodeLocator(sendToCoordinator) =>
        val redistributers = actorMapping.values.toSet
        val nodeLocator = NodeLocator(lastNodeLocator.locationData, lastNodeLocator.actors.map(connector => actorMapping(connector)))
        if (sendToCoordinator) {
          redistributionCoordinator ! RedistributerNodeLocator(nodeLocator)
        }
        val localRedistributers = redistributers.filter(redistributer => isLocal(redistributer))
        localRedistributers.foreach(redistributer => redistributer ! DistributeData(nodeLocator))
        val locatorHolders = otherNodeLocatorHolders.map(actor => actor -> true).toMap
        gatherAndSharePrimaryRedistributionAssignments(locatorHolders, redistributers, Seq.empty, NodeLocatorBuilder(nodeLocator.graphSize), None, redistributionCoordinator)

      case PrimaryAssignmentBatch(indices, responsibleActor, batchNumber, sender) =>
        ctx.self ! PrimaryAssignmentBatch(indices, responsibleActor, batchNumber, sender)
        gatherRedistributerDistInfo(otherNodeLocatorHolders, lastNodeLocator, actorMapping, redistributionCoordinator)
    }

  def gatherAndSharePrimaryRedistributionAssignments(otherNodeLocatorHolders: Map[ActorRef[NodeLocationEvent], Boolean],
                                                     redistributers: Set[ActorRef[RedistributionEvent]],
                                                     distInfoBatches: Seq[(ActorRef[SearchOnGraphEvent], Seq[Int])],
                                                     nodeLocatorBuilder: NodeLocatorBuilder[SearchOnGraphEvent],
                                                     finalNodeLocator: Option[NodeLocator[SearchOnGraphEvent]],
                                                     redistributionCoordinator: ActorRef[RedistributionCoordinationEvent]): Behavior[NodeLocationEvent] =
    Behaviors.receiveMessagePartial {
      case LocalPrimaryNodeAssignments(nodeAssignments) =>
        val batches = nodeAssignments.keys.toSeq.groupBy{ index => nodeAssignments(index) }.toSeq.flatMap { case (actor, indices) =>
          indices.grouped(maxMessageSize).map(batchedIndices => (actor, batchedIndices))
        }
        val batchNumber = distInfoBatches.length
        // send new info immediately to those actors who have already asked for more
        val updatedNodeLocatorHolders = otherNodeLocatorHolders.transform { (nodeLocatorHolder, sendImmediately) =>
          if (sendImmediately) {
            nodeLocatorHolder ! PrimaryAssignmentBatch(batches.head._2, batches.head._1, batchNumber, ctx.self)
          }
          false
        }
        batches.foreach(batch => nodeLocatorBuilder.addLocation(batch._2, batch._1))
        val nodeLocator = nodeLocatorBuilder.nodeLocator()
        if (nodeLocator.isDefined) {
          redistributionCoordinator ! PrimaryAssignmentsDone
        }
        gatherAndSharePrimaryRedistributionAssignments(updatedNodeLocatorHolders, redistributers, distInfoBatches ++ batches, nodeLocatorBuilder, nodeLocator, redistributionCoordinator)

      case PrimaryAssignmentBatch(indices, responsibleActor, batchNumber, sender) =>
        val nodeLocator = nodeLocatorBuilder.addLocation(indices, responsibleActor)
        if (nodeLocator.isDefined) {
          redistributionCoordinator ! PrimaryAssignmentsDone
        } else {
          sender ! GetNextBatch(batchNumber + 1, ctx.self)
        }
        gatherAndSharePrimaryRedistributionAssignments(otherNodeLocatorHolders, redistributers, distInfoBatches, nodeLocatorBuilder, nodeLocator, redistributionCoordinator)

      case GetNextBatch(nextBatchNumber, sender) =>
        if (nextBatchNumber < distInfoBatches.length) {
          val nextBatch = distInfoBatches(nextBatchNumber)
          sender ! PrimaryAssignmentBatch(nextBatch._2, nextBatch._1, nextBatchNumber, ctx.self)
          gatherAndSharePrimaryRedistributionAssignments(otherNodeLocatorHolders, redistributers, distInfoBatches, nodeLocatorBuilder, finalNodeLocator, redistributionCoordinator)
        } else {
          gatherAndSharePrimaryRedistributionAssignments(otherNodeLocatorHolders + (sender -> true), redistributers, distInfoBatches, nodeLocatorBuilder, finalNodeLocator, redistributionCoordinator)
        }

        // once all nodeLocators are complete we are either told to collect secondaryAssignments or to share the primary ones immediately
      case LocalSecondaryNodeAssignments(nodeAssignments) =>
        ctx.self ! LocalSecondaryNodeAssignments(nodeAssignments)
        val locatorHolders = otherNodeLocatorHolders.transform((_, _) => true)
        val localWorkers = finalNodeLocator.get.allActors.count(worker => isLocal(worker))
        gatherAndShareSecondaryRedistributionAssignments(locatorHolders, Seq.empty, Map.empty, finalNodeLocator.get, localWorkers, otherNodeLocatorHolders.size, redistributionCoordinator)

      case SecondaryAssignmentBatch(indices, responsibleActor, batchNumber, lastBatch, sender) =>
        ctx.self ! SecondaryAssignmentBatch(indices, responsibleActor, batchNumber, lastBatch, sender)
        val locatorHolders = otherNodeLocatorHolders.transform((_, _) => true)
        val localWorkers = finalNodeLocator.get.allActors.count(worker => isLocal(worker))
        gatherAndShareSecondaryRedistributionAssignments(locatorHolders, Seq.empty, Map.empty, finalNodeLocator.get, localWorkers, otherNodeLocatorHolders.size, redistributionCoordinator)

      case ShareRedistributionAssignments(_) =>
        val nodeLocator = finalNodeLocator.get
        val localSogActors = nodeLocator.allActors.filter(sogActor => isLocal(sogActor))
        localSogActors.foreach(graphHolder => graphHolder ! RedistributeGraph(nodeLocator, Map.empty, redistributionCoordinator))
        localDataHolder ! RedistributeData(nodeLocator, Map.empty)
        holdSOGNodeLocator(otherNodeLocatorHolders.keys.toSet, nodeLocator)
    }

  def gatherAndShareSecondaryRedistributionAssignments(otherNodeLocatorHolders: Map[ActorRef[NodeLocationEvent], Boolean],
                                                       distInfoBatches: Seq[(Set[ActorRef[SearchOnGraphEvent]], Seq[Int])],
                                                       secondaryAssignments: Map[Int, Set[ActorRef[SearchOnGraphEvent]]],
                                                       primaryAssignments: NodeLocator[SearchOnGraphEvent],
                                                       waitingOnLocals: Int,
                                                       waitingOnNodeLocators: Int,
                                                       redistributionCoordinator: ActorRef[RedistributionCoordinationEvent]): Behavior[NodeLocationEvent] =
    Behaviors.receiveMessagePartial {
      case LocalSecondaryNodeAssignments(nodeAssignments) =>
        val batches = nodeAssignments.keys.toSeq.groupBy{ index => nodeAssignments(index) }.toSeq.flatMap { case (actor, indices) =>
          indices.grouped(maxMessageSize).map(batchedIndices => (actor, batchedIndices))
        }
        // ctx.log.info("Received {} local batches", batches.length)
        val batchNumber = distInfoBatches.length
        // send new info immediately to those actors who have already asked for more
        val updatedNodeLocatorHolders = otherNodeLocatorHolders.transform { (nodeLocatorHolder, sendImmediately) =>
          if (sendImmediately) {
            if (batches.nonEmpty) {
              val lastBatch = waitingOnLocals == 1 && batches.length == 1
              nodeLocatorHolder ! SecondaryAssignmentBatch(batches.head._2, batches.head._1, batchNumber, lastBatch, ctx.self)
            } else if (waitingOnLocals == 1) {
              // the other still expects a message from us but there is (and won't be) any new info to send
              nodeLocatorHolder ! SecondaryAssignmentBatch(Seq.empty, Set.empty, 0, lastBatch = true, ctx.self)
            }
          }
          // if it was true before and nothing was sent, it is still true
          sendImmediately && waitingOnLocals > 1 && batches.isEmpty
        }
        if (waitingOnLocals == 1 && waitingOnNodeLocators == 0) {
          redistributionCoordinator ! SecondaryAssignmentsDone
        }
        gatherAndShareSecondaryRedistributionAssignments(updatedNodeLocatorHolders,
          distInfoBatches ++ batches,
          secondaryAssignments ++ nodeAssignments,
          primaryAssignments,
          waitingOnLocals - 1,
          waitingOnNodeLocators,
          redistributionCoordinator)

      case SecondaryAssignmentBatch(indices, responsibleActors, batchNumber, lastBatch, sender) =>
        val newAssignments = indices.map(index => index -> responsibleActors)
        val updatedSecondaryAssignments = secondaryAssignments ++ newAssignments
        val updatedWaitingOnNodeLocators = if (lastBatch) { waitingOnNodeLocators - 1 } else { waitingOnNodeLocators }
        // ctx.log.info("Got batch {}, which is the last: {}", batchNumber, lastBatch)
        if (!lastBatch && indices.nonEmpty) {
          sender ! GetNextBatch(batchNumber + 1, ctx.self)
        } else if (updatedWaitingOnNodeLocators == 0 && waitingOnLocals == 0) {
          redistributionCoordinator ! SecondaryAssignmentsDone
        }
        gatherAndShareSecondaryRedistributionAssignments(otherNodeLocatorHolders, distInfoBatches, updatedSecondaryAssignments, primaryAssignments, waitingOnLocals, updatedWaitingOnNodeLocators, redistributionCoordinator)

      case GetNextBatch(nextBatchNumber, sender) =>
        if (nextBatchNumber < distInfoBatches.length) {
          val nextBatch = distInfoBatches(nextBatchNumber)
          val lastBatch = nextBatchNumber == distInfoBatches.length - 1 && waitingOnLocals == 0
          sender ! SecondaryAssignmentBatch(nextBatch._2, nextBatch._1, nextBatchNumber, lastBatch, ctx.self)
          gatherAndShareSecondaryRedistributionAssignments(otherNodeLocatorHolders,
            distInfoBatches,
            secondaryAssignments,
            primaryAssignments,
            waitingOnLocals,
            waitingOnNodeLocators,
            redistributionCoordinator)
        } else {
          sender ! SecondaryAssignmentBatch(Seq.empty, Set.empty, -1, waitingOnLocals == 0, ctx.self)
          gatherAndShareSecondaryRedistributionAssignments(otherNodeLocatorHolders + (sender -> true),
            distInfoBatches,
            secondaryAssignments,
            primaryAssignments,
            waitingOnLocals,
            waitingOnNodeLocators,
            redistributionCoordinator)
        }

      case ShareRedistributionAssignments(replicationModel) =>
        val localSogActors = primaryAssignments.allActors.filter(sogActor => isLocal(sogActor))
        val updatedSecondaryAssignments = if (replicationModel == AllSharedReplication) {
          secondaryAssignments.transform((node, _) => primaryAssignments.allActors - primaryAssignments.findResponsibleActor(node))
        } else {
          secondaryAssignments
        }
        localSogActors.foreach(graphHolder => graphHolder ! RedistributeGraph(primaryAssignments, updatedSecondaryAssignments, redistributionCoordinator))
        localDataHolder ! RedistributeData(primaryAssignments, updatedSecondaryAssignments)
        holdSOGNodeLocator(otherNodeLocatorHolders.keys.toSet, primaryAssignments)
    }
}
