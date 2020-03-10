# Cluster Ideas:

## master c_Node:
- read Data from file
- save results (knng and nsg) to file?
- decide on initial data distribution (which node/actor is responsible for the edges of which g_node)
- also new distribution for createNSG stage?
- let the other know 
- manage stages of the algorithm
	- potentially decide when NNDescent is done and tell others 
	- start / manage depth first tree search to check for connectivity

## other c_Nodes
### at all stages
**DataHolder** ?
- get Data (either from file or from other(master) node) 

**graph_Coordinator / coordinator** ? 
- know how many graph_Actors to start with which nodes -> parent for graph_Actors
- know when to switch from *NNDescent* to *createNSG* (in communication with master node)
- hold knng subset during *createNSG*?

**replicator actor/router / graph_listener**
- know where each g_node's edges are stored
- use Distributed Data -> replicator actor, others get adresses with Get-Message
- instead use as a kind of router and forward each message to correct actor?
- Store Addresses in Map (g_node->ActorRef)
  - containing ActorRefs or identifiers
  - during *NNDescent* actual ActorRef to NNDescent_Actor, during *createNSG* the node_information_Actor?

### during NNDescent
**NN_Descent_Actor / knng_builder** ?
- hold edges for a specified set of nodes 
- update these edges
- calculate distances between neighbors and tell them
	- need to know where each g_node/neighbor is stored, to send messages
- find medoid for later NSG-distribution?

### during create_NSG
save knng in Coordinator, with read only by all other Actors

**create_NSG_Actor / candidate_finder** ? 
- one per search or one per thread?
- *searchOnGraph* method for finding edge candidates to send to merge_NSG_Actor
- don't be idle while waiting for *searchOnGraph* answers from other nodes(node_information_Actor)
- maybe combine with node_information_actor?

**node_information_Actor / neighbor_finder**
- send edge information for specific g_nodes on c_node (*searchOnGraph* questions from other nodes)
- just part of Coordinator?
- just send neighbors or already calculate distances (and maybe cull the ones that are already too long)

**merge_NSG_Actor / NSG_holder**
- do edge selection on candidate sets
- combine calculated edgesto the final (sub)graph
- *searchOnGraph* returning just the final answer (for querying and connectivity)
- add connectivity edges

**API**: 
- get queries and run on NSG 
- forward to correct actor on correct node (in Communication with graphCoordinator?)