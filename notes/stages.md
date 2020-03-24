# Stages on Cluster
## Read data
- One (Leader Dataholder) reads from file
- (lines / c_nodes) lines per node
- stream to other nodes
- save index offsets and which index range leads to which node

## Distribute data
- on c_node -> distributionTree for g_nodes
- send FinnishApproximateGraph to Leader Coordinator

## Build Approximate graph
- over all c_nodes (started by Leader Coordinator)
- normal Efanna candidates on c_node 
- Ask each other node for candidates from one leaf
    - send request to Coordinator (via receptionist?)
    - it uses its own distributionTree to ask correct actor
- when done tell Coordinator -> tells Leader Coordinator

## NNDescent
- over all c_nodes
- save ActorRef for neighbors on other nodes?
  - or hold distributionTree for all nodes?
  - NodePlacer class that holds distributionTrees and index ranges -> right tree?
- Termination 
  - once again tell own Coordinator -> tells Leader Coordinator
- Find Navigating Node?
- Save to file? 
  - Stream graph info to Leader Dataholder -> saves to file

## NSG 
- redistribute data
- new Worker Actors
- How to distribute