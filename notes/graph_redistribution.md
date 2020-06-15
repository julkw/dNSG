# Graph Redistribution

- When?
    - primarily between building the AKNNG and building the NSG
    - potentially also between building the first approximate AKNNG and NNDescent
      - in that case, the connectivity to the NavigatingNode would not be important
      - -> last c_node just gets all leftover nodes including the ones with several designated c_nodes (close to the root)?
- Based on what?
    - a distributed graph (Map from g_node index to Seq[neighbor index] in the order of distance to the g_node)
    
## Steps
### Build Tree
- Build a tree over the graph with the Navigating Node as Root
  - every tree_node knows 
    - its number of children (including grandchildren etc)
    - which of its neighbors are its direct children 
    - its parent
- Ensure connectivity of tree 
  - potentially add neighbors to graph
  - this would require searchOnGraph again
    - send GetKNearestNeighbors to searchOnGraph?
    - This should be broken right now because of the different states. oops.
- Decide on range of number of g_nodes per c_node

### Choose c_nodes for subtrees
- Do Depth-First-Search to find first tree_node with #children + #parents smaller than or in range
  - if in range -> this node and all its children are designated one c_node
  - if smaller than range ->
    - add node to waitingList
    - backtrack one step and continue DFS for a node with #children + #parents smaller or in range - #children of all nodes in waitingList
      - if smaller -> add to waitingList
      - if in -> all nodes in the waitingList are designated one c_node
  - continue search from parent of now designated node
  - for this I need to know which children were already searched
    - could be done by checking if already designated a c_node, but that information does not lie with the parent
    - to avoid extra datastructure just remove "searched" nodes from children and update #children accordingly

#### Questions / Problems / Improvements
- after adding a node to the waiting list, continue the search with
  - a random other child of the parent
    - -> different subtrees on one node might not be close to each other
  - the closest tree_node to the tree_node that was last added to the waitingList
    - -> extra step in protocol to get locations
- Ranges: 
  - What if all c_nodes are designated nodes in the lower end of the range and some nodes are left over?
    - update ranges after each c_node designation with the number of leftover g_- and c_nodes
  - What formulas to use for which data replication strategy?
    - g = #g_nodes to distribute
    - c = #c_nodes to distribute to
      - No data replication:
        - g/c +- g/(100(c-1))
      - Some data replication:
        - g/c - g/(100(c-1))
      - max data replication:
        - g/c - g/(100(c-1))
    - When using replication the last node WILL fall short of range, because the shared nodes aren't counted in the current algorithm
      - if using data replication, add parents to in-range-calculation?
      - in that case always use same range (the no replication one)

### Designate c_nodes for tree_nodes
- once a tree_node is designated a c_node it shares this designation with all its children (who share it with their children etc)
- The first tree_node to be designated also designates its parent (who tell their parent etc)
  - This ensure connectivity between each distribution and the navigatingNode
  - If NO data replication:
      - The ranges are chosen with no data replication in mind
      - the last c_node takes all "not yet claimed" tree_nodes
      - those include the shared ones, as they don't count for the comparison agains range
      - parents c_nodes are only updated, if this is last distribution
        - all tree_nodes only expect one c_node
  - If SOME data replication:
    - every tree_node is send to all designated c_nodes it received during this process
      - -> there is a path from the NavigatingNode to each node in the distribution
      - but there will still be a lot of asking for locations in the first few steps for the other neighbors of the Navigating Node
  - If extra setting?
    - Every c_node gets every tree_node up to a certain depth and the rest are distributed based on their designated c_nodes from the algorithm
  - If MAX data replication:
    - Every tree_node with more than one designated c_node gets send to every c_node
  
### Send g_nodes to their new holders
- location to dataHolder of c_node
- graph info to correct worker (searchOnGraph Actor?)
  - since the whole process starts with finding the NavigatingNode, the SearchOnGraph Actors are already started
  - They then send the new graph/responsibilities to the NSGWorkers
    - Same for KNNGWorkers? 
    - They would need to be adjusted to accept a new graph
- determine dataHolder by comparing adress to worker?
- each worker bundles its g_nodes by their location and sends them in bulk

## Need to know at every step
- data replication strategy (actor parameter?)
- c_nodes / workers to distribute to
  - Sidenote: Which do I do? 
    - The data (locations of g_nodes) goes to the c_nodes
    - the graph data goes to workers
    - Distribute to workers and match to dataHolders by address?
    - Idea: Two different searches for c_node and worker distribution? 
      - after having found c_node distribution, continue with worker distribution 
      - worker distribution can be done in parallel
      - would need a very similar but different protocol to run in parallel
        - would need to switch between subtree roots instead of backtracking when having reached a subtree root 
  - c_node represented by dataHolder here
- the number of leftover c_nodes and g_nodes
  - include in every message in search?
    - Set/Seq of who to distribute to (if in Seq ordered by c_node)
    - number of not yet distributed g_nodes
- the number of parents for a node
- waitingList (nodeIndex and its #children)
- nodeLocator for TreeBuildingActors
- nodeLocator for SearchOnGraphActors
  - for establishing connectivity
- the tree