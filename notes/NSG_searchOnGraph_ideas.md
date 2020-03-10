# How to best parallelize distributed searchOnGraph for finding NSG candidates?

one c_node responsible for n g_nodes

goal: 
- answer requests for neighbors from other nodes
  - include distance calculations?
- run n searches
  - include asking other nodes -> don't be idle while waiting for answers

ideas:
- one or an arbitrary number of actors (arbitrary numer > 1 => parallel)
  - see below 
  - if this kind of actor responsible for other nodes' request long reponse times 
- one actor per search
    - too many actors? (one per node...)
    - when idle hopefully automatically not blocking a thread
- **extra actor for answering other nodes' requests**
  - dont calculate distances, as that would take to long and there will only be one per node?

# Preventing ideling when one Actor is responsible of several g_nodes 

## Better idea aka Lose iterations
- do first iteration and send neighbors even if local like another node would respond
- everytime a message with neighbors is received update query and go to next step
  - if there is no next step, because all the g_nodes in the query have been checked, send candidates to correct actor and don't send any more messages about query to self
    - possibly count done queries to shutdown self when done with work
- repeat

Why better?
- actor not using ressources while waiting
- not having to skip waiting or done queries each iteration
- no weird restart messages necessary

## original idea
- keep iterating over nodes the actor is responsible for
- each iteration starts with a message to itself
- after an iteration is done this message is resend to start the next iteration
- each iteration only consideres the first node in the query
  - if neighbors local, update query or send message with info to self
    - more messages but possibly more time for other nodes to respond so the query does not need to skip an iteration 
  - if not, ask for neighbors from correct c_node and mark query as "waiting" 
- after each iteration the message queue is worked through
  - update correct queries (should state in the message which one) and and remove "waiting" mark
- during the next iteration skip all queries marked as still waiting for a response
- if a query is finished, mark as "done", send candidates to correct actor and skip in future iterations

- if all queries are waiting during an iteration do not send the next iteration message until next message is received (how to implement?)
  - maybe just a timer instead

