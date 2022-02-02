# go_KeyValueStore
A causally consistent, replicated and sharded key value store with a RESTful API. Runs through the use of a Docker container.

Given a system that is replicated and sharded across numerous nodes, we have an interface that practices Representational State Transfer (REST),
wherein certain endpoints, when accessed, can either:

- Add a key value pair to the store
- Update a key value pair to the store
- Delete a key value pair in the store
- Print out all key value pairs in the store (across all shards)

All operations are completely causually consistent, where causal order of events are enforeced (similar to a Distributed System).

**Mechanism Description:**
How the system detects when a replica goes down:
The system can detect when a replica is down when we broadcast messages (and we only broadcast messages that we receive
from the client that also change our database i.e. we don't broadcast GETs). It detects a down replica when it first pings 
that replica whilst waiting for a timeout -- if the ping times out within 1 second, then we consider the replica down. After 
this step we then update the views of all other replicas to reflect this.

How the system tracks causal dependencies:
Our system is very similar to CBCAST Vector Clocks, wherein vector clocks increment at the sender
index on Sends. In order to detect violations, we first check if the metadata is nil, if it isnt, then we check if the metadata
is from the client. If it is, we make sure that the request vector is <= the local vector at all indexes or else there's a consistency
violation. If the metadata is from a replica, then we make sure the vector clock's value at the senders index is 1 greater than
the local vector's and <= at all other indexes or else there is another violation.

How the system divides nodes into shards:
Our system divides nodes into shards by first keeping all the node IP's in an array. We then modulo the index of each of these IP's
by the number of shards, which will return a number from 0 to shardCount - 1. We then decide which shard the node belongs to based
off of that (we essentially evenly distribute the nodes to shards).

How the system shards keys across nodes:
First we created a hash function, which takes a go hash library and returns an integer which we then modulo with the shard count to
produce an integer between 0 and shardCount-1 (lets call this value x). we then have variable 'hashToIndexArr' which is an array of 
all the shard ID's. We can find the key to shard mapping by indexing the array by the value x we got earlier to know what shard the 
key belongs to.
