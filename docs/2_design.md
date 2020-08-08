# Design
## System overview
DalvDB system could be divided to two general parts, clients and servers. Client is any software running on the end-user 
device, server is the any instance of Dalv server. backend application is the backend application running on the servers
other than Dalv nodes. clients could connect to Dalv servers as they can connect to backend application.
Number of clients is equal or greater than the user of the system which their data stored in the DalvDB servers 
(greater because each user could use several devices). Clients could be online or offline, synced or out of sync,
and they are usually some application with underlying storage capability, like Mobile applications, Single Web Page applications, or desktop applications.

Servers managed by the service owner as a cluster of (virtual)machines, connected to each other over network. They could be 
laid on different data-centers. Each online client could contact the server which contains its user data (leader) by a
TCP call (gRPC).

Whole data, partitioned base on user and each user data stored in several servers to guarantee durability. For each 
partition there is just one leader server and configurable number of followers (By default 2). Clients should establish their 
connections to the leader of their data. In the case of leader failure, cluster elect a new leader for each 
partition that was on the crashed leader. New leaders (because each server is a leader of several partitions, so each of
these partitions could find a new leader) are the previous followers and after elections client should contact new its
new leader. Each user data has just one leader and each server could be leader of several users and follower of several other users.

It's important to note that if client connect to a wrong server, the server could tell it the address of its leader. 
so client could contact each server and if the server wasn't its leader, it could then figure it out by the response 
from the wrong server and contact its own leader on the second try.

User data are in key/value form. There are two types of key, 1) Keys that user owned their value. 2) keys that System owned their values.
For keys type 1, which are owned by the user, updates could be initiated by the backend system or by updates that user 
make in his/her own copy and passed them by sync command to the servers. 
For keys type 2, keys that owned by the system, could just be updated from the backend system, and client could just read by sync command.

## Consistency
There are two types of data, user owned data could be updated on the client devices even if the user is offline. 
system owned data could be just updated on server side, and the client just has a copy of it.

For system owned data, as there is just one writable point for them (the leader), all writes are handled sequentially 
in the leader. even if several backend-server try to update a single data, leader do their updates sequentially by 
locking the user bucket.

For data owned by the user as they could be updated both by the client side and server in parallel, we need a conflict 
resolver.

### client side conflict resolver
During synchronization, clients send all of their updates and the snapshot-Id which indicate the last point of the data
that client synced previously. we talk about snapshots and snapshot Ids later.
The leader check if any key that clients wants to update, changed after the snapshot point which client is aware of or not. if 
any key changed, server reject the client updates and sends all updates happened after the client snapshot-id, it also 
provides a new snapshot-id in its response. now this is the responsibility of the client to merge all the updates
happened in the server with its own updates, after merging them it can send a sync request again with new snapshot-id 
and updates (resulted from merging). 

## Snapshot and SnapshotId
Dalv servers store the user data as an append-only list of log entries. each update transform to a single log entry, however
there is another type of log entry, 'snapshot'. snapshots are the special points in the logs, that clients could refer to them.
Snapshot automatically added by the Dalv server after each Sync command which result in returning updates. server does 
not generate new snapshot if Sync contains no updates and result is empty, at least one of these conditions should be 
meet to generate a new snapshot.

## Commands
Clients and backend servers talk to Dalv servers via Commands. each command except watch, entails a short-living TCP 
connection which is initiated by the requester and may carry some information (usually updates).

### Sync Command (Client only)
Clients could manipulate the data (only keys owned by the user) offline. These manipulations contain several 'updates' entry.
Update entry could be an assignment (PUT) or deletion (DELETE), reading data does not generate 'update' entry.
At some point in time client will decide to send their updates to the Server and receive updates happened in the server. 
At this point, client should send a Sync Request to the server containing:
- the last snapshotId that client is aware of, and
- a list of all updates happened after last sync
if the server is not the leader of the user data, it will respond the address of the leader.

if the server is the leader, then it may not accept the updates because it has detected some conflicts. if the server is
the leader, in any case (accept the updates or not) it's response contains:
- the list of the updates since the last snapshotId provided by the client (it will entail the updates that client has 
sent by this sync command, if the updates had been accepted)
- an indicator which indicate if the updates from the user accepted by the server or not.(OK or NOK)
- SnapshotId which indicate the point exactly after the updates which is carried by the response. (if nothing happened 
since last sync it will be the last snapshotId client provided)

If the server reject the client updates, it is the client responsibility to merge the received updates by its own and
send another Sync command with new snapshotId provided in the first try.
One important aspect of Sync command is that it's atomic, all or nothing. There is no chance that some updates applied 
and some of them not.
### Update Command (Backend only)
As we have assumed that Backend-servers are online all the time, we expect Dalv servers informed for all manipulations
to the user data immediately, so backend-servers should send Update commands to the Dalv server for each update, 
although, backend-servers may want to write several keys at an atomic operation, in this case they can send a list of 
updates by one Update command. Update request consist of:
- list of updates
and its response has an indication that if the updates applied successfully or not.

### Watch
Watch is a mechanism for backend-server and clients, to get notification when some particular key updated. for this purpose, 
they should establish a long-running TCP connection to the leader, and send the list of keys that they are interested in,
backend servers could subscribe keys for all users, for example they can say, please notify me if any user updates its 
theme color which is stored under the "theme.color" key. clients could only subscribe to their own data. for example a 
client may say, please notify me if the list of my timeline updated, timeline data stored under "timeline" key.

### Propagate Updates (Dalv-to=Dalv)
Dalv servers does not response back to the requester of Sync or Update commands until it makes sure the updates get
stored on the majority of the replicas. Thus, for each Sync or Update command, the leader assign a chronological 
TransactionId and send all updates stamped by this transactionId to the user data followers, The leader send the response
as soon as it takes the acknowledgment from the majority of the replicas(including itself).
for example if there are 2 followers and one leader, the leader will send the response to the requester as soon as it gets 
the first propagation acknowledgment. we will talk more about this mechanism in 'Servers coordination - Updates propagation' section. 

## Partitioning
Dalv partitions data based on User, each user data is a separated partition. Thus, for each user there is a partition on
the Dalv servers. usually the number of partitions are far more than servers in the cluster, so each server should act as
leader for several partitions, it may also be a follower for several other partitions.
Followers and the leader of a partition, form a mini-cluster, each server could be a part of several mini-cluster.
The partitioning mechanism used in Dalv is consistent-hashing, each server on start-up, choose several hashes on the ring,
and put its hashes on the zookeeper, each partition has a hash produced based on the UserId, if we move clockwise on the 
ring, from the hash of the partition, next 3(or more based on the replication configuration) hashes indicate the 
mini-cluster of the partition, the first one would be the leader.

## Servers coordination
Server nodes use Zookeeper to maintain their coordination. Servers could talk to each 
other in order to re-balancing(on the case of joining/removing nodes) and propagating updates from leaders to followers. 

### Updates Propagation
TODO
#### Filling the Holes
TODO
### Failure handling and Leader Election
TODO
### Re-balancing
TODO
