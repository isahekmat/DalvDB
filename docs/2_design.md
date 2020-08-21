# Design
## System overview
DalvDB system could be divided into two general parts, clients and servers. Client is any software running on the end-user 
device, server is any instance of Dalv server. Backend application is the backend application running on the servers
other than Dalv nodes. Clients could connect to Dalv servers as they can connect to backend application.
Number of clients is equal or greater than the system users which their data stored in the DalvDB servers 
(greater because each user could use several devices). Clients could be online or offline, synced or out of sync,
and they are usually some application with underlying storage capability, like Mobile applications, Single Web Page applications, or desktop applications.

Servers managed by the service owner as a cluster of (virtual)machines, connected to each other over network. They could be 
laid on different data-centers. Each online client could contact the server which contains its user data (leader) by a
TCP call (gRPC).

Whole data, partitioned base on user and each user data stored in several servers to guarantee durability. For each 
partition there is just one leader server and a configurable number of followers (By default 2). Clients should establish their 
connections to the leader of their data. In case of leader failure, cluster elects a new leader for each 
partition that the crashed leader was located on. New leaders (Each server is a leader of several partitions, so each of
these partitions could find a new leader) are the previous followers and after elections client should contact with its
new leader. Each user data has just one leader and each server could be a leader of several users and a follower of several other users.

It's important to note that if client connects to a wrong server, the server could tell it the address of its leader. 
so client could contact each server and if the server wasn't its leader, it could then figure it out by the response 
from the wrong server and contact its own leader on the second try.

User data are in key/value form. There are two types of key, 1) Keys that user owned their values. 2) keys that system owned their values.
For keys of type 1, which are owned by user, updates could be initiated by backend system or by updates made by user 
in his/her own copy and passed by SYNC command to servers. 
For keys of type 2, which are owned by system, updates could only be applied by backend system and client only can read them by SYNC command.

## Consistency
There are two types of data, user owned data could be updated on client devices even if the user is offline. 
System owned data could be just updated on server side, and the client just has a copy of it.

For system owned data, as there is just one writable point for them (the leader), all writes are handled sequentially 
in the leader. Even if several backend-servers try to update a single data, leader do their updates sequentially by 
locking the user bucket.

For data owned by user, as they could be updated both by client and server sides in parallel, a conflict 
resolver is needed.

### Client side conflict resolver
During synchronization, clients send all updates along with a snapshot-Id, which indicates the last point of data
that client synced previously. we talk about snapshots and snapshot Ids later.
The leader checks whether or not any keys that clients wants to update changed after the snapshot point which client is aware of. If 
any key changed, server rejects the client updates and sends all updates happened after the client snapshot-id, it also 
provides a new snapshot-id in its response. Now this is client's responsibility to merge all the updates
happened in server with its own updates. After merging them, it can send a SYNC request again with new snapshot-id 
and updates (resulted from merging). 

## Snapshot and SnapshotId
Dalv servers store the user data as an append-only list of log entries. Each update transforms to a single log entry, however
there is another type of log entry, 'snapshot'. Snapshots are the special points in the logs which clients could refer to.
Snapshot automatically added by the Dalv server after each Sync command which results in returning updates. Server does 
not generate new snapshot if Sync contains no updates and result is empty, in other words, at least one of these conditions should be 
met to generate a new snapshot.

## Commands
Clients and backend servers talk to Dalv servers via Commands. Each command except WATCH entails a short-living TCP 
connection initiated by requester and may carry some information (usually updates).

### Sync Command (Client only)
Clients could manipulate the data (only user-owned keys) offline. These manipulations contain several 'updates' entry.
Update entry could be an assignment (PUT) or deletion (DELETE) and reading data does not generate 'update' entry.
At some point in time, client will decide to send their updates to the server and receive updates happened in the server. 
At this point, client should send a SYNC Request to server containing:
- the last snapshotId that client is aware of, and
- a list of all updates happened after last sync
If server is not the leader of user data, it will respond the address of the leader.

if the server is the leader, then it may not accept the updates because it has detected some conflicts. If server is
the leader, in any case (accept the updates or not) its response contains:
- the list of the updates since the last snapshotId provided by the client (it will entail the updates that client has 
sent by this SYNC command, if the updates had been accepted)
- an indicator which indicates if the updates from user accepted by server or not.(OK or NOK)
- SnapshotId which indicates the point exactly after the updates carried by response. (if nothing happened 
since last sync it will be the last snapshotId client provided)

If server rejects client's updates, it is client's responsibility to merge received updates by its own and
send another SYNC command with new snapshotId provided in the first try.
A striking feature of SYNC command is that it's Atomic, all or nothing. There is no chance that some updates applied 
and some of them not.
### Update Command (Backend only)
Assuming that Backend-servers are online all the time, we'd expect Dalv servers be informed by all manipulations
applied on user data immediately, meaning that backend-servers should send Update commands to the Dalv server for each update, 
although, backend-servers may want to write several keys in an Atomic operation whereby they can send a list of 
updates by one Update command. Update request consists of:
- list of updates
and its response has an indication of whether the updates applied successfully.

### Watch
WATCH is a mechanism for backend-server and clients to receive notification once some particular keys are updated. For this purpose, 
they should establish a long-running TCP connection to the leader and send a list of keys they are interested in.
Backend servers could subscribe keys for all users, for example they can request, please notify me if any user updates its 
theme color stored under the "theme.color" key. Clients could only subscribe to their own data. For example a 
client may rrequest, please notify me if my timeline list, which is stored under "timeline" key, is updated.

### Propagate Updates (Dalv-to=Dalv)
Dalv servers do not respond back to the requester of SYNC or UPDATE commands until it makes sure the updates get
stored on the majority of the replicas. Thus, for each SYNC or UPDATE commands, the leader assigns a chronological 
TransactionId and sends all updates stamped by this TransactionId to the user data followers. Leader sends the response
as soon as it receives the acknowledgment from the majority of the replicas(including itself).
For example if there are 2 followers and one leader, the leader will send the response to the requester as soon as it gets 
the first propagation acknowledgment. We will talk more about this mechanism in 'Servers coordination - Updates propagation' section. 

## Partitioning
Dalv partitions data based on User, each user data is a separated partition. Thus, for each user there is a partition on
the Dalv servers. Usually the number of partitions are far more than servers in the cluster, so each server should act as
leader for several partitions, it may also be a follower for several other partitions.
Followers and the leader of a partition, form a mini-cluster and each server could be a part of several mini-clusters.
The partitioning mechanism used in Dalv is consistent-hashing and each server on start-up chooses several hashes on the ring
and puts its hashes on the zookeeper, so each partition has a hash produced based on the UserId. If we move clockwise on the 
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
