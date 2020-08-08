# Introduction
A considerable amount of data of each online service, belong to its users. these data modify by user him/her self or 
other server side components. previously partitioning methods, partition data based on user identifier is useful to
spread out data to multiple machines or even multiple data-centers in the server side, usually keeping data near to user
location to reduce read latency.

DalvDB leveraging modern client's capability for storing data as stateful clients, to move forward this 
partitioning technique to its end, by storing a replica of the user data on him/her client device. this method is not 
something new and many applications and online services already using this approach to reduce the read latency and the 
pressure of read queries on their server side components. They usually store some part of data i client device and using
it as a cache to improve customer experience by reducing read latency.

DalvDB tries to generalize this method by providing a general purpose API for developers while caring about authorized 
updates by user to the data. It allows user to update their data and synchronize their updates to the server, however it
just allows modification on the part of the data that is owned by the user itself. As it handles updates, it's not a
simple cache anymore, however some part of the data could act as a cache to reduce the amount of read requests to the 
server, but DalvDB is not limited to cache.

## Use Cases
There are several examples of the data that is owned by the user him/her self and might changed only by the user 
him/her self. To name some of them:
- User preferences
- Shopping card
- profile info
- saved posts/products/pages
- search history
- following list

Also, virtually everything that could be cached in the server side has this capability to cache in the client device, 
despite from the fact that the user is the owner of the data or not, for example:
- Order history
- List of follower
- Timeline
- Product categories