# Introduction
A considerable amount of data generated from each online service belong to its users. Such data are modified by user themselves or 
other server side components. Conventional partitioning methods pigeonholing data based on user identifier is useful to
spread out data to multiple machines or even multiple data-centers in the server side, usually keeping data near user's
location to reduce read latency.

DalvDB leverages modern client's capability to store data as stateful clients, to move forward this 
partitioning technique to its end by storing a replica of the user data on him/her client's device. This method is not 
something new and many applications and online services already using this approach to reduce the read latency and the 
pressure of read queries on their server side components. They usually store some part of data in client device and using
it as a cache to improve customer experience by reducing read latency.

DalvDB tries to generalize this method by providing developers a general-purpose API, while caring about authorized 
updates carried out by user to data. It allows user to update their data and synchronize their updates with server, however it
just allows modification on the part of the data that is owned by the user itself. As it handles updates, it's not a
simple cache anymore, however some part of data could act as a cache to reduce the amount of read requests to 
server, but DalvDB is not limited to cache.

## Use Cases
There are several examples of data owned by user him/her self and might be changed only by the user themselves. The use cases include 
but not limited to:
- User preferences
- Shopping card
- profile info
- saved posts/products/pages
- search history
- following list

Also, virtually everything that could be cached on server side has this capability to cache on client device, 
despite the fact that the user is the owner of the data or not, for example:
- Order history
- List of follower
- Timeline
- Product categories
