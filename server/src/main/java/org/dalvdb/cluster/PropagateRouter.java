/*
 * Copyright (C) 2020-present Isa Hekmatizadeh
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.dalvdb.cluster;

import dalv.common.Common;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.dalvdb.proto.PropagateProto;
import org.dalvdb.proto.PropagateServiceGrpc;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class PropagateRouter implements Router {
  private static PropagateRouter instance;
  private final Map<Node, Replicator> replicas;
  private final Map<String, AtomicInteger> opIds;
  private final Map<String, SortedMap<Integer, CompletableFuture<Void>>> futures;
  private final Map<String, AtomicInteger> lastCommitted;
  private final ExecutorService responseExecutor;

  public synchronized static PropagateRouter getInstance() {
    if (instance == null) {
      instance = new PropagateRouter();
    }
    return instance;
  }

  private PropagateRouter() {
    replicas = new ConcurrentHashMap<>();
    opIds = new ConcurrentHashMap<>();
    lastCommitted = new ConcurrentHashMap<>();

    //TODO: should split of for each user: Map<userId, SortedMap<OpId,CompletableFuture>>
    futures = new ConcurrentHashMap<>();
    responseExecutor = Executors.newFixedThreadPool(8);
  }

  @Override
  public CompletableFuture<Void> propagate(String userId, Common.Operation op) {
    Locator locator = Locator.getInstance();
    List<Node> nodes = locator.replicas(userId);
    CompletableFuture<Void> resp = new CompletableFuture<>();
    opIds.computeIfAbsent(userId, s -> new AtomicInteger());
    int opId = opIds.get(userId).getAndIncrement();
    futures.computeIfAbsent(userId, (u) -> Collections.synchronizedSortedMap(new TreeMap<>()));
    futures.get(userId).put(opId, resp);
    for (Node node : nodes) {
      if (!replicas.containsKey(node))
        replicas.put(node, new Replicator(node));
      replicas.get(node).replicate(userId, op, opId);
    }
    return resp;
  }

  @Override
  public void commit(String userId) {
    Locator locator = Locator.getInstance();
    List<Node> nodes = locator.replicas(userId);
    for (Node node : nodes) {
      replicas.get(node).commit(userId, lastCommitted.get(userId).get());
    }
  }

  private class Replicator {
    private PropagateServiceGrpc.PropagateServiceFutureStub client;
    private StreamObserver<PropagateProto.CommitRequest> commitStream;
    private ManagedChannel channel;
    private Node node;

    private Replicator(Node node) {
      this.node = node;
      channel = ManagedChannelBuilder.forAddress(node.getHost(), node.getPort())
          .usePlaintext().build();
      client = PropagateServiceGrpc.newFutureStub(channel);
      commitStream = PropagateServiceGrpc.newStub(channel).commit(new StreamObserver<>() {
        @Override
        public void onNext(Common.Empty value) {
        }

        @Override
        public void onError(Throwable t) {
        }

        @Override
        public void onCompleted() {
        }
      });
    }

    public void replicate(String userId, Common.Operation op, int opId) {
      PropagateProto.PropagateRequest req = PropagateProto.PropagateRequest.newBuilder()
          .setUserId(userId)
          .setOp(op)
          .setOpId(opId)
          .build();

      client.propagate(req).addListener(() -> futures.get(userId).headMap(opId).forEach((e, c) -> {
        //TODO: assume that the replication factor is 3 so the first response received means that the operation should
        // be committed
        c.complete(null);
        futures.get(userId).remove(e);
        lastCommitted.computeIfAbsent(userId, s -> new AtomicInteger(opId));
        do {
          int last = lastCommitted.get(userId).get();
          if (last < opId) {
            if (lastCommitted.get(userId).compareAndSet(last, opId))
              break;
          } else break;
        } while (true);
      }), responseExecutor);
    }

    public void commit(String userId, int lastCommittedOpId) {
      commitStream.onNext(PropagateProto.CommitRequest.newBuilder()
          .setUserId(userId)
          .setLastCommitedId(lastCommittedOpId)
          .build());
    }
  }
}

