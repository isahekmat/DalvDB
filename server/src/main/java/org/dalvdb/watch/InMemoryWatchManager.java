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

package org.dalvdb.watch;

import com.google.common.annotations.VisibleForTesting;
import dalv.common.Common;
import io.grpc.stub.StreamObserver;
import org.dalvdb.DalvConfig;
import org.dalvdb.proto.BackendProto;
import org.dalvdb.proto.ClientProto;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryWatchManager implements WatchManager {
  private static InMemoryWatchManager instance;

  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final Map<String, List<StreamObserver<BackendProto.WatchResponse>>> backendWatches = new HashMap<>();
  private final Map<String, Map<String, List<StreamObserver<ClientProto.WatchResponse>>>> clientWatches = new HashMap<>();
  private final ExecutorService watcherExecutor = Executors.newFixedThreadPool(
      DalvConfig.getInt(DalvConfig.WATCHER_THREAD_NUM));

  public synchronized static InMemoryWatchManager getInstance() {
    if (instance == null) {
      instance = new InMemoryWatchManager();
    }
    return instance;
  }

  private InMemoryWatchManager() {
  }

  @Override
  public void addBackendWatch(String key, StreamObserver<BackendProto.WatchResponse> responseObserver) {
    lock.writeLock().lock();
    try {
      backendWatches.putIfAbsent(key, new LinkedList<>());
      backendWatches.get(key).add(responseObserver);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void addClientWatch(String userId, String key, StreamObserver<ClientProto.WatchResponse> responseObserver) {
    lock.writeLock().lock();
    try {
      clientWatches.putIfAbsent(userId, new HashMap<>());
      clientWatches.get(userId).putIfAbsent(key, new LinkedList<>());
      clientWatches.get(userId).get(key).add(responseObserver);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void notifyChange(String userId, List<Common.Operation> operations) {
    operations.forEach(operation -> notifyChange(userId, operation));
  }

  @Override
  public void notifyChange(String userId, Common.Operation operation) {
    lock.readLock().lock();
    try {
      watcherExecutor.submit(() -> backendNotifyChange(userId, operation));
      watcherExecutor.submit(() -> clientNotifyChange(userId, operation));
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void cancelAllClientWatch(String userId) {
    lock.writeLock().lock();
    try {
      Map<String, List<StreamObserver<ClientProto.WatchResponse>>> map = clientWatches.remove(userId);
      if (map == null) return;
      map.values().forEach(soList -> soList.forEach(StreamObserver::onCompleted));
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void cancelClientWatch(String userId, String key) {
    lock.writeLock().lock();
    try {
      Map<String, List<StreamObserver<ClientProto.WatchResponse>>> map = clientWatches.remove(userId);
      if (map == null) return;
      List<StreamObserver<ClientProto.WatchResponse>> soList = map.get(key);
      if (soList == null) return;
      soList.forEach(StreamObserver::onCompleted);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void cancelAllBackendWatch() {
    lock.writeLock().lock();
    try {
      backendWatches.values().forEach(soList -> soList.forEach(StreamObserver::onCompleted));
      backendWatches.clear();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void cancelBackendWatch(String key) {
    lock.writeLock().lock();
    try {
      List<StreamObserver<BackendProto.WatchResponse>> list = backendWatches.remove(key);
      if (list == null) return;
      list.forEach(StreamObserver::onCompleted);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void clientNotifyChange(String userId, Common.Operation operation) {
    List<StreamObserver<ClientProto.WatchResponse>> streamObservers = clientWatches.get(userId)
        .getOrDefault(operation.getKey(), null);
    if (streamObservers == null) return;
    for (StreamObserver<ClientProto.WatchResponse> o : streamObservers) {
      o.onNext(ClientProto.WatchResponse.newBuilder()
          .setOperation(operation)
          .build());
    }
  }

  private void backendNotifyChange(String userId, Common.Operation operation) {
    List<StreamObserver<BackendProto.WatchResponse>> streamObservers = backendWatches.get(operation.getKey());
    if (streamObservers == null) return;
    for (StreamObserver<BackendProto.WatchResponse> o : streamObservers) {
      o.onNext(BackendProto.WatchResponse.newBuilder()
          .setOperation(operation)
          .setUserId(userId)
          .build());
    }
  }

  @Override
  public void close() {
    lock.writeLock().lock();
    try {
      backendWatches.values().forEach(streamObservers -> streamObservers.forEach(StreamObserver::onCompleted));
      clientWatches.values().forEach(map -> map.values().forEach(
          streamObservers -> streamObservers.forEach(StreamObserver::onCompleted)));
    } finally {
      lock.writeLock().unlock();
    }
  }

  @VisibleForTesting
  void clear() {
    backendWatches.clear();
    clientWatches.clear();
  }
}
