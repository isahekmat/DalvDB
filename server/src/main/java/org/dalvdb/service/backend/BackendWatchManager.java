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

package org.dalvdb.service.backend;

import dalv.common.Common;
import io.grpc.stub.StreamObserver;
import org.dalvdb.DalvConfig;
import org.dalvdb.proto.BackendProto;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BackendWatchManager implements WatchManager, Closeable {

  private final Map<String, List<StreamObserver<BackendProto.WatchResponse>>> watches = new HashMap<>();
  private final ExecutorService watcherExecutor = Executors.newFixedThreadPool(
      DalvConfig.getInt(DalvConfig.WATCHER_THREAD_NUM));

  public synchronized void addWatch(List<String> keysList, StreamObserver<BackendProto.WatchResponse> responseObserver) {
    keysList.forEach(key -> {
      watches.putIfAbsent(key, new LinkedList<>());
      watches.get(key).add(responseObserver);
    });
  }

  public void notifyChange(String userId, List<Common.Operation> operations) {
    operations.forEach(operation ->
        watcherExecutor.submit(() -> internalNotifyChange(userId, operation)));
  }

  private void internalNotifyChange(String userId, Common.Operation operation) {
    String key = operation.getKey();
    List<StreamObserver<BackendProto.WatchResponse>> streamObservers = watches.get(key);
    if (streamObservers == null) return;
    for (StreamObserver<BackendProto.WatchResponse> o : streamObservers) {
      o.onNext(BackendProto.WatchResponse.newBuilder()
          .setOperation(operation)
          .setUserId(userId)
          .build());
    }
  }

  @Override
  public synchronized void close() throws IOException {
    watches.values().forEach(streamObservers -> streamObservers.forEach(StreamObserver::onCompleted));
  }
}
