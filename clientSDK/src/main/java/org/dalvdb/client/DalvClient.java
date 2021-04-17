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

package org.dalvdb.client;

import com.google.protobuf.ByteString;
import dalv.common.Common;
import io.grpc.stub.StreamObserver;
import org.dalvdb.client.conflict.Conflict;
import org.dalvdb.client.conflict.ConflictResolver;
import org.dalvdb.common.util.ByteUtil;
import org.dalvdb.common.watch.WatchEvent;
import org.dalvdb.common.watch.Watcher;
import org.dalvdb.proto.ClientProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DalvClient implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(DalvClient.class);
  private final List<DalvConnector> connectors;
  private final Storage storage;
  private final Object syncLock = new Object();

  private DalvConnector currentConnector;
  private int currentConnectorIndex;

  DalvClient(List<DalvConnector> connectors, Storage storage) {
    if (connectors.size() < 1)
      throw new IllegalArgumentException("At least one connector should be existed");
    this.connectors = connectors;
    this.storage = storage;
    this.currentConnectorIndex = 0;
    this.currentConnector = connectors.get(0);
  }

  public void sync(ConflictResolver resolver) {
    synchronized (syncLock) {
      SyncResponse res = sync();
      if (res.hasConflict()) {
        List<Common.Operation> resolveOps = new LinkedList<>();
        for (Conflict conflict : res.getConflicts()) {
          resolveOps.addAll(resolver.resolve(conflict));
        }
        resolveOps.addAll(res.getOperationsWithoutConflict());
        resolve(res.getResolvedSnapShot(), resolveOps);
        sync(); //retry
      }
    }
  }

  public SyncResponse sync() {
    synchronized (syncLock) {
      int lastSnapshotId = storage.getLastSnapshotId();
      List<Common.Operation> unsynced = storage.getUnsyncOps();
      ClientProto.SyncResponse res = currentConnector.sync(unsynced, lastSnapshotId);
      if (res.getSyncResponse() == Common.RepType.OK) {
        storage.apply(res.getOpsList(), res.getSnapshotId());
        return new SyncResponse(res.getSnapshotId());
      } else {
        return extractConflicts(unsynced, res.getOpsList(), res.getSnapshotId());
      }
    }
  }

  public void resolve(int resolveSnapshotId, List<Common.Operation> resolveOps) {
    storage.resolveConflict(resolveSnapshotId, resolveOps);
  }

  public void put(String key, byte[] val) { //TODO: accept each type and serialize/deserialize
    Common.Operation op = Common.Operation.newBuilder()
        .setKey(key)
        .setVal(ByteString.copyFrom(val))
        .setType(Common.OpType.PUT)
        .build();
    synchronized (syncLock) {
      storage.apply(op);
    }
  }

  public void delete(String key) {
    Common.Operation op = Common.Operation.newBuilder()
        .setKey(key)
        .setType(Common.OpType.DEL)
        .build();
    synchronized (syncLock) {
      storage.apply(op);
    }
  }

  public boolean cancelWatch(String key) {
    return currentConnector.cancelWatch(key).getResponse() == Common.RepType.OK;
  }

  public boolean cancelAllWatch() {
    return currentConnector.cancelAllWatch().getResponse() == Common.RepType.OK;
  }

  public void watch(final String key, final Watcher watcher) {
    StreamObserver<ClientProto.WatchResponse> so = new StreamObserver<ClientProto.WatchResponse>() {

      @Override
      public void onNext(ClientProto.WatchResponse value) {
        WatchEvent event = new WatchEvent()
            .setNewValue(value.getOperation().getVal().toByteArray())
            .setOperationType(value.getOperation().getType())
            .setKey(key);
        watcher.process(event);
      }

      @Override
      public void onError(Throwable t) {
        logger.error("error occurred while processing watch event over key {}", key, t);
      }

      @Override
      public void onCompleted() {
        logger.info("watch stream closed for key {}", key);
      }
    };
    currentConnector.watch(key, so);
  }

  public List<byte[]> getAsList(String key) {
    byte[] bytes = storage.get(key);
    return ByteUtil.decodeList(bytes);
  }

  public byte[] get(String key) {
    List<byte[]> single = getAsList(key);
    if (single == null || single.size() != 1) return null;
    return single.get(0);
  }

  private SyncResponse extractConflicts(List<Common.Operation> unsynced,
                                        List<Common.Operation> opsList,
                                        int snapshotId) {
    List<Common.Operation> opsWithoutConflict = new LinkedList<>();
    Map<String, List<Common.Operation>> unsyncedMap = new HashMap<>(unsynced.size());
    for (Common.Operation op : unsynced) {
      putToMap(unsyncedMap, op);
    }
    Map<String, List<Common.Operation>> conflictMap = new HashMap<>();
    for (Common.Operation op : opsList) {
      if (!unsyncedMap.containsKey(op.getKey())) {
        opsWithoutConflict.add(op);
        continue;
      }
      putToMap(conflictMap, op);
    }

    List<Conflict> conflictList = new LinkedList<>();
    for (String key : conflictMap.keySet()) {
      conflictList.add(new Conflict(conflictMap.get(key), unsyncedMap.get(key)));
    }

    for (String k : unsyncedMap.keySet()) {
      if (!conflictMap.containsKey(k)) {
        opsWithoutConflict.addAll(unsyncedMap.get(k));
      }
    }
    return new SyncResponse(conflictList, opsWithoutConflict, snapshotId);
  }

  private void putToMap(Map<String, List<Common.Operation>> map, Common.Operation op) {
    List<Common.Operation> val = map.get(op.getKey());
    if (val == null) {
      val = new LinkedList<>();
      val.add(op);
      map.put(op.getKey(), val);
    } else
      val.add(op);
  }

  @Override
  public void close() {
    cancelAllWatch();
    this.storage.close();
    for (DalvConnector c : this.connectors) {
      c.close();
    }
  }
}
