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
import org.dalvdb.client.conflict.Conflict;
import org.dalvdb.client.conflict.ConflictResolver;
import org.dalvdb.proto.ClientProto;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DalvClient {
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
      storage.put(op);
    }
  }

  public void delete(String key) {
    Common.Operation op = Common.Operation.newBuilder()
        .setKey(key)
        .setType(Common.OpType.DEL)
        .build();
    synchronized (syncLock) {
      storage.put(op);
    }
  }

  public byte[] get(String key) {
    byte[] bytes = storage.get(key);
    if (bytes == null)
      return new byte[0];
    return bytes;
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
}
