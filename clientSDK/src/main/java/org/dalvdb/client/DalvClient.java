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
import org.dalvdb.client.conflict.Conflict;
import org.dalvdb.client.conflict.ConflictResolver;
import org.dalvdb.client.conflict.resolver.AcceptServerResolver;
import org.dalvdb.proto.ClientProto;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DalvClient {
  private final String[] addressArr;
  private int nextAddressIndex = 0;
  private final String accessToken;
  private DalvConnector connector;
  private final Storage storage;
  private final Object syncLock = new Object();

  public DalvClient(String serverAddresses, String accessToken, String dataDir) {
    this.accessToken = accessToken;
    addressArr = serverAddresses.split(",");
    this.storage = new Storage(dataDir);
    createNextConnector();
  }

  public void sync(ConflictResolver resolver) {
    SyncResponse res = sync();
    List<ClientProto.Operation> resolveOps = new LinkedList<>();
    for (Conflict conflict : res.getConflicts()) {
      resolveOps.addAll(resolver.resolve(conflict));
    }
    resolve(res.getResolvedSnapShot(), resolveOps);
    sync(); //retry
  }

  public SyncResponse sync() {
    synchronized (syncLock) {
      int lastSnapshotId = storage.getLastSnapshotId();
      List<ClientProto.Operation> unsynced = storage.getUnsynced();
      ClientProto.SyncResponse res = connector.sync(unsynced, lastSnapshotId);
      if (res.getSyncResponse() == ClientProto.RepType.OK) {
        storage.apply(res.getOpsList(), res.getSnapshotId());
        return new SyncResponse(res.getSnapshotId());
      } else {
        return extarctConflicts(unsynced, res.getOpsList(), res.getSnapshotId());
      }
    }
  }

  public void resolve(int resolveSnapshotId, List<ClientProto.Operation> resolveOps) {
    storage.resolveConflict(resolveSnapshotId, resolveOps);
  }

  private SyncResponse extarctConflicts(List<ClientProto.Operation> unsynced,
                                        List<ClientProto.Operation> opsList,
                                        int snapshotId) {
    Map<String, List<ClientProto.Operation>> unsyncedMap = new HashMap<>(unsynced.size());
    for (ClientProto.Operation op : unsynced) {
      putToMap(unsyncedMap, op);
    }
    Map<String, List<ClientProto.Operation>> conflictMap = new HashMap<>();
    for (ClientProto.Operation op : opsList) {
      if (!unsyncedMap.containsKey(op.getKey()))
        continue;
      putToMap(conflictMap, op);
    }

    List<Conflict> conflictList = new LinkedList<>();
    for (String key : conflictMap.keySet()) {
      conflictList.add(new Conflict(conflictMap.get(key), unsyncedMap.get(key)));
    }
    return new SyncResponse(conflictList, snapshotId);
  }

  private void putToMap(Map<String, List<ClientProto.Operation>> map, ClientProto.Operation op) {
    List<ClientProto.Operation> val = map.get(op.getKey());
    if (val == null) {
      val = new LinkedList<>();
      val.add(op);
      map.put(op.getKey(), val);
    }
    val.add(op);
  }

  public void put(String key, byte[] val) { //TODO: accept each type and serialize/deserialize
    ClientProto.Operation op = ClientProto.Operation.newBuilder()
        .setKey(key)
        .setVal(ByteString.copyFrom(val))
        .setType(ClientProto.OpType.PUT)
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

  private void createNextConnector() {
    connector = new DalvConnector(addressArr[nextAddressIndex++], accessToken);
  }

  public static void main(String[] args) {
    DalvClient client = new DalvClient("localhost:7472",
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOiJlc2EifQ.Gd3BoQIu3tAX2rxlKsgUMJkG38MbDZxoYmKOQfJ9N4g",
        ".client-data");
    client.put("theme", "blue".getBytes());
    client.sync(new AcceptServerResolver());

    System.out.println(new String(client.get("theme")));
    System.out.println("Done");
  }

}
