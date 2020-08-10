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
import com.google.protobuf.InvalidProtocolBufferException;
import org.dalvdb.proto.ClientProto;
import org.rocksdb.RocksDBException;

public class DalvClient {
  private final String[] addressArr;
  private int nextAddressIndex = 0;
  private final String accessToken;
  private DalvConnector connector;
  private final Storage storage;
  private final Object syncLock = new Object();

  public DalvClient(String serverAddresses, String accessToken, String dataDir) throws RocksDBException {
    this.accessToken = accessToken;
    addressArr = serverAddresses.split(",");
    this.storage = new Storage(dataDir);
    createNextConnector();
  }

  public void sync() throws RocksDBException, InvalidProtocolBufferException {
    synchronized (syncLock) {
      int lastSnapshotId = storage.getLastSnapshotId();
      System.out.println("sync by lastSnapshotId = " + lastSnapshotId);
      ClientProto.SyncResponse res = connector.sync(storage.getUnsynced(), lastSnapshotId);
      if (res.getSyncResponse() == ClientProto.RepType.OK) {
        storage.apply(res.getOpsList(), res.getSnapshotId());
      } else {
        System.out.println("ERROR");
      }
    }
  }

  public void put(String key, byte[] val) throws RocksDBException { //TODO: accept each type and serialize/deserialize
    ClientProto.Operation op = ClientProto.Operation.newBuilder()
        .setKey(key)
        .setVal(ByteString.copyFrom(val))
        .setType(ClientProto.OpType.PUT)
        .build();
    synchronized (syncLock) {
      storage.put(op);
    }
  }

  public byte[] get(String key) throws RocksDBException {
    byte[] bytes = storage.get(key);
    if (bytes == null)
      return new byte[0];
    return bytes;
  }

  private void createNextConnector() {
    connector = new DalvConnector(addressArr[nextAddressIndex++], accessToken);
  }

  public static void main(String[] args) throws RocksDBException, InvalidProtocolBufferException {
    DalvClient client = new DalvClient("localhost:7472",
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOiJlc2EifQ.Gd3BoQIu3tAX2rxlKsgUMJkG38MbDZxoYmKOQfJ9N4g",
        "client-data");
//    client.put("color", "blue".getBytes());
    client.put("theme", "white".getBytes());
    client.sync();
    System.out.println(new String(client.get("name")));
    System.out.println(new String(client.get("age")));
    System.out.println(new String(client.get("lname")));
    System.out.println(new String(client.get("color")));
    System.out.println(new String(client.get("theme")));
    System.out.println("Done");
  }

}
