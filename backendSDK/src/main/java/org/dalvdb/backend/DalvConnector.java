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

package org.dalvdb.backend;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.dalvdb.proto.BackendProto;
import org.dalvdb.proto.BackendServerGrpc;

class DalvConnector {
  private final BackendServerGrpc.BackendServerBlockingStub client;

  public DalvConnector(String address) {
    String[] addressArr = address.split(":");
    final String host = addressArr[0];
    final int port = Integer.parseInt(addressArr[1]);
    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    client = BackendServerGrpc.newBlockingStub(channel);
  }

  public BackendProto.PutResponse put(String userId, String key, ByteString value) {
    BackendProto.PutRequest request = BackendProto.PutRequest.newBuilder()
        .setUserId(userId)
        .setKey(key)
        .setValue(value)
        .build();
    return client.put(request);
  }

  public BackendProto.DelResponse del(String userId, String key) {
    BackendProto.DelRequest request = BackendProto.DelRequest.newBuilder()
        .setUserId(userId)
        .setKey(key)
        .build();
    return client.del(request);
  }

  public BackendProto.GetResponse get(String userId, String key) {
    BackendProto.GetRequest request = BackendProto.GetRequest.newBuilder()
        .setUserId(userId)
        .setKey(key)
        .build();
    return client.get(request);
  }

  public BackendProto.AddToListResponse addToList(String userId, String listKey, ByteString value) {
    BackendProto.AddToListRequest request = BackendProto.AddToListRequest.newBuilder()
        .setUserId(userId)
        .setListKey(listKey)
        .setValue(value)
        .build();
    return client.addToList(request);
  }

  public BackendProto.RemoveFromListResponse removeFromList(String userId, String listKey, ByteString value) {
    BackendProto.RemoveFromListRequest request = BackendProto.RemoveFromListRequest.newBuilder()
        .setUserId(userId)
        .setListKey(listKey)
        .setValue(value)
        .build();
    return client.removeFromList(request);
  }

}
