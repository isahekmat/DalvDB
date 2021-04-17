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

import dalv.common.Common;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.dalvdb.proto.ClientProto;
import org.dalvdb.proto.ClientServerGrpc;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.TimeUnit;

class DalvConnector implements Closeable {
  private final String jwt;
  private final ClientServerGrpc.ClientServerBlockingStub client;
  private final ClientServerGrpc.ClientServerStub clientNonBlocking;
  private final ManagedChannel channel;

  public DalvConnector(String address, String jwt) {
    this.jwt = jwt;
    String[] addressArr = address.split(":");
    final String host = addressArr[0];
    final int port = Integer.parseInt(addressArr[1]);
    channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    client = ClientServerGrpc.newBlockingStub(channel);
    clientNonBlocking = ClientServerGrpc.newStub(channel);
  }

  public ClientProto.SyncResponse sync(List<Common.Operation> ops, int lastSnapshotId) {
    ClientProto.SyncRequest request = ClientProto.SyncRequest.newBuilder()
        .setLastSnapshotId(lastSnapshotId)
        .addAllOps(ops)
        .setJwt(jwt)
        .build();
    return client.sync(request);
  }

  public void watch(String key, StreamObserver<ClientProto.WatchResponse> observer) {
    ClientProto.WatchRequest request = ClientProto.WatchRequest.newBuilder()
        .setJwt(jwt)
        .setKey(key)
        .build();
    clientNonBlocking.watch(request, observer);
  }

  public ClientProto.WatchCancelResponse cancelWatch(String key) {
    ClientProto.WatchCancelRequest request = ClientProto.WatchCancelRequest.newBuilder()
        .setJwt(jwt)
        .setKey(key)
        .build();
    return client.watchCancel(request);
  }

  public ClientProto.WatchCancelResponse cancelAllWatch() {
    ClientProto.WatchCancelAllRequest request = ClientProto.WatchCancelAllRequest.newBuilder()
        .setJwt(jwt)
        .build();
    return client.watchCancelAll(request);
  }

  @Override
  public void close() {
    channel.shutdown();
    try {
      channel.awaitTermination(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
      channel.shutdownNow();
    }
  }
}
