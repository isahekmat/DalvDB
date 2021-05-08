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
import org.dalvdb.cluster.Locator;
import org.dalvdb.cluster.Router;
import org.dalvdb.db.BackendRequestHandler;
import org.dalvdb.exception.InvalidNodeException;
import org.dalvdb.proto.BackendProto;
import org.dalvdb.proto.BackendServerGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class BackendServerImpl extends BackendServerGrpc.BackendServerImplBase {
  private static final Logger logger = LoggerFactory.getLogger(BackendServerImpl.class);
  private static BackendServerImpl instance;

  public synchronized static BackendServerImpl getInstance() {
    if (instance == null) {
      instance = new BackendServerImpl();
    }
    return instance;
  }

  private BackendServerImpl() {
  }

  @Override
  public void get(BackendProto.GetRequest request, StreamObserver<BackendProto.GetResponse> responseObserver) {
    if (isLocal(request.getUserId(), responseObserver))
      BackendRequestHandler.getInstance().get(request, responseObserver);
  }

  @Override
  public void put(BackendProto.PutRequest request, StreamObserver<Common.Empty> responseObserver) {
    if (!isLocal(request.getUserId(), responseObserver))
      return;
    Router router = Router.getInstance();
    CompletableFuture<Void> replicated = router.propagate(request.getUserId(), Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey(request.getKey())
        .setVal(request.getValue()).build());
    replicated.thenRun(() -> {
      BackendRequestHandler.getInstance().put(request, responseObserver);
      router.commit(request.getUserId());
    });
  }

  @Override
  public void del(BackendProto.DelRequest request, StreamObserver<Common.Empty> responseObserver) {
    BackendRequestHandler.getInstance().del(request, responseObserver);
  }

  @Override
  public void addToList(BackendProto.AddToListRequest request, StreamObserver<Common.Empty> responseObserver) {
    BackendRequestHandler.getInstance().addToList(request, responseObserver);
  }

  @Override
  public void removeFromList(BackendProto.RemoveFromListRequest request, StreamObserver<Common.Empty> responseObserver) {
    BackendRequestHandler.getInstance().removeFromList(request, responseObserver);
  }

  @Override
  public void watch(BackendProto.WatchRequest request, StreamObserver<BackendProto.WatchResponse> responseObserver) {
    BackendRequestHandler.getInstance().watch(request, responseObserver);
  }

  @Override
  public void watchCancel(BackendProto.WatchCancelRequest request, StreamObserver<Common.Empty> responseObserver) {
    BackendRequestHandler.getInstance().watchCancel(request, responseObserver);
  }

  @Override
  public void watchCancelAll(Common.Empty request, StreamObserver<Common.Empty> responseObserver) {
    BackendRequestHandler.getInstance().watchCancelAll(responseObserver);
  }


  private boolean isLocal(String userId, StreamObserver<?> responseObserver) {
    Locator locator = Locator.getInstance();
    String location = locator.locate(userId);

    if (!locator.me.equals(location)) {
      responseObserver.onError(new InvalidNodeException(location));
      return false;
    }
    return true;
  }

}
