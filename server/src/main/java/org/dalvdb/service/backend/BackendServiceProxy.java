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
import org.dalvdb.db.BackendRequestHandler;
import org.dalvdb.proto.BackendProto;
import org.dalvdb.proto.BackendServerGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackendServiceProxy extends BackendServerGrpc.BackendServerImplBase {
  private static final Logger logger = LoggerFactory.getLogger(BackendServiceProxy.class);
  private static BackendServiceProxy instance;

  public static BackendServiceProxy getInstance() {
    if (instance == null) {
      instance = new BackendServiceProxy();
    }
    return instance;
  }

  private BackendServiceProxy() {
  }

  @Override
  public void get(BackendProto.GetRequest request, StreamObserver<BackendProto.GetResponse> responseObserver) {
    BackendRequestHandler.getInstance().get(request, responseObserver);
  }

  @Override
  public void put(BackendProto.PutRequest request, StreamObserver<BackendProto.PutResponse> responseObserver) {
    BackendRequestHandler.getInstance().put(request, responseObserver);
  }

  @Override
  public void del(BackendProto.DelRequest request, StreamObserver<BackendProto.DelResponse> responseObserver) {
    BackendRequestHandler.getInstance().del(request, responseObserver);
  }

  @Override
  public void addToList(BackendProto.AddToListRequest request, StreamObserver<BackendProto.AddToListResponse> responseObserver) {
    BackendRequestHandler.getInstance().addToList(request, responseObserver);
  }

  @Override
  public void removeFromList(BackendProto.RemoveFromListRequest request, StreamObserver<BackendProto.RemoveFromListResponse> responseObserver) {
    BackendRequestHandler.getInstance().removeFromList(request, responseObserver);
  }

  @Override
  public void watch(BackendProto.WatchRequest request, StreamObserver<BackendProto.WatchResponse> responseObserver) {
    BackendRequestHandler.getInstance().watch(request, responseObserver);
  }

  @Override
  public void watchCancel(BackendProto.WatchCancelRequest request,
                          StreamObserver<BackendProto.WatchCancelResponse> responseObserver) {
    BackendRequestHandler.getInstance().watchCancel(request, responseObserver);
  }

  @Override
  public void watchCancelAll(Common.Empty request,
                             StreamObserver<BackendProto.WatchCancelResponse> responseObserver) {
    BackendRequestHandler.getInstance().watchCancelAll(responseObserver);
  }
}
