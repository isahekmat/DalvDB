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

public class BackendServiceImpl extends BackendServerGrpc.BackendServerImplBase {
  private static final Logger logger = LoggerFactory.getLogger(BackendServiceImpl.class);
  private static BackendServiceImpl instance;

  public static BackendServiceImpl getInstance() {
    if (instance == null) {
      instance = new BackendServiceImpl();
    }
    return instance;
  }

  private BackendServiceImpl() {
  }

  @Override
  public void get(BackendProto.GetRequest request, StreamObserver<BackendProto.GetResponse> responseObserver) {
    BackendRequestHandler.getInstance().get(request, responseObserver);
  }

  @Override
  public void put(BackendProto.PutRequest request, StreamObserver<Common.Empty> responseObserver) {
    BackendRequestHandler.getInstance().put(request, responseObserver);
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
}
