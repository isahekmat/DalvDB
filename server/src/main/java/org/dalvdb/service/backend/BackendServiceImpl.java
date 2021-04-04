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

import com.google.protobuf.ByteString;
import dalv.common.Common;
import io.grpc.stub.StreamObserver;
import org.dalvdb.DalvConfig;
import org.dalvdb.exception.InternalServerException;
import org.dalvdb.lock.UserLockManager;
import org.dalvdb.proto.BackendProto;
import org.dalvdb.proto.BackendServerGrpc;
import org.dalvdb.storage.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackendServiceImpl extends BackendServerGrpc.BackendServerImplBase {
  private static final Logger logger = LoggerFactory.getLogger(BackendServiceImpl.class);
  private final StorageService storageService;
  private final UserLockManager userLockManager;
  private final WatchManager watchManager;

  public BackendServiceImpl(StorageService storageService, WatchManager watchManager) {
    this.storageService = storageService;
    this.watchManager = watchManager;
    this.userLockManager = UserLockManager.getInstance();
  }

  @Override
  public void get(BackendProto.GetRequest request, StreamObserver<BackendProto.GetResponse> responseObserver) {
    try {
      ByteString value = storageService.getValue(request.getUserId(), request.getKey());
      BackendProto.GetResponse response = BackendProto.GetResponse.newBuilder()
          .setRepType(value == null ? Common.RepType.NOK : Common.RepType.OK)
          .setValue(value)
          .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (InternalServerException e) {
      logger.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void put(BackendProto.PutRequest request, StreamObserver<BackendProto.PutResponse> responseObserver) {
    try {
      BackendProto.PutResponse response;
      if (userLockManager.tryWriteLock(request.getUserId(), DalvConfig.getInt(DalvConfig.LOCK_TIMEOUT))) {
        storageService.addOperation(request.getUserId(), Common.Operation.newBuilder()
            .setKey(request.getKey())
            .setType(Common.OpType.PUT)
            .setVal(request.getValue())
            .build());
        response = BackendProto.PutResponse.newBuilder().setRepType(Common.RepType.OK).build();
      } else {
        response = BackendProto.PutResponse.newBuilder().setRepType(Common.RepType.NOK).build();
      }
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (InternalServerException | InterruptedException e) {
      logger.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void del(BackendProto.DelRequest request, StreamObserver<BackendProto.DelResponse> responseObserver) {
    try {
      BackendProto.DelResponse response;
      if (userLockManager.tryWriteLock(request.getUserId(), DalvConfig.getInt(DalvConfig.LOCK_TIMEOUT))) {
        storageService.addOperation(request.getUserId(), Common.Operation.newBuilder()
            .setKey(request.getKey())
            .setType(Common.OpType.DEL)
            .build());
        response = BackendProto.DelResponse.newBuilder().setRepType(Common.RepType.OK).build();
      } else {
        response = BackendProto.DelResponse.newBuilder().setRepType(Common.RepType.NOK).build();
      }
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (InternalServerException | InterruptedException e) {
      logger.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void addToList(BackendProto.AddToListRequest request, StreamObserver<BackendProto.AddToListResponse> responseObserver) {
    try {
      BackendProto.AddToListResponse response;
      if (userLockManager.tryWriteLock(request.getUserId(), DalvConfig.getInt(DalvConfig.LOCK_TIMEOUT))) {
        storageService.addOperation(request.getUserId(), Common.Operation.newBuilder()
            .setKey(request.getListKey())
            .setType(Common.OpType.ADD_TO_LIST)
            .setVal(request.getValue())
            .build());
        response = BackendProto.AddToListResponse.newBuilder().setRepType(Common.RepType.OK).build();
      } else {
        response = BackendProto.AddToListResponse.newBuilder().setRepType(Common.RepType.NOK).build();
      }
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (InternalServerException | InterruptedException e) {
      logger.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void removeFromList(BackendProto.RemoveFromListRequest request, StreamObserver<BackendProto.RemoveFromListResponse> responseObserver) {
    try {
      BackendProto.RemoveFromListResponse response;
      if (userLockManager.tryWriteLock(request.getUserId(), DalvConfig.getInt(DalvConfig.LOCK_TIMEOUT))) {
        storageService.addOperation(request.getUserId(), Common.Operation.newBuilder()
            .setKey(request.getListKey())
            .setType(Common.OpType.REMOVE_FROM_LIST)
            .setVal(request.getValue())
            .build());
        response = BackendProto.RemoveFromListResponse.newBuilder().setRepType(Common.RepType.OK).build();
      } else {
        response = BackendProto.RemoveFromListResponse.newBuilder().setRepType(Common.RepType.NOK).build();
      }
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (InternalServerException | InterruptedException e) {
      logger.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void watch(BackendProto.WatchRequest request, StreamObserver<BackendProto.WatchResponse> responseObserver) {
    watchManager.addWatch(request.getKeysList(), responseObserver);
  }
}
