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
import org.dalvdb.watch.WatchManager;
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
    logger.debug("GET command received: userId:{} key:{}", request.getUserId(), request.getKey());
    BackendProto.GetResponse response;
    try {
      if (userLockManager.tryReadLock(request.getUserId(), DalvConfig.getInt(DalvConfig.LOCK_TIMEOUT))) {
        try {
          ByteString value = storageService.getValue(request.getUserId(), request.getKey());
          response = BackendProto.GetResponse.newBuilder()
              .setRepType(value == null ? Common.RepType.NOK : Common.RepType.OK)
              .setValue(value)
              .build();
        } finally {
          userLockManager.releaseReadLock(request.getUserId());
        }
      } else
        response = BackendProto.GetResponse.newBuilder().setRepType(Common.RepType.NOK).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (InternalServerException | InterruptedException e) {
      logger.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void put(BackendProto.PutRequest request, StreamObserver<BackendProto.PutResponse> responseObserver) {
    try {
      logger.debug("PUT command received: userId:{} key:{}", request.getUserId(), request.getKey());
      BackendProto.PutResponse response;
      Common.Operation op = null;
      if (userLockManager.tryWriteLock(request.getUserId(), DalvConfig.getInt(DalvConfig.LOCK_TIMEOUT))) {
        try {
          op = Common.Operation.newBuilder()
              .setKey(request.getKey())
              .setType(Common.OpType.PUT)
              .setVal(request.getValue())
              .build();
          storageService.addOperation(request.getUserId(), op);
          response = BackendProto.PutResponse.newBuilder().setRepType(Common.RepType.OK).build();
        } finally {
          userLockManager.releaseWriteLock(request.getUserId());
        }
      } else {
        response = BackendProto.PutResponse.newBuilder().setRepType(Common.RepType.NOK).build();
      }
      responseObserver.onNext(response);
      responseObserver.onCompleted();
      logger.debug("PUT command processed: userId:{} key:{}", request.getUserId(), request.getKey());
      if (response.getRepType() == Common.RepType.OK)
        watchManager.notifyChange(request.getUserId(), op);
    } catch (InternalServerException | InterruptedException e) {
      logger.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void del(BackendProto.DelRequest request, StreamObserver<BackendProto.DelResponse> responseObserver) {
    try {
      logger.debug("DEL command received: userId:{} key:{}", request.getUserId(), request.getKey());
      BackendProto.DelResponse response;
      Common.Operation op = null;
      if (userLockManager.tryWriteLock(request.getUserId(), DalvConfig.getInt(DalvConfig.LOCK_TIMEOUT))) {
        try {
          op = Common.Operation.newBuilder()
              .setKey(request.getKey())
              .setType(Common.OpType.DEL)
              .build();
          storageService.addOperation(request.getUserId(), op);
          response = BackendProto.DelResponse.newBuilder().setRepType(Common.RepType.OK).build();
        } finally {
          userLockManager.releaseWriteLock(request.getUserId());
        }
      } else {
        response = BackendProto.DelResponse.newBuilder().setRepType(Common.RepType.NOK).build();
      }
      responseObserver.onNext(response);
      responseObserver.onCompleted();
      if (response.getRepType() == Common.RepType.OK)
        watchManager.notifyChange(request.getUserId(), op);
    } catch (InternalServerException | InterruptedException e) {
      logger.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void addToList(BackendProto.AddToListRequest request, StreamObserver<BackendProto.AddToListResponse> responseObserver) {
    logger.debug("ADD_TO_LIST command received: userId:{} listKey:{}", request.getUserId(), request.getListKey());
    BackendProto.AddToListResponse response;
    Common.Operation op = null;
    try {
      if (userLockManager.tryWriteLock(request.getUserId(), DalvConfig.getInt(DalvConfig.LOCK_TIMEOUT))) {
        try {
          op = Common.Operation.newBuilder()
              .setKey(request.getListKey())
              .setType(Common.OpType.ADD_TO_LIST)
              .setVal(request.getValue())
              .build();
          storageService.addOperation(request.getUserId(), op);
          response = BackendProto.AddToListResponse.newBuilder().setRepType(Common.RepType.OK).build();
        } finally {
          userLockManager.releaseWriteLock(request.getUserId());
        }
      } else {
        response = BackendProto.AddToListResponse.newBuilder().setRepType(Common.RepType.NOK).build();
      }
    responseObserver.onNext(response);
    responseObserver.onCompleted();
    if (response.getRepType() == Common.RepType.OK)
      watchManager.notifyChange(request.getUserId(), op);
    } catch (InternalServerException | InterruptedException e) {
      logger.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void removeFromList(BackendProto.RemoveFromListRequest request, StreamObserver<BackendProto.RemoveFromListResponse> responseObserver) {
    try {
      logger.debug("REMOVE_FROM_LIST command received: userId:{} listKey:{}", request.getUserId(), request.getListKey());
      BackendProto.RemoveFromListResponse response;
      Common.Operation op = null;
      if (userLockManager.tryWriteLock(request.getUserId(), DalvConfig.getInt(DalvConfig.LOCK_TIMEOUT))) {
        try {

          op = Common.Operation.newBuilder()
              .setKey(request.getListKey())
              .setType(Common.OpType.REMOVE_FROM_LIST)
              .setVal(request.getValue())
              .build();
          storageService.addOperation(request.getUserId(), op);
          response = BackendProto.RemoveFromListResponse.newBuilder().setRepType(Common.RepType.OK).build();
        } finally {
          userLockManager.releaseWriteLock(request.getUserId());
        }
      } else {
        response = BackendProto.RemoveFromListResponse.newBuilder().setRepType(Common.RepType.NOK).build();
      }
      responseObserver.onNext(response);
      responseObserver.onCompleted();
      if (response.getRepType() == Common.RepType.OK)
        watchManager.notifyChange(request.getUserId(), op);
    } catch (InternalServerException | InterruptedException e) {
      logger.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void watch(BackendProto.WatchRequest request, StreamObserver<BackendProto.WatchResponse> responseObserver) {
    logger.debug("BACKEND WATCH command received on key:{}", request.getKey());
    watchManager.addBackendWatch(request.getKey(), responseObserver);
    logger.debug("BACKEND WATCH command processed on key:{}", request.getKey());
  }

  @Override
  public void watchCancel(BackendProto.WatchCancelRequest request,
                          StreamObserver<BackendProto.WatchCancelResponse> responseObserver) {
    logger.debug("BACKEND WATCH CANCEL command received on key:{}", request.getKey());
    watchManager.cancelBackendWatch(request.getKey());
    responseObserver.onNext(BackendProto.WatchCancelResponse.newBuilder().setResponse(Common.RepType.OK).build());
    responseObserver.onCompleted();
    logger.debug("BACKEND WATCH CANCEL command processed on key:{}", request.getKey());
  }

  @Override
  public void watchCancelAll(Common.Empty request,
                             StreamObserver<BackendProto.WatchCancelResponse> responseObserver) {
    logger.debug("BACKEND WATCH CANCEL ALL command received");
    watchManager.cancelAllBackendWatch();
    responseObserver.onNext(BackendProto.WatchCancelResponse.newBuilder().setResponse(Common.RepType.OK).build());
    responseObserver.onCompleted();
    logger.debug("BACKEND WATCH CANCEL ALL command processed");
  }
}
