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

package org.dalvdb.db;

import com.google.protobuf.ByteString;
import dalv.common.Common;
import io.grpc.stub.StreamObserver;
import org.dalvdb.DalvConfig;
import org.dalvdb.db.storage.StorageService;
import org.dalvdb.exception.InternalServerException;
import org.dalvdb.lock.UserLockManager;
import org.dalvdb.proto.BackendProto;
import org.dalvdb.watch.WatchManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackendRequestHandler {
  private static final Logger logger = LoggerFactory.getLogger(BackendRequestHandler.class);
  private static BackendRequestHandler instance;

  public synchronized static BackendRequestHandler getInstance() {
    if (instance == null) {
      instance = new BackendRequestHandler();
    }
    return instance;
  }

  private BackendRequestHandler() {
  }

  public void get(BackendProto.GetRequest request, StreamObserver<BackendProto.GetResponse> responseObserver) {
    logger.debug("GET command received: userId:{} key:{}", request.getUserId(), request.getKey());
    try {
      if (UserLockManager.getInstance().tryReadLock(request.getUserId(), DalvConfig.getInt(DalvConfig.LOCK_TIMEOUT))) {
        try {
          ByteString value = StorageService.getInstance().getValue(request.getUserId(), request.getKey());
          BackendProto.GetResponse response = BackendProto.GetResponse.newBuilder()
              .setValue(value)
              .build();
          responseObserver.onNext(response);
          responseObserver.onCompleted();
        } finally {
          UserLockManager.getInstance().releaseReadLock(request.getUserId());
        }
      } else
        throw new InternalServerException("could not acquire lock for this user");
    } catch (InternalServerException | InterruptedException e) {
      logger.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  public void put(BackendProto.PutRequest request, StreamObserver<Common.Empty> responseObserver) {
    try {
      logger.debug("PUT command received: userId:{} key:{}", request.getUserId(), request.getKey());
      Common.Operation op;
      if (UserLockManager.getInstance().tryWriteLock(request.getUserId(), DalvConfig.getInt(DalvConfig.LOCK_TIMEOUT))) {
        try {
          op = Common.Operation.newBuilder()
              .setKey(request.getKey())
              .setType(Common.OpType.PUT)
              .setVal(request.getValue())
              .build();
          StorageService.getInstance().addOperation(request.getUserId(), op);
          responseObserver.onNext(Common.Empty.newBuilder().build());
          responseObserver.onCompleted();
        } finally {
          UserLockManager.getInstance().releaseWriteLock(request.getUserId());
        }
      } else {
        throw new InternalServerException("could not acquire lock for this user");
      }

      logger.debug("PUT command processed: userId:{} key:{}", request.getUserId(), request.getKey());
      WatchManager.getInstance().notifyChange(request.getUserId(), op);
    } catch (InternalServerException | InterruptedException e) {
      logger.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  public void del(BackendProto.DelRequest request, StreamObserver<Common.Empty> responseObserver) {
    try {
      logger.debug("DEL command received: userId:{} key:{}", request.getUserId(), request.getKey());
      Common.Operation op;
      if (UserLockManager.getInstance().tryWriteLock(request.getUserId(), DalvConfig.getInt(DalvConfig.LOCK_TIMEOUT))) {
        try {
          op = Common.Operation.newBuilder()
              .setKey(request.getKey())
              .setType(Common.OpType.DEL)
              .build();
          StorageService.getInstance().addOperation(request.getUserId(), op);
          responseObserver.onNext(Common.Empty.newBuilder().build());
          responseObserver.onCompleted();
        } finally {
          UserLockManager.getInstance().releaseWriteLock(request.getUserId());
        }
      } else {
        throw new InternalServerException("could not acquire lock for this user");
      }

      WatchManager.getInstance().notifyChange(request.getUserId(), op);
    } catch (InternalServerException | InterruptedException e) {
      logger.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  public void addToList(BackendProto.AddToListRequest request, StreamObserver<Common.Empty> responseObserver) {
    logger.debug("ADD_TO_LIST command received: userId:{} listKey:{}", request.getUserId(), request.getListKey());
    Common.Operation op;
    try {
      if (UserLockManager.getInstance().tryWriteLock(request.getUserId(), DalvConfig.getInt(DalvConfig.LOCK_TIMEOUT))) {
        try {
          op = Common.Operation.newBuilder()
              .setKey(request.getListKey())
              .setType(Common.OpType.ADD_TO_LIST)
              .setVal(request.getValue())
              .build();
          StorageService.getInstance().addOperation(request.getUserId(), op);
          responseObserver.onNext(Common.Empty.newBuilder().build());
          responseObserver.onCompleted();
        } finally {
          UserLockManager.getInstance().releaseWriteLock(request.getUserId());
        }
      } else {
        throw new InternalServerException("could not acquire lock for this user");
      }

      WatchManager.getInstance().notifyChange(request.getUserId(), op);
    } catch (InternalServerException | InterruptedException e) {
      logger.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  public void removeFromList(BackendProto.RemoveFromListRequest request, StreamObserver<Common.Empty> responseObserver) {
    try {
      logger.debug("REMOVE_FROM_LIST command received: userId:{} listKey:{}", request.getUserId(), request.getListKey());
      Common.Operation op;
      if (UserLockManager.getInstance().tryWriteLock(request.getUserId(), DalvConfig.getInt(DalvConfig.LOCK_TIMEOUT))) {
        try {

          op = Common.Operation.newBuilder()
              .setKey(request.getListKey())
              .setType(Common.OpType.REMOVE_FROM_LIST)
              .setVal(request.getValue())
              .build();
          StorageService.getInstance().addOperation(request.getUserId(), op);
          responseObserver.onNext(Common.Empty.newBuilder().build());
          responseObserver.onCompleted();
        } finally {
          UserLockManager.getInstance().releaseWriteLock(request.getUserId());
        }
      } else {
        throw new InternalServerException("could not acquire lock for this user");
      }
      WatchManager.getInstance().notifyChange(request.getUserId(), op);
    } catch (InternalServerException | InterruptedException e) {
      logger.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  public void watch(BackendProto.WatchRequest request, StreamObserver<BackendProto.WatchResponse> responseObserver) {
    logger.debug("BACKEND WATCH command received on key:{}", request.getKey());
    WatchManager.getInstance().addBackendWatch(request.getKey(), responseObserver);
    logger.debug("BACKEND WATCH command processed on key:{}", request.getKey());
  }

  public void watchCancel(BackendProto.WatchCancelRequest request, StreamObserver<Common.Empty> responseObserver) {
    logger.debug("BACKEND WATCH CANCEL command received on key:{}", request.getKey());
    WatchManager.getInstance().cancelBackendWatch(request.getKey());
    responseObserver.onNext(Common.Empty.newBuilder().build());
    responseObserver.onCompleted();
    logger.debug("BACKEND WATCH CANCEL command processed on key:{}", request.getKey());
  }

  public void watchCancelAll(StreamObserver<Common.Empty> responseObserver) {
    logger.debug("BACKEND WATCH CANCEL ALL command received");
    WatchManager.getInstance().cancelAllBackendWatch();
    responseObserver.onNext(Common.Empty.newBuilder().build());
    responseObserver.onCompleted();
    logger.debug("BACKEND WATCH CANCEL ALL command processed");
  }
}
