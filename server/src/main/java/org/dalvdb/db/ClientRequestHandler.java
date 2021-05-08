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

import dalv.common.Common;
import io.grpc.stub.StreamObserver;
import org.dalvdb.DalvConfig;
import org.dalvdb.db.storage.StorageService;
import org.dalvdb.exception.InternalServerException;
import org.dalvdb.lock.UserLockManager;
import org.dalvdb.proto.ClientProto;
import org.dalvdb.watch.WatchManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ClientRequestHandler {
  private static final Logger logger = LoggerFactory.getLogger(ClientRequestHandler.class);
  private static ClientRequestHandler instance;

  public synchronized static ClientRequestHandler getInstance() {
    if (instance == null) {
      instance = new ClientRequestHandler();
    }
    return instance;
  }

  private ClientRequestHandler() {
  }

  public void watch(String userId, ClientProto.WatchRequest request, StreamObserver<ClientProto.WatchResponse> responseObserver) {
    logger.debug("CLIENT WATCH command received on key:{}", request.getKey());
    WatchManager.getInstance().addClientWatch(userId, request.getKey(), responseObserver);
    logger.debug("CLIENT WATCH command processed on key:{}", request.getKey());
  }

  public void watchCancel(String userId, ClientProto.WatchCancelRequest request,
                          StreamObserver<Common.Empty> responseObserver) {
    logger.debug("CLIENT WATCH CANCEL command received on key:{}", request.getKey());
    WatchManager.getInstance().cancelClientWatch(userId, request.getKey());
    responseObserver.onNext(Common.Empty.newBuilder().build());
    responseObserver.onCompleted();
    logger.debug("CLIENT WATCH CANCEL command processed on key:{}", request.getKey());
  }

  public void watchCancelAll(String userId, StreamObserver<Common.Empty> responseObserver) {
    logger.debug("CLIENT WATCH CANCEL ALL command received");
    WatchManager.getInstance().cancelAllClientWatch(userId);
    responseObserver.onNext(Common.Empty.newBuilder().build());
    responseObserver.onCompleted();
    logger.debug("CLIENT WATCH CANCEL ALL command processed");
  }

  public void sync(String userId, ClientProto.SyncRequest request, StreamObserver<ClientProto.SyncResponse> responseObserver) {
    logger.debug("SYNC command received: userId:{}, lastSnapshotId:{}, operations:{}",
        userId, request.getLastSnapshotId(), request.getOpsList());
    ClientProto.SyncResponse res = null;
    try {
      res = handleSync(userId, request.getOpsList(), request.getLastSnapshotId());
    } catch (InternalServerException e) {
      logger.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
    responseObserver.onNext(res);
    responseObserver.onCompleted();
    WatchManager.getInstance().notifyChange(userId, request.getOpsList());
  }

  private ClientProto.SyncResponse handleSync(String userId, List<Common.Operation> ops, int lastSnapshotId)
      throws InternalServerException {
    try {
      if (ops != null && ops.size() > 0) {
        return handleSyncWithUpdate(userId, ops, lastSnapshotId);
      } else {
        return handleSyncWithoutUpdate(userId, lastSnapshotId);
      }
    } catch (InterruptedException e) {
      throw new InternalServerException(e);
    }
  }

  private ClientProto.SyncResponse handleSyncWithoutUpdate(String userId, int lastSnapshotId) throws InterruptedException {
    ClientProto.SyncResponse.Builder resBuilder = ClientProto.SyncResponse.newBuilder();
    //TODO: maybe it's better to retry for a limited time if it cannot acquire the lock
    if (UserLockManager.getInstance().tryReadLock(userId, DalvConfig.getInt(DalvConfig.LOCK_TIMEOUT))) {
      try {
        read(userId, lastSnapshotId, resBuilder);
        resBuilder.setSyncResponse(Common.RepType.OK);
        return resBuilder.build();
      } finally {
        UserLockManager.getInstance().releaseReadLock(userId);
      }
    }
    resBuilder.setSyncResponse(Common.RepType.NOK);
    return resBuilder.build();
  }

  private ClientProto.SyncResponse handleSyncWithUpdate(String userId, List<Common.Operation> ops, int lastSnapshotId) throws InterruptedException {
    ClientProto.SyncResponse.Builder resBuilder = ClientProto.SyncResponse.newBuilder();
    //TODO: maybe it's better to retry for a limited time if it cannot acquire the lock
    if (UserLockManager.getInstance().tryWriteLock(userId, DalvConfig.getInt(DalvConfig.LOCK_TIMEOUT))) {
      try {
        boolean updatesHandledSuccessfully = StorageService.getInstance().handleOperations(userId, ops, lastSnapshotId);
        resBuilder.setSyncResponse(updatesHandledSuccessfully ? Common.RepType.OK : Common.RepType.NOK);
        read(userId, lastSnapshotId, resBuilder);
        return resBuilder.build();
      } finally {
        UserLockManager.getInstance().releaseWriteLock(userId);
      }
    }
    resBuilder.setSyncResponse(Common.RepType.NOK);
    return resBuilder.build();
  }

  private void read(String userId, int lastSnapshotId,
                    ClientProto.SyncResponse.Builder responseBuilder) {
    List<Common.Operation> ops = StorageService.getInstance().get(userId, lastSnapshotId);
    responseBuilder.addAllOps(ops);
    if (ops.isEmpty() || ops.get(ops.size() - 1).getType() == Common.OpType.SNAPSHOT) {
      responseBuilder.setSnapshotId(lastSnapshotId);
    } else {
      responseBuilder.setSnapshotId(StorageService.getInstance().snapshot(userId));
    }
  }
}
