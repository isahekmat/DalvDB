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

package org.dalvdb.service.client;

import dalv.common.Common;
import io.grpc.stub.StreamObserver;
import io.jsonwebtoken.*;
import io.jsonwebtoken.security.SecurityException;
import org.dalvdb.DalvConfig;
import org.dalvdb.exception.InternalServerException;
import org.dalvdb.lock.UserLockManager;
import org.dalvdb.proto.ClientProto;
import org.dalvdb.proto.ClientServerGrpc;
import org.dalvdb.storage.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

public class ClientServerImpl extends ClientServerGrpc.ClientServerImplBase {
  private static final Logger logger = LoggerFactory.getLogger(ClientServerImpl.class);
  private final StorageService storage;
  private final UserLockManager userLockManager;
  private final JwtParser parser = Jwts.parserBuilder()
      .setSigningKey(DalvConfig.getStr(DalvConfig.JWT_SIGN)).build();

  public ClientServerImpl(StorageService storage) {
    this.storage = storage;
    this.userLockManager = UserLockManager.getInstance();
  }

  @Override
  public void sync(ClientProto.SyncRequest request, StreamObserver<ClientProto.SyncResponse> responseObserver) {
    String jwt = request.getJwt();
    String userId = validate(jwt);
    ClientProto.SyncResponse res = null;
    if (Objects.nonNull(userId)) {
      try {
        res = handleSync(userId, request);
      } catch (InternalServerException e) {
        logger.error(e.getMessage(), e);
        responseObserver.onError(e);
      }
    } else {
      res = ClientProto.SyncResponse.newBuilder()
          .setSyncResponse(Common.RepType.UNRECOGNIZED).build();
    }
    responseObserver.onNext(res);
    responseObserver.onCompleted();
  }

  private ClientProto.SyncResponse handleSync(String userId, ClientProto.SyncRequest request)
      throws InternalServerException {
    try {
      if (request.getOpsList() != null && request.getOpsList().size() > 0) {
        return handleSyncWithUpdate(userId, request);
      } else {
        return handleSyncWithoutUpdate(userId, request);
      }
    } catch (InterruptedException e) {
      throw new InternalServerException(e);
    }
  }

  private ClientProto.SyncResponse handleSyncWithoutUpdate(String userId, ClientProto.SyncRequest request) throws InterruptedException {
    ClientProto.SyncResponse.Builder resBuilder = ClientProto.SyncResponse.newBuilder();
    //TODO: maybe it's better to retry for a limited time if it cannot acquire the lock
    if (userLockManager.tryReadLock(userId, DalvConfig.getInt(DalvConfig.LOCK_TIMEOUT))) {
      try {
        read(userId, request.getLastSnapshotId(), resBuilder);
        resBuilder.setSyncResponse(Common.RepType.OK);
        return resBuilder.build();
      } finally {
        userLockManager.releaseReadLock(userId);
      }
    }
    resBuilder.setSyncResponse(Common.RepType.NOK);
    return resBuilder.build();
  }

  private ClientProto.SyncResponse handleSyncWithUpdate(String userId, ClientProto.SyncRequest request) throws InterruptedException {
    ClientProto.SyncResponse.Builder resBuilder = ClientProto.SyncResponse.newBuilder();
    //TODO: maybe it's better to retry for a limited time if it cannot acquire the lock
    if (userLockManager.tryWriteLock(userId, DalvConfig.getInt(DalvConfig.LOCK_TIMEOUT))) {
      try {
        boolean updatesHandledSuccessfully = storage.handleOperations(userId, request.getOpsList(), request.getLastSnapshotId());
        resBuilder.setSyncResponse(updatesHandledSuccessfully ? Common.RepType.OK : Common.RepType.NOK);
        read(userId, request.getLastSnapshotId(), resBuilder);
        return resBuilder.build();
      } finally {
        userLockManager.releaseWriteLock(userId);
      }
    }
    resBuilder.setSyncResponse(Common.RepType.NOK);
    return resBuilder.build();
  }

  private void read(String userId, int lastSnapshotId,
                    ClientProto.SyncResponse.Builder responseBuilder) {
    List<Common.Operation> ops = storage.get(userId, lastSnapshotId);
    responseBuilder.addAllOps(ops);
    if (ops.isEmpty() || ops.get(ops.size() - 1).getType() == Common.OpType.SNAPSHOT) {
      responseBuilder.setSnapshotId(lastSnapshotId);
    } else {
      responseBuilder.setSnapshotId(storage.snapshot(userId));
    }
  }

  private String validate(String jwt) {
    String userId;
    try {
      Claims body = parser.parseClaimsJws(jwt).getBody();
      userId = body.get("userId", String.class);
      return userId;
    } catch (SecurityException e) {
      logger.warn("Invalid JWT signature.");
      logger.trace("Invalid JWT signature trace:", e);
    } catch (MalformedJwtException e) {
      logger.warn("Invalid JWT token.");
      logger.trace("Invalid JWT token trace:", e);
    } catch (ExpiredJwtException e) {
      logger.warn("Expired JWT token.");
      logger.trace("Expired JWT token trace:", e);
    } catch (UnsupportedJwtException e) {
      logger.warn("Unsupported JWT token.");
      logger.trace("Unsupported JWT token trace:", e);
    } catch (IllegalArgumentException e) {
      logger.warn("JWT token compact of handler are invalid.");
      logger.trace("JWT token compact of handler are invalid trace: ", e);
    }
    return null;
  }
}
