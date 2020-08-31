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
import org.dalvdb.DalvConfig;
import org.dalvdb.exception.InternalServerException;
import org.dalvdb.lock.UserLockManager;
import org.dalvdb.proto.BackendProto;
import org.dalvdb.proto.BackendServerGrpc;
import org.dalvdb.proto.ClientProto;
import org.dalvdb.storage.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackendServiceImpl extends BackendServerGrpc.BackendServerImplBase {
  private static final Logger logger = LoggerFactory.getLogger(BackendServiceImpl.class);
  private final StorageService storageService;
  private final UserLockManager userLockManager;

  public BackendServiceImpl(StorageService storageService) {
    this.storageService = storageService;
    this.userLockManager = UserLockManager.getInstance();
  }

  @Override
  public void get(BackendProto.GetRequest request, StreamObserver<BackendProto.GetResponse> responseObserver) {
    super.get(request, responseObserver);
  }

  @Override
  public void put(BackendProto.PutRequest request, StreamObserver<BackendProto.PutResponse> responseObserver) {
    try {
      if (userLockManager.tryWriteLock(request.getUserId(), DalvConfig.getInt(DalvConfig.LOCK_TIMEOUT))) {
        storageService.addOperation(request.getUserId(), ClientProto.Operation.newBuilder()
            .setKey(request.getKey())
            .setType(ClientProto.OpType.PUT)
            .setVal(request.getValue())
            .build());
      } else {
        responseObserver.onNext(BackendProto.PutResponse.newBuilder()
            .setRepType(Common.RepType.NOK).build());
        responseObserver.onCompleted();
      }
    } catch (InternalServerException | InterruptedException e) {
      logger.error(e.getMessage(), e);
      responseObserver.onError(e);
    }

  }

  @Override
  public void del(BackendProto.DelRequest request, StreamObserver<BackendProto.DelResponse> responseObserver) {
    super.del(request, responseObserver);
  }
}
