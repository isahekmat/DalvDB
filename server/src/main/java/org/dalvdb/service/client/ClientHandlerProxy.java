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
import io.jsonwebtoken.security.SignatureException;
import org.dalvdb.DalvConfig;
import org.dalvdb.db.ClientHandler;
import org.dalvdb.proto.ClientProto;
import org.dalvdb.proto.ClientServerGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class ClientHandlerProxy extends ClientServerGrpc.ClientServerImplBase {
  private static final Logger logger = LoggerFactory.getLogger(ClientHandlerProxy.class);
  private static ClientHandlerProxy instance;
  private final JwtParser parser = Jwts.parserBuilder()
      .setSigningKey(DalvConfig.getStr(DalvConfig.JWT_SIGN)).build();

  public synchronized static ClientHandlerProxy getInstance() {
    if (instance == null) {
      instance = new ClientHandlerProxy();
    }
    return instance;
  }

  private ClientHandlerProxy() {
  }

  @Override
  public void watch(ClientProto.WatchRequest request, StreamObserver<ClientProto.WatchResponse> responseObserver) {
    String userId = validate(request.getJwt(), responseObserver);
    if (Objects.nonNull(userId))
      ClientHandler.getInstance().watch(userId, request, responseObserver);
  }

  @Override
  public void watchCancel(ClientProto.WatchCancelRequest request,
                          StreamObserver<Common.Empty> responseObserver) {
    String userId = validate(request.getJwt(), responseObserver);
    if (Objects.nonNull(userId))
      ClientHandler.getInstance().watchCancel(userId, request, responseObserver);
  }

  @Override
  public void watchCancelAll(ClientProto.WatchCancelAllRequest request,
                             StreamObserver<Common.Empty> responseObserver) {
    String userId = validate(request.getJwt(), responseObserver);
    if (Objects.nonNull(userId))
      ClientHandler.getInstance().watchCancelAll(userId, responseObserver);
  }

  @Override
  public void sync(ClientProto.SyncRequest request, StreamObserver<ClientProto.SyncResponse> responseObserver) {
    String userId = validate(request.getJwt(), responseObserver);
    if (Objects.nonNull(userId))
      ClientHandler.getInstance().sync(userId, request, responseObserver);
  }

  private String validate(String jwt, StreamObserver<?> responseObserver) {
    String userId;
    try {
      Claims body = parser.parseClaimsJws(jwt).getBody();
      userId = body.get("userId", String.class);
      return userId;
    } catch (UnsupportedJwtException | MalformedJwtException | SignatureException | ExpiredJwtException |
        IllegalArgumentException | SecurityException e) {
      logger.error("Invalid provided jwt", e);
      responseObserver.onError(e);
      return null;
    }
  }
}
