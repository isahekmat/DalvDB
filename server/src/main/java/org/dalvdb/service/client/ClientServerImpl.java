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
import org.dalvdb.db.ClientRequestHandler;
import org.dalvdb.proto.ClientProto;
import org.dalvdb.proto.ClientServerGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class ClientServerImpl extends ClientServerGrpc.ClientServerImplBase {
  private static final Logger logger = LoggerFactory.getLogger(ClientServerImpl.class);
  private static ClientServerImpl instance;
  private final JwtParser parser = Jwts.parserBuilder()
      .setSigningKey(DalvConfig.getStr(DalvConfig.JWT_SIGN)).build();

  public static synchronized ClientServerImpl getInstance() {
    if (instance == null) {
      instance = new ClientServerImpl();
    }
    return instance;
  }

  private ClientServerImpl() {
  }

  @Override
  public void watch(ClientProto.WatchRequest request, StreamObserver<ClientProto.WatchResponse> responseObserver) {
    String userId = validate(request.getJwt());
    ClientRequestHandler.getInstance().watch(userId, request, responseObserver);
  }

  @Override
  public void watchCancel(ClientProto.WatchCancelRequest request,
                          StreamObserver<ClientProto.WatchCancelResponse> responseObserver) {
    String userId = validate(request.getJwt());
    ClientRequestHandler.getInstance().watchCancel(userId, request, responseObserver);
  }

  @Override
  public void watchCancelAll(ClientProto.WatchCancelAllRequest request,
                             StreamObserver<ClientProto.WatchCancelResponse> responseObserver) {
    String userId = validate(request.getJwt());
    ClientRequestHandler.getInstance().watchCancelAll(userId, responseObserver);
  }

  @Override
  public void sync(ClientProto.SyncRequest request, StreamObserver<ClientProto.SyncResponse> responseObserver) {
    String jwt = request.getJwt();
    String userId = validate(jwt);
    if (Objects.isNull(userId)) {
      responseObserver.onNext(ClientProto.SyncResponse.newBuilder()
          .setSyncResponse(Common.RepType.UNRECOGNIZED).build());
      responseObserver.onCompleted();
      return;
    }
    ClientRequestHandler.getInstance().sync(userId, request, responseObserver);
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
