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

package org.dalvdb.service.propagate;

import dalv.common.Common;
import io.grpc.stub.StreamObserver;
import org.dalvdb.proto.PropagateProto;
import org.dalvdb.proto.PropagateServiceGrpc;

public class PropagateServerImpl extends PropagateServiceGrpc.PropagateServiceImplBase {
  private static PropagateServerImpl instance;

  public synchronized static PropagateServerImpl getInstance() {
    if (instance == null) {
      instance = new PropagateServerImpl();
    }
    return instance;
  }

  private PropagateServerImpl() {
  }

  @Override
  public void propagate(PropagateProto.PropagateRequest request, StreamObserver<PropagateProto.PropagateAck> responseObserver) {
    super.propagate(request, responseObserver);
    //TODO
  }

  @Override
  public StreamObserver<PropagateProto.CommitRequest> commit(StreamObserver<Common.Empty> responseObserver) {
    //TODO
    return super.commit(responseObserver);
  }
}
