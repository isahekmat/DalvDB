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
import org.dalvdb.proto.BackendProto;

import java.util.List;

public interface WatchManager {
  void addWatch(List<String> keysList, StreamObserver<BackendProto.WatchResponse> responseObserver);

  void notifyChange(String userId, List<Common.Operation> operations);
}
