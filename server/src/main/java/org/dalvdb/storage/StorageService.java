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

package org.dalvdb.storage;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dalvdb.proto.ClientProto;
import org.rocksdb.RocksDBException;

import java.io.Closeable;
import java.util.List;

public interface StorageService extends Closeable {
  boolean handlePuts(String userId, List<ClientProto.Operation> opsList,int lastSnapshotId) throws RocksDBException;

  List<ClientProto.Operation> get(String userId, int lastSnapshotId) throws RocksDBException, InvalidProtocolBufferException;

  int snapshot(String userId) throws RocksDBException;

}
