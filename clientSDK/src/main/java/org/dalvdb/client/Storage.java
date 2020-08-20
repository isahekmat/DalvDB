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

package org.dalvdb.client;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dalvdb.client.exception.StorageException;
import org.dalvdb.common.util.ByteUtil;
import org.dalvdb.proto.ClientProto;
import org.rocksdb.*;

import java.nio.ByteBuffer;
import java.util.List;

public class Storage {
  private static final byte[] LAST_SNAPSHOT_ID_KEY = "dalv.lastSnapshotId".getBytes();
  private static final byte[] UNSYNCED = "dalv.unsynced".getBytes();

  private final RocksDB rocksDB;
  private final WriteOptions wo;

  public Storage(String dataDir) {
    Options options = new Options();
    options.setCreateIfMissing(true);
    options.setMergeOperator(new StringAppendOperator((char) (0)));
    wo = new WriteOptions();
    wo.setSync(true);
    try {
      this.rocksDB = RocksDB.open(options, dataDir);
    } catch (RocksDBException e) {
      throw new StorageException("Could not open database", e);
    }
  }

  public int getLastSnapshotId() {
    byte[] lastSnapshotId;
    try {
      lastSnapshotId = rocksDB.get(LAST_SNAPSHOT_ID_KEY);
    } catch (RocksDBException e) {
      throw new StorageException(e);
    }
    if (lastSnapshotId == null) return 0;
    return ByteBuffer.wrap(lastSnapshotId).getInt();
  }

  public void apply(List<ClientProto.Operation> opsList, int snapshotId) {
    WriteBatch wb = new WriteBatch();
    try {
      for (ClientProto.Operation op : opsList)
        handleOperation(op, wb);
      wb.put(LAST_SNAPSHOT_ID_KEY, ByteBuffer.allocate(4).putInt(snapshotId).array());
      wb.delete(UNSYNCED);
      rocksDB.write(wo, wb);
    } catch (RocksDBException e) {
      throw new StorageException(e);
    }
  }

  public void resolveConflict(int resolvedSnapShot, List<ClientProto.Operation> resolveOps) {
    WriteBatch wb = new WriteBatch();
    try {
      wb.delete(UNSYNCED);
      for (ClientProto.Operation op : resolveOps) {
        handleOperation(op, wb);
        wb.merge(UNSYNCED, ByteUtil.opToByte(op));
        rocksDB.write(wo, wb);
      }
      wb.put(LAST_SNAPSHOT_ID_KEY, ByteBuffer.allocate(4).putInt(resolvedSnapShot).array());
      rocksDB.write(wo, wb);
    } catch (RocksDBException e) {
      throw new StorageException(e);
    }
  }

  public byte[] get(String key) {
    try {
      return rocksDB.get(key.getBytes());
    } catch (RocksDBException e) {
      throw new StorageException(e);
    }
  }

  public void put(ClientProto.Operation op) {
    WriteBatch wb = new WriteBatch();
    try {
      handleOperation(op, wb);
      wb.merge(UNSYNCED, ByteUtil.opToByte(op));
      rocksDB.write(wo, wb);
    } catch (RocksDBException e) {
      throw new StorageException(e);
    }
  }

  public List<ClientProto.Operation> getUnsynced() {
    try {
      return ByteUtil.byteToOps(rocksDB.get(UNSYNCED));
    } catch (InvalidProtocolBufferException | RocksDBException e) {
      throw new StorageException(e);
    }
  }

  private void handleOperation(ClientProto.Operation op, WriteBatch wb) {
    try {
      if (op.getType() == ClientProto.OpType.PUT)
        wb.put(op.getKey().getBytes(), op.getVal().toByteArray());
      else if (op.getType() == ClientProto.OpType.DEL)
        wb.delete(op.getKey().getBytes());
      else if (op.getType() == ClientProto.OpType.ADD_TO_LIST) {
        //TODO
      }
    } catch (RocksDBException e) {
      throw new StorageException(e);
    }
  }

}
