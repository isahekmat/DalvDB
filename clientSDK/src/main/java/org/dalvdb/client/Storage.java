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
import dalv.common.Common;
import org.dalvdb.client.exception.StorageException;
import org.dalvdb.common.util.ByteUtil;
import org.rocksdb.*;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class Storage implements Closeable {
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

  public void apply(List<Common.Operation> opsList, int snapshotId) {
    WriteBatch wb = new WriteBatch();
    try {
      for (Common.Operation op : opsList)
        handleOperation(op, wb);
      wb.put(LAST_SNAPSHOT_ID_KEY, ByteBuffer.allocate(4).putInt(snapshotId).array());
      wb.delete(UNSYNCED);
      rocksDB.write(wo, wb);
    } catch (RocksDBException e) {
      throw new StorageException(e);
    }
  }

  public void resolveConflict(int resolvedSnapShot, List<Common.Operation> resolveOps) {
    WriteBatch wb = new WriteBatch();
    try {
      wb.delete(UNSYNCED);
      for (Common.Operation op : resolveOps) {
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

  public void apply(Common.Operation op) {
    WriteBatch wb = new WriteBatch();
    try {
      handleOperation(op, wb);
      wb.merge(UNSYNCED, ByteUtil.opToByte(op));
      rocksDB.write(wo, wb);
    } catch (RocksDBException e) {
      throw new StorageException(e);
    }
  }

  public List<Common.Operation> getUnsyncOps() {
    try {
      return ByteUtil.byteToOps(rocksDB.get(UNSYNCED));
    } catch (InvalidProtocolBufferException | RocksDBException e) {
      throw new StorageException(e);
    }
  }

  private void handleOperation(Common.Operation op, WriteBatch wb) {
    try {
      switch (op.getType()) {
        case SNAPSHOT:
          break;
        case PUT:
          wb.put(op.getKey().getBytes(),
              ByteBuffer.allocate(4 + op.getVal().size()).putInt(op.getVal().size()).put(op.getVal().toByteArray()).array());
          break;
        case DEL:
          wb.delete(op.getKey().getBytes());
          break;
        case ADD_TO_LIST:
          wb.merge(op.getKey().getBytes(),
              ByteBuffer.allocate(4 + op.getVal().size()).putInt(op.getVal().size()).put(op.getVal().toByteArray()).array());
          break;
        case REMOVE_FROM_LIST:
          byte[] bytes = rocksDB.get(op.getKey().getBytes());
          List<byte[]> values = ByteUtil.decodeListWithSeparator(bytes);
          if (values == null) return;
          byte[] valueToRemove = op.getVal().toByteArray();
          int newSize = bytes.length - (5 + op.getVal().size()); //5 == 4(len size) + 1(separator)

          ByteBuffer newVal = ByteBuffer.allocate(newSize);
          boolean exist = false;
          boolean first = true;
          for (byte[] b : values) {
            if (!Arrays.equals(b, valueToRemove) && newVal.position() <= newSize) {
              if (first)
                first = false;
              else
                newVal.put((byte) (0)); //merge separator
              newVal.putInt(b.length);
              newVal.put(b);
            } else exist = true;
          }
          if (exist) wb.put(op.getKey().getBytes(), newVal.array());
          break;
        default:
          throw new IllegalArgumentException("Invalid Operation");
      }
    } catch (RocksDBException e) {
      throw new StorageException(e);
    }
  }

  public void close() {
    wo.close();
    rocksDB.close();
  }
}
