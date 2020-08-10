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

  public Storage(String dataDir) throws RocksDBException {
    Options options = new Options();
    options.setCreateIfMissing(true);
    options.setMergeOperator(new StringAppendOperator((char) (0)));
    wo = new WriteOptions();
    wo.setSync(true);
    this.rocksDB = RocksDB.open(options, dataDir);
  }

  public int getLastSnapshotId() throws RocksDBException {
    byte[] lastSnapshotId = rocksDB.get(LAST_SNAPSHOT_ID_KEY);
    if (lastSnapshotId == null) return 0;
    return ByteBuffer.wrap(lastSnapshotId).getInt();
  }

  public void apply(List<ClientProto.Operation> opsList, int snapshotId) throws RocksDBException {
    System.out.println(opsList);
    WriteBatch wb = new WriteBatch();
    for (ClientProto.Operation op : opsList)
      handleOperation(op, wb);
    wb.put(LAST_SNAPSHOT_ID_KEY, ByteBuffer.allocate(4).putInt(snapshotId).array());
    wb.delete(UNSYNCED);
    rocksDB.write(wo, wb);
  }

  public byte[] get(String key) throws RocksDBException {
    return rocksDB.get(key.getBytes());
  }

  public void put(ClientProto.Operation op) throws RocksDBException {
    WriteBatch wb = new WriteBatch();
    handleOperation(op, wb);
    wb.merge(UNSYNCED, ByteUtil.opToByte(op));
    rocksDB.write(wo, wb);
  }

  public List<ClientProto.Operation> getUnsynced() throws RocksDBException, InvalidProtocolBufferException {
    return ByteUtil.byteToOps(rocksDB.get(UNSYNCED));
  }

  private void handleOperation(ClientProto.Operation op, WriteBatch wb) throws RocksDBException {
    if (op.getType() == ClientProto.OpType.PUT)
      wb.put(op.getKey().getBytes(), op.getVal().toByteArray());
    else if (op.getType() == ClientProto.OpType.DEL)
      wb.delete(op.getKey().getBytes());
    else if (op.getType() == ClientProto.OpType.ADD_TO_LIST) {
      //TODO
    }
  }

}
