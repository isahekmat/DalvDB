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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.dalvdb.DalvConfig;
import org.dalvdb.proto.ClientProto;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class RocksStorageService implements StorageService {
  private static final Logger logger = LoggerFactory.getLogger(RocksStorageService.class);
  private final RocksDB rocksDB;
  private final WriteOptions wo;

  public RocksStorageService() {
    String dataDir = DalvConfig.getStr(DalvConfig.DATA_DIR);
    RocksDB db = null;
    WriteOptions writeOptions = null;
    try {
      Options options = new Options();
      options.setCreateIfMissing(true);
      options.setMergeOperator(new StringAppendOperator((char) (0)));
      db = RocksDB.open(options, dataDir);
      writeOptions = new WriteOptions();
      writeOptions.setSync(true);
    } catch (RocksDBException e) {
      logger.error("could not open storage dir", e);
      System.exit(1);
    }
    this.rocksDB = db;
    this.wo = writeOptions;
  }

  @Override
  public boolean handlePuts(String userId, List<ClientProto.Operation> opsList, int lastSnapshotId) throws RocksDBException {
    byte[] key = userId.getBytes(Charset.defaultCharset());
    WriteBatch wb = new WriteBatch();
    for (ClientProto.Operation operation : opsList) {
      byte[] oprBytes = operation.toByteArray();
      byte[] len = ByteBuffer.allocate(4).putInt(oprBytes.length).array();
      wb.merge(key, len);
      wb.merge(key, oprBytes);
    }

    rocksDB.write(wo, wb);
    wo.close();
    return true;
  }

  @Override
  public List<ClientProto.Operation> get(String userId, int lastSnapshotId) throws RocksDBException, InvalidProtocolBufferException {
    byte[] bytes = rocksDB.get(userId.getBytes(Charset.defaultCharset()));
    LinkedList<ClientProto.Operation> operations = byteToOps(bytes);
    Iterator<ClientProto.Operation> operationIterator = operations.descendingIterator();
    int i = operations.size();
    while (operationIterator.hasNext()) {
      ClientProto.Operation op = operationIterator.next();
      if (op.getType() == ClientProto.OpType.SNAPSHOT && op.getSnapshotId() == lastSnapshotId) {
        break;
      }
      i--;
    }
    return operations.subList(i, operations.size());
  }

  @Override
  public int snapshot(String userId) throws RocksDBException {
    int snapshotId = lastSnapshotId(userId) + 1;
    ClientProto.Operation op = ClientProto.Operation.newBuilder().setType(ClientProto.OpType.SNAPSHOT)
        .setSnapshotId(snapshotId).build();
    byte[] opBytes = op.toByteArray();
    byte[] len = ByteBuffer.allocate(4).putInt(opBytes.length).array();
    WriteBatch wb = new WriteBatch(8 + opBytes.length);
    wb.merge(userId.getBytes(Charset.defaultCharset()), len);
    wb.merge(userId.getBytes(Charset.defaultCharset()), opBytes);
    wb.put((userId + ".lastSnapshotId").getBytes(Charset.defaultCharset()),
        ByteBuffer.allocate(4).putInt(snapshotId).array());
    rocksDB.write(wo, wb);
    return snapshotId;
  }

  private int lastSnapshotId(String userId) throws RocksDBException {
    byte[] lastSnapShotId = rocksDB.get((userId + ".lastSnapshotId").getBytes(Charset.defaultCharset()));
    if (lastSnapShotId == null)
      return 0;
    return ByteBuffer.wrap(lastSnapShotId).getInt();
  }

  private LinkedList<ClientProto.Operation> byteToOps(byte[] bytes) throws InvalidProtocolBufferException {
    LinkedList<ClientProto.Operation> ops = new LinkedList<>();
    int offset = 0;
    while (offset < bytes.length) {
      int len = ByteBuffer.wrap(bytes, offset, 4).getInt();
      offset += 5;
      ClientProto.Operation op = ClientProto.Operation.parseFrom(ByteString.copyFrom(bytes, offset, len));
      offset += len;
      ops.add(op);
      offset++;
    }
    return ops;
  }

  @Override
  public void close() throws IOException {
    rocksDB.close();
    wo.close();
  }

  public void clear() throws RocksDBException, IOException {
    close();
    RocksDB.destroyDB(DalvConfig.getStr(DalvConfig.DATA_DIR), new Options());
  }
}
