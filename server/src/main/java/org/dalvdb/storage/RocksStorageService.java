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
import dalv.common.Common;
import org.dalvdb.DalvConfig;
import org.dalvdb.common.util.ByteUtil;
import org.dalvdb.exception.InternalServerException;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The default implementation of {@link StorageService} which employs RocksDB as the internal storage engine to store data on
 * permanent storage
 */
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

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean handleOperations(String userId, List<Common.Operation> opsList, int lastSnapshotId) {
    if (checkForConflict(get(userId, lastSnapshotId), opsList))
      return false;
    try {
      byte[] key = userId.getBytes(Charset.defaultCharset());
      WriteBatch wb = new WriteBatch();
      for (Common.Operation operation : opsList) {
        wb.merge(key, ByteUtil.opToByte(operation));
      }

      rocksDB.write(wo, wb);
      return true;
    } catch (RocksDBException e) {
      throw new InternalServerException(e);
    }
  }

  @Override
  public boolean addOperation(String userId, Common.Operation operation) {
    try {
      byte[] key = userId.getBytes(Charset.defaultCharset());
      rocksDB.merge(key, ByteUtil.opToByte(operation));
      return true;
    } catch (RocksDBException e) {
      throw new InternalServerException(e);
    }
  }

  private boolean checkForConflict(List<Common.Operation> oldOps,
                                   List<Common.Operation> newOps) {
    if (oldOps.isEmpty()) return false;
    Set<String> modifiedKeys = oldOps.stream().map(Common.Operation::getKey).collect(Collectors.toSet());
    return newOps.stream().filter(op -> op.getType() != Common.OpType.SNAPSHOT)
        .map(Common.Operation::getKey).anyMatch(modifiedKeys::contains);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<Common.Operation> get(String userId, int lastSnapshotId) {
    try {
      byte[] bytes = rocksDB.get(userId.getBytes(Charset.defaultCharset()));
      LinkedList<Common.Operation> operations = ByteUtil.byteToOps(bytes);
      Iterator<Common.Operation> operationIterator = operations.descendingIterator();
      int i = operations.size();
      while (operationIterator.hasNext()) {
        Common.Operation op = operationIterator.next();
        if (op.getType() == Common.OpType.SNAPSHOT && op.getSnapshotId() == lastSnapshotId) {
          break;
        }
        i--;
      }
      return operations.subList(i, operations.size());
    } catch (InvalidProtocolBufferException | RocksDBException e) {
      throw new InternalServerException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int snapshot(String userId) {
    try {
      int snapshotId = lastSnapshotId(userId) + 1;
      Common.Operation op = Common.Operation.newBuilder().setType(Common.OpType.SNAPSHOT)
          .setSnapshotId(snapshotId).build();
      WriteBatch wb = new WriteBatch(8 + op.toByteArray().length);
      wb.merge(userId.getBytes(Charset.defaultCharset()), ByteUtil.opToByte(op));
      wb.put((userId + ".lastSnapshotId").getBytes(Charset.defaultCharset()),
          ByteBuffer.allocate(4).putInt(snapshotId).array());
      rocksDB.write(wo, wb);
      return snapshotId;
    } catch (RocksDBException e) {
      throw new InternalServerException(e);
    }
  }

  private int lastSnapshotId(String userId) {
    try {
      byte[] lastSnapShotId = rocksDB.get((userId + ".lastSnapshotId").getBytes(Charset.defaultCharset()));
      if (lastSnapShotId == null)
        return 0;
      return ByteBuffer.wrap(lastSnapShotId).getInt();
    } catch (RocksDBException e) {
      throw new InternalServerException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(String userId) {
    try {
      rocksDB.delete(userId.getBytes(Charset.defaultCharset()));
      rocksDB.delete((userId + ".lastSnapshotId").getBytes(Charset.defaultCharset()));
    } catch (RocksDBException e) {
      throw new InternalServerException(e);
    }
  }

  /**
   * Close the underling rocks db instance and its writeOptions
   */
  @Override
  public void close() {
    wo.close();
    rocksDB.close();
  }
}
