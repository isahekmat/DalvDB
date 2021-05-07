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

package org.dalvdb.db.storage;

import com.google.protobuf.ByteString;
import dalv.common.Common;
import org.dalvdb.DalvConfig;
import org.dalvdb.common.util.ByteUtil;
import org.dalvdb.common.util.OpUtil;
import org.dalvdb.exception.InternalServerException;
import org.dalvdb.lock.UserLockManager;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;

/**
 * The default implementation of {@link StorageService} which employs RocksDB as the internal storage engine to store data on
 * permanent storage
 */
public class RocksStorageService implements StorageService {
  private static final Logger logger = LoggerFactory.getLogger(RocksStorageService.class);
  private static RocksStorageService instance;
  private final RocksDB rocksDB;
  private final WriteOptions wo;
  private final Map<String, Queue<Common.Operation>> mirroredUser = new HashMap<>();
  private final ColumnFamilyHandle metaData;
  private final CompactionScheduler compactionScheduler;

  public static synchronized RocksStorageService getInstance() {
    if (instance == null) {
      instance = new RocksStorageService();
    }
    return instance;
  }

  private RocksStorageService() {
    logger.debug("rocks storage starting...");
    String dataDir = DalvConfig.getStr(DalvConfig.DATA_DIR);
    RocksDB db = null;
    WriteOptions writeOptions = null;
    ColumnFamilyHandle metadataHandler = null;
    try {
      Options options = new Options();
      options.setCreateIfMissing(true);
      options.setCreateMissingColumnFamilies(true);
      options.setMergeOperator(new StringAppendOperator((char) (0)));
      List<byte[]> cfs = RocksDB.listColumnFamilies(options, dataDir);
      if (cfs.size() == 2) { //meta and default column families exist
        List<ColumnFamilyHandle> hs = new LinkedList<>();
        List<ColumnFamilyDescriptor> cfdList = new LinkedList<>();
        cfdList.add(new ColumnFamilyDescriptor("default".getBytes(), new ColumnFamilyOptions(options)));
        cfdList.add(new ColumnFamilyDescriptor("meta".getBytes(), new ColumnFamilyOptions(options)));
        db = RocksDB.open(dataDir, cfdList, hs);
        metadataHandler = hs.get(0);
      } else {
        db = RocksDB.open(options, dataDir);
        metadataHandler = db.createColumnFamily(new ColumnFamilyDescriptor("meta".getBytes()));
      }
      writeOptions = new WriteOptions();
      writeOptions.setSync(true);
    } catch (RocksDBException e) {
      logger.error("could not open storage dir", e);
      System.exit(1);
    }
    this.rocksDB = db;
    this.wo = writeOptions;
    this.metaData = metadataHandler;
    this.compactionScheduler = new CompactionScheduler(this);
    this.compactionScheduler.startScheduler();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean handleOperations(String userId, List<Common.Operation> opsList, int lastSnapshotId) {
    if (checkForConflict(get(userId, lastSnapshotId), opsList))
      return false;
    try {
      Queue<Common.Operation> mirrorQueue = mirroredUser.get(userId);
      byte[] key = userId.getBytes(Charset.defaultCharset());
      WriteBatch wb = new WriteBatch();
      for (Common.Operation operation : opsList) {
        wb.merge(key, ByteUtil.opToByte(operation));
        if (mirrorQueue != null) mirrorQueue.offer(operation);
      }

      rocksDB.write(wo, wb);
      compactionScheduler.updateReceived(userId);
      return true;
    } catch (RocksDBException e) {
      throw new InternalServerException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addOperation(String userId, Common.Operation operation) {
    try {
      byte[] key = userId.getBytes(Charset.defaultCharset());
      rocksDB.merge(key, ByteUtil.opToByte(operation));

      Queue<Common.Operation> mirrorQueue = mirroredUser.get(userId);
      if (mirrorQueue != null) mirrorQueue.offer(operation);

      compactionScheduler.updateReceived(userId);
    } catch (RocksDBException e) {
      throw new InternalServerException(e);
    }
  }

  private boolean checkForConflict(List<Common.Operation> oldOps,
                                   List<Common.Operation> newOps) {
    if (oldOps.isEmpty()) return false;
    if (oldOps.get(0).equals(OpUtil.REMOVE_ALL_OP)) return true;
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
      Iterator<Common.Operation> operationIterator = ByteUtil.getReverseIterator(bytes);
      LinkedList<Common.Operation> result = new LinkedList<>();
      boolean foundSnapshot = false;
      while (operationIterator.hasNext()) {
        Common.Operation op = operationIterator.next();
        if (op.getType() == Common.OpType.SNAPSHOT && op.getSnapshotId() == lastSnapshotId) {
          foundSnapshot = true;
          break;
        }
        result.addFirst(op);
      }
      if (!foundSnapshot && lastSnapshotId > 0)
        result.addFirst(OpUtil.REMOVE_ALL_OP);
      return result;
    } catch (RocksDBException e) {
      throw new InternalServerException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ByteString getValue(String userId, String key) {
    try {
      Iterator<Common.Operation> reverseOpsIterator =
          ByteUtil.getReverseIterator(rocksDB.get(userId.getBytes(Charset.defaultCharset())));

      Set<ByteString> additionToList = new HashSet<>();
      Set<ByteString> deletionFromList = new HashSet<>();

      int resultLen = 0;
      while (reverseOpsIterator.hasNext()) {
        Common.Operation op = reverseOpsIterator.next();
        if (key.equals(op.getKey())) {
          switch (op.getType()) {
            case PUT:
              additionToList.add(op.getVal());
              resultLen += op.getVal().size();
              return constructValue(additionToList, resultLen);
            case DEL:
              return constructValue(additionToList, resultLen);
            case ADD_TO_LIST:
              if (!deletionFromList.contains(op.getVal())) {
                resultLen += op.getVal().size();
                additionToList.add(op.getVal());
              }
              break;
            case REMOVE_FROM_LIST:
              deletionFromList.add(op.getVal());
              break;
          }
        }
      }
      return constructValue(additionToList, resultLen);
    } catch (RocksDBException e) {
      throw new InternalServerException(e);
    }
  }

  private ByteString constructValue(Set<ByteString> vals, int len) {
    if (vals.isEmpty()) return ByteString.EMPTY;
    //bufferSize = (total values len) + (4 byte for each value len)
    ByteBuffer buffer = ByteBuffer.allocate(len + 4 * (vals.size()));
    for (ByteString val : vals) {
      buffer.putInt(val.size());
      buffer.put(val.toByteArray());
    }
    return ByteString.copyFrom(buffer.array());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int snapshot(String userId) {
    try {
      WriteBatch wb = new WriteBatch();
      int snapshotId = snapshot(userId, wb);
      rocksDB.write(wo, wb);
      return snapshotId;
    } catch (RocksDBException e) {
      throw new InternalServerException(e);
    }
  }

  public int snapshot(String userId, WriteBatch wb) throws RocksDBException {
    int snapshotId = lastSnapshotId(userId) + 1;
    Common.Operation op = Common.Operation.newBuilder().setType(Common.OpType.SNAPSHOT)
        .setSnapshotId(snapshotId).build();
    wb.merge(userId.getBytes(Charset.defaultCharset()), ByteUtil.opToByte(op));
    wb.put(metaData, (userId + ".lastSnapshotId").getBytes(Charset.defaultCharset()),
        ByteBuffer.allocate(4).putInt(snapshotId).array());
    return snapshotId;
  }

  private int lastSnapshotId(String userId) {
    try {
      byte[] lastSnapShotId = rocksDB.get(metaData, (userId + ".lastSnapshotId").getBytes(Charset.defaultCharset()));
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
      rocksDB.delete(metaData, (userId + ".lastSnapshotId").getBytes(Charset.defaultCharset()));
    } catch (RocksDBException e) {
      throw new InternalServerException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void compact(String userId) {
    byte[] userKey = userId.getBytes(Charset.defaultCharset());
    OpUtil.OperatorsReverseIterator iterator;
    try {
      //it's a write lock because we start mirroring and we don't want any other write interfere this
      if (UserLockManager.getInstance().tryWriteLock(userId, 10)) {
        try {
          iterator = new OpUtil.OperatorsReverseIterator(rocksDB.get(userKey));
          //start mirroring
          mirroredUser.put(userId, new LinkedList<>());
        } catch (RocksDBException e) {
          throw new InternalServerException(e);
        } finally {
          UserLockManager.getInstance().releaseWriteLock(userId);
        }
      } else return;
    } catch (InterruptedException e) {
      logger.error(e.getMessage(), e);
      return;
    }

    List<Common.Operation> result = compactOperations(iterator);
    mergeBack(userId, result);
  }

  private List<Common.Operation> compactOperations(OpUtil.OperatorsReverseIterator iterator) {
    Set<String> ignoreKeys = new HashSet<>();
    Map<String, List<ByteString>> ignoreItemInList = new HashMap<>();
    LinkedList<Common.Operation> result = new LinkedList<>();
    boolean first = true;
    while (iterator.hasNext()) {
      Common.Operation op = iterator.next();
      if (first) {
        if (op.getType() == Common.OpType.SNAPSHOT) {
          result.addFirst(op);
          continue;
        }
        first = false;
      }
      if (op.getType() == Common.OpType.SNAPSHOT || ignoreKeys.contains(op.getKey())) continue;
      if ((op.getType() == Common.OpType.ADD_TO_LIST || op.getType() == Common.OpType.PUT) &&
          ignoreItemInList.containsKey(op.getKey()) &&
          ignoreItemInList.get(op.getKey()).contains(op.getVal()))
        continue;
      if (op.getType() != Common.OpType.DEL && op.getType() != Common.OpType.REMOVE_FROM_LIST)
        result.addFirst(op);
      if (op.getType() == Common.OpType.PUT || op.getType() == Common.OpType.DEL)
        ignoreKeys.add(op.getKey());
      else if (op.getType() == Common.OpType.REMOVE_FROM_LIST) {
        ignoreItemInList.putIfAbsent(op.getKey(), new LinkedList<>());
        ignoreItemInList.get(op.getKey()).add(op.getVal());
      }
    }
    return result;
  }

  private void mergeBack(String userId, List<Common.Operation> result) {
    byte[] userKey = userId.getBytes(Charset.defaultCharset());
    try {
      if (UserLockManager.getInstance().tryWriteLock(userId, 10)) {
        try {
          if (!result.isEmpty() &&
              result.get(result.size() - 1).getType() == Common.OpType.SNAPSHOT &&
              !mirroredUser.get(userId).isEmpty())
            result.remove(result.size() - 1);
          result.addAll(mirroredUser.get(userId));
          WriteBatch wb = new WriteBatch();
          wb.delete(userKey);
          boolean seeSnapshot = false;
          for (Common.Operation operation : result) {
            if (operation.getType() == Common.OpType.SNAPSHOT) seeSnapshot = true;
            wb.merge(userKey, ByteUtil.opToByte(operation));
          }
          if (!seeSnapshot)
            snapshot(userId, wb);
          rocksDB.write(wo, wb);
        } catch (RocksDBException e) {
          throw new InternalServerException(e);
        } finally {
          UserLockManager.getInstance().releaseWriteLock(userId);
        }
      }
    } catch (InterruptedException e) {
      logger.error(e.getMessage(), e);
    } finally {
      //stop mirroring
      mirroredUser.remove(userId);
    }
  }

  RocksIterator keyIterator() {
    return rocksDB.newIterator();
  }

  /**
   * Close the underling rocks db instance and its writeOptions
   */
  @Override
  public void close() {
    compactionScheduler.close();
    metaData.close();
    wo.close();
    rocksDB.close();
    synchronized (RocksStorageService.class) {
      instance = null;
    }
  }
}
