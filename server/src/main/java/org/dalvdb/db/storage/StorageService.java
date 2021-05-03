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

import java.io.Closeable;
import java.util.List;

/**
 * A server-side storage layer, responsible for storing operations for users, generating snapshots and provide access
 * to the user's data
 *
 * @see RocksStorageService the default implementation
 */
public interface StorageService extends Closeable {

  /**
   * handle all operations for a user in an atomic way.
   *
   * @param userId         the user identification
   * @param opsList        list of operation to handle
   * @param lastSnapshotId last snapshotId seen by user, for conflict detection
   * @return true if operations persisted successfully, false if any conflict detected
   */
  boolean handleOperations(String userId, List<Common.Operation> opsList, int lastSnapshotId);

  /**
   * add a single operation for a specific user
   *
   * @param userId    the user identification
   * @param operation the operation to add
   */
  void addOperation(String userId, Common.Operation operation);

  /**
   * Get the list of operations for a user after the lastSnapshotId
   *
   * if the lastSnapshotId does not exist and greater than 0
   * then add a remove all operation in the beginning of the result
   *
   * @param userId         the user identification
   * @param lastSnapshotId return operations after this snapshot
   * @return the list of operations for a specific user which occur after the lastSnapshotId
   */
  List<Common.Operation> get(String userId, int lastSnapshotId);

  /**
   * get the value of a specific key for a user
   *
   * @param userId the user identification
   * @param key    the key to query
   * @return the value corresponding to the provided key
   */
  ByteString getValue(String userId, String key);

  /**
   * add a snapshot in the user's log and return the newly generated snapshot id
   *
   * @param userId the user identification
   * @return newly generated snapshot id
   */
  int snapshot(String userId);

  /**
   * Delete the information of a specific user
   *
   * @param userId the user identification
   */
  void delete(String userId);

  /**
   * Run compaction process on a specific user
   *
   * @param userId the user which the storage want to compact it's data
   */
  void compact(String userId);
}
