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

  boolean addOperation(String userId, Common.Operation operation);

  /**
   * get the list of user's operations after the lastSnapshotId
   *
   * @param userId         the user identification
   * @param lastSnapshotId last snapshotId seen by user
   * @return the list of user's operations which occur after the lastSnapshotId
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
}
