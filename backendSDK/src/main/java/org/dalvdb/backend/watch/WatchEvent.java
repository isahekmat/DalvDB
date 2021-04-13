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

package org.dalvdb.backend.watch;

import dalv.common.Common;

import java.util.Arrays;

public class WatchEvent {
  private String userId;
  private Common.OpType operationType;
  private byte[] newValue;
  private String key;

  public String getUserId() {
    return userId;
  }

  public WatchEvent setUserId(String userId) {
    this.userId = userId;
    return this;
  }

  public Common.OpType getOperationType() {
    return operationType;
  }

  public WatchEvent setOperationType(Common.OpType operationType) {
    this.operationType = operationType;
    return this;
  }

  public byte[] getNewValue() {
    return newValue;
  }

  public WatchEvent setNewValue(byte[] newValue) {
    this.newValue = newValue;
    return this;
  }

  public String getKey() {
    return key;
  }

  public WatchEvent setKey(String key) {
    this.key = key;
    return this;
  }

  @Override
  public String toString() {
    return "WatchEvent{" +
        "userId='" + userId + '\'' +
        ", operationType=" + operationType +
        ", newValue=" + Arrays.toString(newValue) +
        ", key='" + key + '\'' +
        '}';
  }
}
