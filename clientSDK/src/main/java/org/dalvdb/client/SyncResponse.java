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

import org.dalvdb.client.conflict.Conflict;

import java.util.Collections;
import java.util.List;

public class SyncResponse {
  private final List<Conflict> conflicts;
  private final int resolvedSnapShot;

  public SyncResponse(List<Conflict> conflicts, int resolvedSnapShot) {
    this.conflicts = conflicts;
    this.resolvedSnapShot = resolvedSnapShot;
  }

  public SyncResponse(int snapshotId) {
    conflicts = Collections.emptyList();
    this.resolvedSnapShot = snapshotId;
  }

  public boolean hasConflict() {
    return !conflicts.isEmpty();
  }

  public List<Conflict> getConflicts() {
    return conflicts;
  }

  public int getResolvedSnapShot() {
    return resolvedSnapShot;
  }
}
