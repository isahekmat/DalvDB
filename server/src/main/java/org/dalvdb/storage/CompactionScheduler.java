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

import org.dalvdb.DalvConfig;
import org.rocksdb.RocksIterator;

import java.io.Closeable;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

class CompactionScheduler implements Closeable {
  private final LinkedHashSet<String> queue = new LinkedHashSet<>();
  private final ScheduledExecutorService compactionEs = new ScheduledThreadPoolExecutor(1);
  private final ScheduledExecutorService addToQueueEs = new ScheduledThreadPoolExecutor(1);

  private final RocksStorageService storage;

  CompactionScheduler(RocksStorageService storage) {
    this.storage = storage;
  }

  void startScheduler() {
    preFillTheUpdates();
    compactionEs.scheduleWithFixedDelay(this::compaction, DalvConfig.getLong(DalvConfig.COMPACTION_INTERVAL),
        DalvConfig.getLong(DalvConfig.COMPACTION_INTERVAL), TimeUnit.SECONDS);
  }

  void updateReceived(String userId) {
    addToQueueEs.schedule(() -> {
      queue.add(userId);
    }, DalvConfig.getLong(DalvConfig.COMPACTION_DELAY), TimeUnit.SECONDS);
  }

  private void preFillTheUpdates() {
    RocksIterator rocksIterator = storage.keyIterator();
    while (rocksIterator.isValid()) {
      queue.add(new String(rocksIterator.key()));
      rocksIterator.next();
    }
  }

  private void compaction() {
    Iterator<String> it = queue.iterator();
    if (it.hasNext()) {
      String keyToCompact = it.next();
      storage.compact(keyToCompact);
      queue.remove(keyToCompact);
    }
  }

  @Override
  public void close() {
    compactionEs.shutdown();
    addToQueueEs.shutdown();
  }
}
