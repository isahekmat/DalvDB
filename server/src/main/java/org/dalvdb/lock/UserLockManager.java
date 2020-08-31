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

package org.dalvdb.lock;

import com.google.common.annotations.VisibleForTesting;
import org.dalvdb.DalvConfig;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Singleton class that its single instance keep a list of {@link ReadWriteLock} for users. each user has a single lock
 * It acts as a LRU cache, and delete locks once the number of locks reach the threshold, however, it does not delete
 * any locks that that currently hold.
 */
public class UserLockManager {
  private static UserLockManager instance;
  private final Map<String, LockEntry> locks = new ConcurrentHashMap<>();
  private final ReadWriteLock pruningLock = new ReentrantReadWriteLock();
  private final int threshold;

  @VisibleForTesting
  UserLockManager(int threshold) {
    this.threshold = threshold;
  }

  public static synchronized UserLockManager getInstance() {
    if (instance == null) {
      instance = new UserLockManager(DalvConfig.getInt(DalvConfig.LOCK_SIZE_THRESHOLD));
    }
    return instance;
  }

  public boolean tryReadLock(String userId, long timeout) throws InterruptedException {
    LockEntry lockEntry = getLock(userId);
    if (lockEntry != null && lockEntry.lock.readLock().tryLock(timeout, TimeUnit.MILLISECONDS)) {
      pruningLock.readLock().lock();
      lockEntry.numberOfHold.incrementAndGet();
      lockEntry.lastUsedTimestamp = System.currentTimeMillis();
      pruningLock.readLock().unlock();
      return true;
    }
    return false;
  }

  public boolean tryWriteLock(String userId, long timeout) throws InterruptedException {
    LockEntry lockEntry = getLock(userId);
    if (lockEntry != null && lockEntry.lock.writeLock().tryLock(timeout, TimeUnit.MILLISECONDS)) {
      pruningLock.readLock().lock();
      lockEntry.numberOfHold.incrementAndGet();
      lockEntry.lastUsedTimestamp = System.currentTimeMillis();
      pruningLock.readLock().unlock();
      return true;
    }
    return false;
  }

  public void releaseReadLock(String userId) {
    LockEntry lockEntry = getLock(userId);
    Objects.requireNonNull(lockEntry).lock.readLock().unlock();
    lockEntry.numberOfHold.decrementAndGet();
  }

  public void releaseWriteLock(String userId) {
    LockEntry lockEntry = getLock(userId);
    Objects.requireNonNull(lockEntry).lock.writeLock().unlock();
    lockEntry.numberOfHold.decrementAndGet();
  }

  private LockEntry getLock(String userId) {
    return locks.computeIfAbsent(userId, (k) -> {
      if (locks.size() == threshold && !prune())
        return null;
      return new LockEntry();
    });
  }

  private boolean prune() {
    Lock lock = pruningLock.writeLock();
    lock.lock();
    AtomicBoolean success = new AtomicBoolean(false);
    Optional<Map.Entry<String, LockEntry>> removeCandidate = locks.entrySet().stream()
        .filter(e -> e.getValue().numberOfHold.intValue() == 0)
        .min(Comparator.comparing(e -> e.getValue().lastUsedTimestamp));
    removeCandidate.map(Map.Entry::getKey).ifPresent(key -> {
      locks.remove(key);
      success.set(true);
    });
    lock.unlock();
    return success.get();
  }

  private static class LockEntry {
    private final ReadWriteLock lock;
    private Long lastUsedTimestamp;
    private final AtomicInteger numberOfHold;

    public LockEntry() {
      this.lastUsedTimestamp = System.currentTimeMillis();
      numberOfHold = new AtomicInteger(0);
      this.lock = new ReentrantReadWriteLock();
    }
  }
}
