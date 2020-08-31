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

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

public class UserLockManagerTest {

  @Test
  public void getWriteLockBlockReadLocks() throws InterruptedException {
    UserLockManager manager = UserLockManager.getInstance();
    assertThat(manager.tryWriteLock("esa", 0)).isTrue();
    AtomicReference<AssertionError> assertionError = new AtomicReference<>();
    Object monitor = new Object();
    new Thread(() -> {
      synchronized (monitor) {
        try {
          assertThat(manager.tryReadLock("esa", 0)).isFalse();
          assertThat(manager.tryWriteLock("esa", 0)).isFalse();
        } catch (AssertionError e) {
          assertionError.set(e);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        monitor.notify();
      }
    }).start();
    synchronized (monitor) {
      monitor.wait();
    }
    if (assertionError.get() != null)
      throw assertionError.get();
    manager.releaseWriteLock("esa");
  }

  @Test
  public void getSeveralReadLocksBlockWriteLocks() throws InterruptedException {
    UserLockManager manager = UserLockManager.getInstance();
    assertThat(manager.tryReadLock("esa", 0)).isTrue();
    Object monitor = new Object();
    AtomicReference<AssertionError> assertionError = new AtomicReference<>();

    new Thread(() -> {
      try {
        assertThat(manager.tryReadLock("esa", 0)).isTrue();
        Object secondMonitor = new Object();
        new Thread(() -> {
          synchronized (secondMonitor) {
            try {
              assertThat(manager.tryWriteLock("esa", 0)).isFalse();
            } catch (AssertionError e) {
              assertionError.set(e);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            secondMonitor.notify();
          }
        }).start();
        synchronized (secondMonitor) {
          try {
            secondMonitor.wait();
            manager.releaseReadLock("esa");
          } catch (InterruptedException e) {
            Assertions.fail(e.getMessage());
          }
        }
      } catch (AssertionError e) {
        assertionError.set(e);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      synchronized (monitor) {
        monitor.notify();
      }
    }).start();

    synchronized (monitor) {
      monitor.wait();
    }
    if (assertionError.get() != null)
      throw assertionError.get();
    manager.releaseReadLock("esa");
  }

  @Test
  public void getWriteLocksOfDifferentUser() throws InterruptedException {
    UserLockManager manager = UserLockManager.getInstance();
    assertThat(manager.tryWriteLock("esa", 0)).isTrue();
    AtomicReference<AssertionError> assertionError = new AtomicReference<>();
    Object monitor = new Object();
    new Thread(() -> {
      synchronized (monitor) {
        try {
          assertThat(manager.tryWriteLock("ali", 0)).isTrue();
          manager.releaseWriteLock("ali");
        } catch (AssertionError e) {
          assertionError.set(e);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        monitor.notify();
      }
    }).start();
    synchronized (monitor) {
      monitor.wait();
    }
    if (assertionError.get() != null)
      throw assertionError.get();
    manager.releaseWriteLock("esa");
  }

  @Test
  public void blockUntilRelease() throws InterruptedException {
    UserLockManager manager = new UserLockManager(2);
    assertThat(manager.tryReadLock("esa", 0)).isTrue();
    assertThat(manager.tryReadLock("sia", 0)).isTrue();
    assertThat(manager.tryReadLock("nazi", 0)).isFalse();
    manager.releaseReadLock("esa");
    assertThat(manager.tryReadLock("nazi", 0)).isTrue();
    manager.releaseReadLock("sia");
    manager.releaseReadLock("nazi");
  }

}