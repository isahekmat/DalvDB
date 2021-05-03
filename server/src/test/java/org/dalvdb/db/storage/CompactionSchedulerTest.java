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

import org.dalvdb.DalvConfig;
import org.junit.Test;
import org.mockito.Mockito;
import org.rocksdb.RocksIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;


public class CompactionSchedulerTest {

  @Test
  public void testPreFillCompaction() throws InterruptedException {
    Object monitor = new Object();
    List<String> calls = new ArrayList<>();
    AtomicInteger twoIsValid = new AtomicInteger(2);
    AtomicInteger twoKey = new AtomicInteger(2);
    RocksIterator mockIterator = Mockito.mock(RocksIterator.class);
    Mockito.when(mockIterator.isValid()).thenAnswer(invocation -> twoIsValid.getAndDecrement() > 0);
    Mockito.when(mockIterator.key()).thenAnswer(invocation -> {
      int i = twoKey.getAndDecrement();
      if (i == 2) {
        return "key1".getBytes();
      } else if (i == 1)
        return "key2".getBytes();
      else throw new IllegalStateException();
    });
    RocksStorageService mockStorage = Mockito.mock(RocksStorageService.class);
    Mockito.when(mockStorage.keyIterator()).thenReturn(mockIterator);
    Mockito.doAnswer(invocation -> {
      calls.add(invocation.getArgument(0));
      synchronized (monitor) {
        monitor.notify();
      }
      return monitor;
    })
        .when(mockStorage).compact(Mockito.anyString());
    DalvConfig.set(DalvConfig.COMPACTION_DELAY, 1L);
    DalvConfig.set(DalvConfig.COMPACTION_INTERVAL, 2L);
    CompactionScheduler cs = new CompactionScheduler(mockStorage);
    cs.startScheduler();
    synchronized (monitor) {
      monitor.wait();
    }
    synchronized (monitor) {
      monitor.wait();
    }
    assertThat(calls.size()).isEqualTo(2);
    assertThat(calls.get(0)).isEqualTo("key1");
    assertThat(calls.get(1)).isEqualTo("key2");
  }

  @Test
  public void testOrderOfCompaction() throws InterruptedException {
    Object monitor = new Object();
    List<String> calls = new ArrayList<>();
    RocksIterator mockIterator = Mockito.mock(RocksIterator.class);
    Mockito.when(mockIterator.isValid()).thenReturn(false);
    RocksStorageService mockStorage = Mockito.mock(RocksStorageService.class);
    Mockito.when(mockStorage.keyIterator()).thenReturn(mockIterator);
    Mockito.doAnswer(invocation -> {
      calls.add(invocation.getArgument(0));
      synchronized (monitor) {
        monitor.notify();
      }
      return monitor;
    })
        .when(mockStorage).compact(Mockito.anyString());
    DalvConfig.set(DalvConfig.COMPACTION_DELAY, 1L);
    DalvConfig.set(DalvConfig.COMPACTION_INTERVAL, 2L);
    CompactionScheduler cs = new CompactionScheduler(mockStorage);
    cs.startScheduler();
    cs.updateReceived("key1");
    cs.updateReceived("key2");
    cs.updateReceived("key1");
    cs.updateReceived("key2");
    cs.updateReceived("key3");
    cs.updateReceived("key3");
    synchronized (monitor) {
      monitor.wait();
    }
    synchronized (monitor) {
      monitor.wait();
    }
    synchronized (monitor) {
      monitor.wait();
    }
    assertThat(calls.size()).isEqualTo(3);
    assertThat(calls.get(0)).isEqualTo("key1");
    assertThat(calls.get(1)).isEqualTo("key2");
    assertThat(calls.get(2)).isEqualTo("key3");
  }

}