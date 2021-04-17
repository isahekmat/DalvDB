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

package org.dalvdb.it;

import dalv.common.Common;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

public class WatchIT extends BaseITTest {

  @Test
  public void backendWatchCalledByClientTest() throws InterruptedException {
    final AtomicBoolean finished = new AtomicBoolean(false);
    backend.watch("theme", event -> {
      assertThat(event.getKey()).isEqualTo("theme");
      assertThat(event.getOperationType()).isEqualByComparingTo(Common.OpType.PUT);
      assertThat(event.getUserId()).isEqualTo("esa");
      assertThat(new String(event.getNewValue())).isEqualTo("blue");
      finished.set(true);
      synchronized (finished) {
        finished.notifyAll();
      }
    });
    client.put("theme", "blue".getBytes());
    client.sync();

    synchronized (finished) {
      finished.wait(1000);
    }

    assertThat(finished.get()).isTrue();
  }

  @Test
  public void backendWatchCalledByBackendTest() throws InterruptedException {
    final AtomicBoolean finished = new AtomicBoolean(false);
    backend.watch("theme", event -> {
      assertThat(event.getKey()).isEqualTo("theme");
      assertThat(event.getOperationType()).isEqualByComparingTo(Common.OpType.PUT);
      assertThat(event.getUserId()).isEqualTo("esa");
      assertThat(new String(event.getNewValue())).isEqualTo("blue");
      finished.set(true);
      synchronized (finished) {
        finished.notify();
      }
    });

    Thread.sleep(300);
    backend.put("esa", "theme", "blue".getBytes());


    synchronized (finished) {
      finished.wait(1000);
    }

    assertThat(finished.get()).isTrue();
  }

  @Test
  public void clientWatchCalledByClientTest() throws InterruptedException {
    final AtomicBoolean finished = new AtomicBoolean(false);
    client.watch("theme", event -> {
      assertThat(event.getKey()).isEqualTo("theme");
      assertThat(event.getOperationType()).isEqualByComparingTo(Common.OpType.PUT);
      assertThat(new String(event.getNewValue())).isEqualTo("blue");
      finished.set(true);
      synchronized (finished) {
        finished.notifyAll();
      }
    });
    client.put("theme", "blue".getBytes());
    client.sync();

    synchronized (finished) {
      finished.wait(1000);
    }

    assertThat(finished.get()).isTrue();
  }

  @Test
  public void clientWatchCalledByBackendTest() throws InterruptedException {
    final AtomicBoolean finished = new AtomicBoolean(false);
    client.watch("theme", event -> {
      assertThat(event.getKey()).isEqualTo("theme");
      assertThat(event.getOperationType()).isEqualByComparingTo(Common.OpType.PUT);
      assertThat(new String(event.getNewValue())).isEqualTo("blue");
      finished.set(true);
      synchronized (finished) {
        finished.notify();
      }
    });
    Thread.sleep(300);
    backend.put("esa", "theme", "blue".getBytes());
    synchronized (finished) {
      finished.wait(1000);
    }

    assertThat(finished.get()).isTrue();
  }
}
