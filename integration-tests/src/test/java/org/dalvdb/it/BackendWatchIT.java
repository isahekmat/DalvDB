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

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

public class BackendWatchIT extends BaseITTest {

  @Test
  public void watchCalledTest() throws InterruptedException {
    AtomicReference<Boolean> finished = new AtomicReference<>(false);
    backend.watch("theme", event -> {
      synchronized (finished) {
        assertThat(event.getKey()).isEqualTo("theme");
        assertThat(event.getOperationType()).isEqualByComparingTo(Common.OpType.PUT);
        assertThat(event.getUserId()).isEqualTo("esa");
        assertThat(new String(event.getNewValue())).isEqualTo("blue");
        finished.set(true);
        finished.notifyAll();
      }
    });
    client.put("theme", "blue".getBytes());
    client.sync();

    int i = 10;
    synchronized (finished) {
      while (!finished.get() && i-- > 0)
        finished.wait(10);
    }
    assertThat(i).isGreaterThan(0);
  }
}
