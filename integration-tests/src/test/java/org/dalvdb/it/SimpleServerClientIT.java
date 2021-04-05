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

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class SimpleServerClientIT extends BaseITTest {

  @Test
  public void clientsUpdatesAndGets() throws IOException {
    client.put("name", "Isa".getBytes());
    client.sync();
    assertThat(new String(client.get("name"))).isEqualTo("Isa");

    //delete the data directory and sync again to see the value still exist
    client.close();
    FileUtils.deleteDirectory(new File("test-client-data"));
    setupClient();
    assertThat(client.get("name").length).isEqualTo(0);
    client.sync();
    assertThat(new String(client.get("name"))).isEqualTo("Isa");
  }

  @Test
  public void clientsUpdateAndDelete() {
    client.put("name", "Hassan".getBytes());
    client.sync();
    assertThat(new String(client.get("name"))).isEqualTo("Hassan");
    client.delete("name");
    assertThat(client.get("name").length).isEqualTo(0);
  }

  @Test
  public void listOperationWithPutTest() {
    backend.put("esa", "shoppingCard", "item 1".getBytes());
    backend.addToList("esa", "shoppingCard", "item 2".getBytes());
    backend.addToList("esa", "shoppingCard", "item 3".getBytes());
    backend.removeFromList("esa", "shoppingCard", "item 2".getBytes());
    List<byte[]> response = backend.getAsList("esa", "shoppingCard");
    List<String> items = response.stream().map(String::new).collect(Collectors.toList());
    assertThat(items.contains("item 1")).isTrue();
    assertThat(items.contains("item 3")).isTrue();
    assertThat(items.contains("item 2")).isFalse();
    assertThat(items.size()).isEqualTo(2);
  }

  @Test
  public void listOperationWithSeveralPutTest() {
    backend.put("esa", "shoppingCard", "item 1".getBytes());
    backend.addToList("esa", "shoppingCard", "item 2".getBytes());
    backend.put("esa", "shoppingCard", "item 1".getBytes());
    backend.addToList("esa", "shoppingCard", "item 3".getBytes());
    List<byte[]> response = backend.getAsList("esa", "shoppingCard");
    List<String> items = response.stream().map(String::new).collect(Collectors.toList());
    assertThat(items.contains("item 1")).isTrue();
    assertThat(items.contains("item 3")).isTrue();
    assertThat(items.contains("item 2")).isFalse();
    assertThat(items.size()).isEqualTo(2);
  }

  @Test
  public void listOperationWithDelTest() {
    backend.put("esa", "shoppingCard", "item 1".getBytes());
    backend.addToList("esa", "shoppingCard", "item 2".getBytes());
    backend.del("esa", "shoppingCard");
    backend.addToList("esa", "shoppingCard", "item 3".getBytes());
    List<byte[]> response = backend.getAsList("esa", "shoppingCard");
    List<String> items = response.stream().map(String::new).collect(Collectors.toList());
    assertThat(items.contains("item 3")).isTrue();
    assertThat(items.size()).isEqualTo(1);
  }

  @Test
  public void backendPutAndGet() {
    backend.put("esa", "name", "isa".getBytes());
    backend.put("esa", "age", "30".getBytes());
    assertThat(new String(backend.get("esa", "name"))).isEqualTo("isa");
    assertThat(new String(backend.get("esa", "age"))).isEqualTo("30");
  }
}
