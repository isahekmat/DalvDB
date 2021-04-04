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

package org.dalvdb.it;import org.apache.commons.io.FileUtils;
import org.dalvdb.it.BaseITTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class SimpleServerClientIT extends BaseITTest {

  @Test
  public void clientsUpdatesAndGets() throws IOException {
    client.put("name", "Isa".getBytes());
    client.sync();
    Assert.assertEquals(new String(client.get("name")), "Isa");

    //delete the data directory and sync again to see the value still exist
    client.close();
    FileUtils.deleteDirectory(new File("test-client-data"));
    setupClient();
    Assert.assertEquals(client.get("name").length, 0);
    client.sync();
    Assert.assertEquals(new String(client.get("name")), "Isa");
  }

  @Test
  public void clientsUpdateAndDelete() {
    client.put("name", "Hassan".getBytes());
    client.sync();
    Assert.assertEquals(new String(client.get("name")), "Hassan");
    client.delete("name");
    Assert.assertEquals(client.get("name").length, 0);
  }
}
