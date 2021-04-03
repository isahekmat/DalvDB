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

import org.apache.commons.io.FileUtils;
import org.dalvdb.DalvServer;
import org.dalvdb.client.DalvClient;
import org.dalvdb.client.DalvClientBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class SimpleServerClientIT {
  private DalvServer server;
  private DalvClient client;

  @Before
  public void setUp() throws IOException {
    server = DalvServer.startServer(new String[]{"testServer.config"});
    setupClient();
  }

  private void setupClient() throws IOException {
    FileUtils.forceMkdir(new File("test-client-data"));
    client = new DalvClientBuilder().dataDir("test-client-data")
        .serverAddresses("localhost:7472")
        .accessToken("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOiJlc2EifQ.Gd3BoQIu3tAX2rxlKsgUMJkG38MbDZxoYmKOQfJ9N4g")
        .build();
  }

  @After
  public void tearDown() throws IOException {
    server.close();
    client.close();
    FileUtils.deleteDirectory(new File("test-data"));
    FileUtils.deleteDirectory(new File("test-client-data"));
  }

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
