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
import org.dalvdb.DalvServer;
import org.dalvdb.client.DalvClient;
import org.dalvdb.client.DalvClientBuilder;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;

public abstract class BaseITTest {
  protected DalvServer server;
  protected DalvClient client;
  protected org.dalvdb.backend.DalvClient backend;

  @Before
  public void setUp() throws IOException {
    server = DalvServer.startServer(new String[]{"testServer.config"});
    setupClient();
    backend = new org.dalvdb.backend.DalvClient(new String[]{"localhost:7470"});
  }

  protected void setupClient() throws IOException {
    FileUtils.forceMkdir(new File("test-client-data"));
    client = new DalvClientBuilder().dataDir("test-client-data")
        .serverAddresses("localhost:7472")
        //userId = "esa"
        .accessToken("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOiJlc2EifQ.Gd3BoQIu3tAX2rxlKsgUMJkG38MbDZxoYmKOQfJ9N4g")
        .build();
  }

  @After
  public void tearDown() throws IOException {
    client.close();
    backend.close();
    server.close();
    FileUtils.deleteDirectory(new File("test-data"));
    FileUtils.deleteDirectory(new File("test-client-data"));
  }
}
