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

package org.dalvdb;


import org.dalvdb.service.backend.BackendService;
import org.dalvdb.service.client.ClientService;
import org.dalvdb.storage.RocksStorageService;
import org.dalvdb.storage.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

public class DalvServer implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(DalvServer.class);
  private final StorageService storageService;
  private final DalvCluster cluster;
  private final ClientService clientService;
  private final BackendService backendService;

  private DalvServer() {
    this.storageService = new RocksStorageService();
    this.cluster = new DalvCluster();
    this.clientService = new ClientService(this.storageService);
    this.backendService = new BackendService(this.storageService);
    logger.info("Dalv server started up");
  }


  public static void main(String[] args) {
    try {
      if (args.length == 0)
        DalvConfig.loadFromEnvironmentVariable();
      else if (args.length == 1)
        DalvConfig.loadFromConfig(args[0]);
      else {
        System.err.println("USAGE: java " + DalvServer.class.getName() +
            " [dalv_config_dir]");
        System.exit(1);
      }
    } catch (Exception e) {
      logger.error("Error while loading configuration", e);
      System.err.println("Error while loading configuration. Will exit.");
      System.exit(1);
    }

    final DalvServer server = new DalvServer();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        server.close();
      } catch (IOException e) {
        logger.error("error while shutting down", e);
      }
    }));
  }

  @Override
  public void close() throws IOException {
    this.clientService.close();
    this.backendService.close();
    this.cluster.close();
    this.storageService.close();
  }
}
