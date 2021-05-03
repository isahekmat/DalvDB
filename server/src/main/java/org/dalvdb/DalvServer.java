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


import org.dalvdb.cluster.DalvCluster;
import org.dalvdb.service.backend.BackendService;
import org.dalvdb.watch.InMemoryWatchManager;
import org.dalvdb.watch.WatchManager;
import org.dalvdb.service.client.ClientService;
import org.dalvdb.db.storage.RocksStorageService;
import org.dalvdb.db.storage.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * This class contains main method which is the starting point for an instance of a Dalv Server.
 * On bootstrap, an instance of this class will be created and on shutdown this class should be closed.
 * <p>
 * This class also responsible to instantiate and start sub-components of the Dalv server such as Storage-Service or
 * Client-Service
 */
public class DalvServer implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(DalvServer.class);
  private final StorageService storageService;
  private final DalvCluster cluster;
  private final ClientService clientService;
  private final BackendService backendService;

  private DalvServer() {
    this.storageService = new RocksStorageService();
    this.cluster = new DalvCluster();
    WatchManager watchManager = new InMemoryWatchManager();
    this.clientService = new ClientService(this.storageService, watchManager);
    this.backendService = new BackendService(this.storageService, watchManager,this.cluster);
    logger.info("Dalv server started up");
  }


  /**
   * The Starting point of the Dalv Server
   *
   * @param args command line argument which could indicate the configuration file
   */
  public static void main(String[] args) {
    final DalvServer server = startServer(args);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        server.close();
      } catch (IOException e) {
        logger.error("error while shutting down", e);
      }
    }));
  }

  /**
   * This is an internal method which is public just for the integration tests
   */
  public static DalvServer startServer(String[] args) {
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

    return new DalvServer();
  }

  /**
   * Close the all sub-components of DalvServer. on starting, main method add a shutdown hook to close the internal
   * instance of the class.
   */
  @Override
  public void close() throws IOException {
    this.clientService.close();
    this.backendService.close();
    this.cluster.close();
    this.storageService.close();
  }
}
