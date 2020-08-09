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

import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

public class DalvCluster implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(DalvCluster.class);
  private final ZooKeeper zk;

  public DalvCluster() {
    String zkConnectionStr = DalvConfig.getStr(DalvConfig.ZK_CONNECTION_STRING);
    int zkSessionTimeout = DalvConfig.getInt(DalvConfig.ZK_SESSION_TIMEOUT);
    ZooKeeper tempZk = null;
    try {
      tempZk = new ZooKeeper(zkConnectionStr, zkSessionTimeout, new ZKWatcher());
    } catch (IOException e) {
      logger.error("could not connect to zookeeper by the address {}", zkConnectionStr, e);
      System.exit(1);
    }
    zk = tempZk;
    //TODO: takeover leader role for some users from the cluster on startup
  }

  @Override
  public void close() throws IOException {
    try {
      zk.close();
    } catch (InterruptedException e) {
      throw new IOException("could not close zookeeper connection", e);
    }
  }
}
