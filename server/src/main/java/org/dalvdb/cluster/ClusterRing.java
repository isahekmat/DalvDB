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

package org.dalvdb.cluster;

import org.apache.zookeeper.*;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.data.ACL;
import org.dalvdb.DalvConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

public class ClusterRing implements Closeable, Watcher {
  private static final Logger logger = LoggerFactory.getLogger(ClusterRing.class);
  private static ClusterRing instance;

  private final static String BASE_PATH = "/dalv";
  private final static String NODE_PATH = BASE_PATH + "/node";
  private final static String RING_PATH = BASE_PATH + "/ring";
  private final static byte[] BOOTSTRAP = "bootstrap".getBytes();
  private final static byte[] RUNNING = "running".getBytes();

  private final ZooKeeper zk;
  private final String nodeId;

  private final TreeSet<Node> nodes = new TreeSet<>();

  public synchronized static ClusterRing getInstance() {
    if (instance == null) {
      instance = new ClusterRing();
    }
    return instance;
  }

  private ClusterRing() {
    String zkConnectionStr = DalvConfig.getStr(DalvConfig.ZK_CONNECTION_STRING);
    int zkSessionTimeout = DalvConfig.getInt(DalvConfig.ZK_SESSION_TIMEOUT);
    ZooKeeper tempZk = null;
    try {
      tempZk = new ZooKeeper(zkConnectionStr, zkSessionTimeout, this, getZkConfig());
    } catch (IOException e) {
      logger.error("could not connect to zookeeper by the address {}", zkConnectionStr, e);
      System.exit(1);
    }
    zk = tempZk;
    nodeId = DalvConfig.getStr(DalvConfig.NODE_ID);
    try {
      bootstrap();
    } catch (KeeperException | InterruptedException e) {
      logger.error("could not bootstrap in cluster", e);
      System.exit(1);
    }
  }

  private ZKClientConfig getZkConfig() {
    ZKClientConfig cfg = new ZKClientConfig();
    cfg.setProperty("zookeeper.sasl.client", "false");
    return cfg;
  }

  private void bootstrap() throws KeeperException, InterruptedException {
    buildPathsInZK();
    //TODO: create ephemeral node in ZK
    List<ACL> acls = new ArrayList<>();
    acls.add(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));
    zk.create(NODE_PATH + "/" + nodeId, BOOTSTRAP, acls, CreateMode.EPHEMERAL);

    //TODO: pick tokens
    //sample
    int[] tokens = new int[]{1, 1 << 6, 1 << 12, 1 << 14, 1 << 18, 1 << 22, 1 << 23, 1 << 25, 1 << 26, 1 << 27};
    //TODO: ask for data from other nodes

    //TODO: watch node list
    //TODO: setup propagators
    //TODO: change status for the node in ZK

  }

  private void buildPathsInZK() {
    List<ACL> acls = new ArrayList<>();
    acls.add(new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.ANYONE_ID_UNSAFE));
    try {
      zk.create(BASE_PATH, null, acls, CreateMode.PERSISTENT);
      zk.create(NODE_PATH, null, acls, CreateMode.PERSISTENT);
      zk.create(RING_PATH, null, acls, CreateMode.PERSISTENT);
    } catch (KeeperException | InterruptedException e) {
    }
  }

  @Override
  public void close() throws IOException {
    try {
      zk.close();
    } catch (InterruptedException e) {
      throw new IOException("could not close zookeeper connection", e);
    }
  }

  @Override
  public void process(WatchedEvent event) {
    System.out.println(event);
    //TODO
  }

  public String leaderOf(String key) {
    return "";
    //TODO
  }

  public List<Node> replicas(String key) {
    //TODO
    return null;
  }
}
