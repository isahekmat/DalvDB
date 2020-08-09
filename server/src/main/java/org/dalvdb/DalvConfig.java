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

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class DalvConfig {
  //Config Keys
  public static final String DATA_DIR = "data.dir";
  public static final String NODE_ID = "node.id";
  public static final String ZK_CONNECTION_STRING = "zk.connection";
  public static final String ZK_SESSION_TIMEOUT = "zk.sessionTimeout";
  public static final String CLIENT_PORT = "client.port";
  public static final String JWT_SIGN = "jwt.signature";
  public static final String LOCK_TIMEOUT = "lock.timeout";

  //Environment Variable
  private static final String DALV_CONFIG = "DALV_CONFIG";
  private static final String DALV_NODE_ID = "DALV_NODE_ID";

  private static final Map<String, Object> config = new HashMap<>();

  static {
    //put default values
    config.put(DATA_DIR, ".dalv-data/");
    config.put(ZK_CONNECTION_STRING, "localhost:2181");
    config.put(ZK_SESSION_TIMEOUT, 100);
    config.put(CLIENT_PORT, 7472);
    config.put(JWT_SIGN, "havijfarangichekhoobehavijfarangichekhoobehavijfarangichekhoobe");
    config.put(LOCK_TIMEOUT, 200);
  }

  public static void loadFromEnvironmentVariable() throws IOException {
    String dalvConfigFile = System.getenv(DalvConfig.DALV_CONFIG);
    if (dalvConfigFile == null)
      throw new RuntimeException("Dalv config file could not find, please set " +
          DALV_CONFIG + " or provide the config file path as an application argument.");
    loadFromConfig(dalvConfigFile);
  }

  public static void loadFromConfig(String dalvConfigFile) throws IOException {
    Properties props = new Properties();
    props.load(new FileReader(dalvConfigFile));
    for (Object key : props.keySet())
      config.put((String) key, props.get(key));
    String nodeId = System.getenv(DalvConfig.DALV_NODE_ID);
    if (nodeId == null)
      nodeId = (String) config.get(NODE_ID);
    if (nodeId == null)
      nodeId = String.valueOf(UUID.randomUUID());
    config.put(NODE_ID, nodeId);
    validateConfigurations();
  }

  private static void validateConfigurations() {
    //Nothing since now
  }

  public static String getStr(String key) {
    return (String) config.get(key);
  }

  public static Integer getInt(String key) {
    return (Integer) config.get(key);
  }

  public static void setStr(String key, Object val) {
    config.put(key, val);
  }
}
