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

/**
 * Static class which handles all the Dalv Configuration,
 * Read the configuration file supplied to loadFromConfig method. and answer to each part of the application need
 * a particular configuration by its getter methods.
 * <p>
 * This class also provides a list of all configuration keys in a form of constant fields.
 */
public final class DalvConfig {
  //Config Keys
  public static final String DATA_DIR = "data.dir";
  public static final String NODE_ID = "node.id";
  public static final String SINGLETON_MODE = "singleton.mode";
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
    config.put(SINGLETON_MODE, true);
    config.put(DATA_DIR, ".dalv-data/");
    config.put(ZK_CONNECTION_STRING, "localhost:2181");
    config.put(ZK_SESSION_TIMEOUT, 100);
    config.put(CLIENT_PORT, 7472);
    config.put(JWT_SIGN, "havijfarangichekhoobehavijfarangichekhoobehavijfarangichekhoobe");
    config.put(LOCK_TIMEOUT, 200);
  }

  private DalvConfig() {
    throw new IllegalStateException();
  }

  /**
   * Load configurations, from the Dalv configuration file which its address provided by environment variable
   * 'DALV_CONFIG'
   *
   * @throws IOException if the environment variable is empty, does not point to a valid configuration file
   */
  public static void loadFromEnvironmentVariable() throws IOException {
    String dalvConfigFile = System.getenv(DalvConfig.DALV_CONFIG);
    if (dalvConfigFile == null)
      throw new RuntimeException("Dalv config file could not find, please set " +
          DALV_CONFIG + " or provide the config file path as an application argument.");
    loadFromConfig(dalvConfigFile);
  }

  /**
   * Load configuration, form the Dalv configuration file which its address provided by the argument.
   *
   * @param dalvConfigFile the Dalv configuration file address
   * @throws IOException if the config file address is not valid or the file is not readable
   */
  public static void loadFromConfig(String dalvConfigFile) throws IOException {
    Properties props = new Properties();
    props.load(new FileReader(dalvConfigFile));
    for (Object key : props.keySet())
      config.put((String) key, handleBoolean(props.get(key)));
    String nodeId = System.getenv(DalvConfig.DALV_NODE_ID);
    if (nodeId == null)
      nodeId = (String) config.get(NODE_ID);
    if (nodeId == null)
      nodeId = String.valueOf(UUID.randomUUID());
    config.put(NODE_ID, nodeId);
    validateConfigurations();
  }

  private static Object handleBoolean(Object val) {
    if (val instanceof String) {
      if ("true".equalsIgnoreCase((String) val))
        return Boolean.TRUE;
      if ("false".equalsIgnoreCase((String) val))
        return Boolean.FALSE;
    }
    return val;
  }

  private static void validateConfigurations() {
    //Nothing since now
  }

  /**
   * Get a configuration which its value is a String
   *
   * @param key the configuration key
   * @return the configuration value
   */
  public static String getStr(String key) {
    return (String) config.get(key);
  }

  /**
   * Get a configuration which its value is an Integer
   *
   * @param key the configuration key
   * @return the configuration value
   */
  public static Integer getInt(String key) {
    return (Integer) config.get(key);
  }

  /**
   * Get a configuration which its value is a Boolean
   *
   * @param key the configuration key
   * @return the configuration value
   */
  public static Boolean getBoolean(String key) {
    return (Boolean) config.get(key);
  }

  /**
   * Set a configuration. useful to change the configuration in the runtime specially in unit tests
   *
   * @param key the configuration key
   * @param val the configuration value
   */
  public static void setStr(String key, Object val) {
    config.put(key, val);
  }
}
