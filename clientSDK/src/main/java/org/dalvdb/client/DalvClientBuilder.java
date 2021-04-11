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

package org.dalvdb.client;

import java.util.LinkedList;
import java.util.List;

public class DalvClientBuilder {
  private String[] addressArr;
  private String accessToken;
  private String dataDir;

  public DalvClientBuilder(String serverAddresses, String accessToken, String dataDir) {
    this.accessToken = accessToken;
    this.addressArr = serverAddresses.split(",");
    this.dataDir = dataDir;
  }

  public DalvClientBuilder() {
  }

  public DalvClientBuilder accessToken(String accessToken) {
    this.accessToken = accessToken;
    return this;
  }

  public DalvClientBuilder dataDir(String dataDir) {
    this.dataDir = dataDir;
    return this;
  }

  public DalvClientBuilder serverAddresses(String serverAddresses) {
    this.addressArr = serverAddresses.split(",");
    return this;
  }

  public DalvClient build() {
    List<DalvConnector> connectors = new LinkedList<>();
    for (String address : addressArr)
      connectors.add(new DalvConnector(address, accessToken));
    return new DalvClient(connectors, new Storage(dataDir));
  }

}
