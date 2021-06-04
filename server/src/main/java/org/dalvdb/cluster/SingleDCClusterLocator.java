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

import java.io.IOException;
import java.util.List;

public class SingleDCClusterLocator implements Locator {
  private static SingleDCClusterLocator instance;

  public synchronized static SingleDCClusterLocator getInstance() {
    if (instance == null) {
      instance = new SingleDCClusterLocator();
    }
    return instance;
  }

  private SingleDCClusterLocator() {
  }

  @Override
  public String locate(String key) {
    return ClusterRing.getInstance().leaderOf(key);
  }

  @Override
  public List<Node> replicas(String key) {
    return ClusterRing.getInstance().replicas(key);
  }

  @Override
  public void close() throws IOException {
    ClusterRing.getInstance().close();
  }
}
