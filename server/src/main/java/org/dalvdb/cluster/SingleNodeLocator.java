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

import org.dalvdb.DalvConfig;

public class SingleNodeLocator implements Locator {
  private static SingleNodeLocator instance;

  public static SingleNodeLocator getInstance() {
    if (instance == null) {
      instance = new SingleNodeLocator();
    }
    return instance;
  }

  private SingleNodeLocator() {
  }

  @Override
  public boolean isLocal(String key) {
    return true;
  }

  @Override
  public String locate(String key) {
    return DalvConfig.getStr(DalvConfig.NODE_ID);
  }

  @Override
  public void close() {
  }
}
