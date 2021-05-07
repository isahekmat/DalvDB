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

import java.io.Closeable;

public interface Locator extends Closeable {
  boolean singleton = DalvConfig.getBoolean(DalvConfig.SINGLETON_MODE);

  static Locator getInstance() {
    if (singleton)
      return SingleNodeLocator.getInstance();
    else
      return SingleDCClusterLocator.getInstance();
  }

  /**
   * determine if the key is related to the current server or not
   *
   * @param key the user id
   * @return is the key related to the current server or not
   */
  boolean isLocal(String key);

  /**
   * Return the address of server which is responsible for handling this key
   *
   * @param key the user id
   * @return address of server responsible for handling this key
   */
  String locate(String key);
}
