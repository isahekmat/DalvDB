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

import dalv.common.Common;

import java.util.concurrent.CompletableFuture;

public class LocalRouter implements Router {
  private static LocalRouter instance;

  public synchronized static LocalRouter getInstance() {
    if (instance == null) {
      instance = new LocalRouter();
    }
    return instance;
  }

  private LocalRouter() {
  }

  @Override
  public CompletableFuture<Void> propagate(String userId, Common.Operation op) {
    CompletableFuture<Void> ret = new CompletableFuture<>();
    ret.complete(null);
    return ret;
  }

  @Override
  public void commit(String userId) {
  }
}
