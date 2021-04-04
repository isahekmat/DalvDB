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

package org.dalvdb.it;import org.dalvdb.client.conflict.resolver.AcceptServerResolver;
import org.dalvdb.it.BaseITTest;
import org.junit.Assert;
import org.junit.Test;

public class ConflictIT extends BaseITTest {

  @Test
  public void acceptServerConflictResolver() {
    backend.put("esa", "name", "Esa".getBytes());
    client.put("name", "esa".getBytes());
    Assert.assertEquals(new String(client.get("name")), "esa");
    client.sync(new AcceptServerResolver());
    Assert.assertEquals(new String(client.get("name")), "Esa");
  }

}
