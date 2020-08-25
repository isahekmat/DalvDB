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

import com.google.protobuf.ByteString;
import org.dalvdb.proto.ClientProto;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class StorageTest {

  private Storage storage;
  private String dataDir;

  @Before
  public void setUp() {
    dataDir = UUID.randomUUID().toString();
    storage = new Storage(dataDir);
  }

  @After
  public void tearDown() throws Exception {
    storage.close();
    RocksDB.destroyDB(dataDir, new Options());
  }

  @Test
  public void getLastSnapshotIdTest() {
    assertThat(storage.getLastSnapshotId()).isEqualTo(0);
    storage.apply(Collections.<ClientProto.Operation>emptyList(), 3);
    assertThat(storage.getLastSnapshotId()).isEqualTo(3);

  }

  @Test
  public void applyTest() {
    ClientProto.Operation op1 = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("esa", Charset.defaultCharset()))
        .build();
    storage.apply(Collections.singletonList(op1), 1);
    assertThat(storage.getLastSnapshotId()).isEqualTo(1);
    assertThat(new String(storage.get("name"))).isEqualTo("esa");
  }

  @Test
  public void resolveConflictTest() {
    ClientProto.Operation op1 = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("esa", Charset.defaultCharset()))
        .build();

    storage.put(op1);

    ClientProto.Operation op2 = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("ali", Charset.defaultCharset()))
        .build();

    storage.resolveConflict(1, Collections.singletonList(op2));

    assertThat(storage.getLastSnapshotId()).isEqualTo(1);
    assertThat(storage.getUnsyncOps().size()).isEqualTo(1);
    assertThat(storage.getUnsyncOps().get(0)).isEqualTo(op2);
    assertThat(new String(storage.get("name"))).isEqualTo("ali");
  }
}