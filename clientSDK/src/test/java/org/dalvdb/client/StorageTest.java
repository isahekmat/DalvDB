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
import dalv.common.Common;
import org.dalvdb.common.util.ByteUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
    storage.apply(Collections.<Common.Operation>emptyList(), 3);
    assertThat(storage.getLastSnapshotId()).isEqualTo(3);

  }

  @Test
  public void applyTest() {
    Common.Operation op1 = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("esa", Charset.defaultCharset()))
        .build();
    storage.apply(Collections.singletonList(op1), 1);
    assertThat(storage.getLastSnapshotId()).isEqualTo(1);
    assertThat(ByteBuffer.wrap(storage.get("name"), 0, 4).getInt()).isEqualTo(3);
    assertThat(ByteString.copyFrom(storage.get("name"), 4, 3).toString(Charset.defaultCharset()))
        .isEqualTo("esa");
  }

  @Test
  public void resolveConflictTest() {
    Common.Operation op1 = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("esa", Charset.defaultCharset()))
        .build();

    storage.apply(op1);

    Common.Operation op2 = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("ali", Charset.defaultCharset()))
        .build();

    storage.resolveConflict(1, Collections.singletonList(op2));

    assertThat(storage.getLastSnapshotId()).isEqualTo(1);
    assertThat(storage.getUnsyncOps().size()).isEqualTo(1);
    assertThat(storage.getUnsyncOps().get(0)).isEqualTo(op2);

    assertThat(ByteBuffer.wrap(storage.get("name"), 0, 4).getInt()).isEqualTo(3);
    assertThat(ByteString.copyFrom(storage.get("name"), 4, 3).toString(Charset.defaultCharset()))
        .isEqualTo("ali");
  }

  @Test
  public void listRemoveLastTest(){
    Common.Operation op1 = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("esa", Charset.defaultCharset()))
        .build();
    Common.Operation op2 = Common.Operation.newBuilder()
        .setType(Common.OpType.ADD_TO_LIST)
        .setKey("name")
        .setVal(ByteString.copyFrom("isa", Charset.defaultCharset()))
        .build();
    Common.Operation op3 = Common.Operation.newBuilder()
        .setType(Common.OpType.ADD_TO_LIST)
        .setKey("name")
        .setVal(ByteString.copyFrom("issa", Charset.defaultCharset()))
        .build();
    Common.Operation op4 = Common.Operation.newBuilder()
        .setType(Common.OpType.REMOVE_FROM_LIST)
        .setKey("name")
        .setVal(ByteString.copyFrom("issa", Charset.defaultCharset()))
        .build();
    storage.apply(op1);
    storage.apply(op2);
    storage.apply(op3);
    storage.apply(op4);

    List<byte[]> names = ByteUtil.decodeListWithSeparator(storage.get("name"));
    assertThat(names.size()).isEqualTo(2);
    assertThat(
        Arrays.equals(names.get(0),"esa".getBytes()) ||
        Arrays.equals(names.get(1),"esa".getBytes())
    ).isTrue();
    assertThat(
        Arrays.equals(names.get(0),"isa".getBytes()) ||
            Arrays.equals(names.get(1),"isa".getBytes())
    ).isTrue();
  }
  @Test
  public void listRemoveFirstTest() {
    Common.Operation op1 = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("esa", Charset.defaultCharset()))
        .build();
    Common.Operation op2 = Common.Operation.newBuilder()
        .setType(Common.OpType.ADD_TO_LIST)
        .setKey("name")
        .setVal(ByteString.copyFrom("isa", Charset.defaultCharset()))
        .build();
    Common.Operation op3 = Common.Operation.newBuilder()
        .setType(Common.OpType.ADD_TO_LIST)
        .setKey("name")
        .setVal(ByteString.copyFrom("issa", Charset.defaultCharset()))
        .build();
    Common.Operation op4 = Common.Operation.newBuilder()
        .setType(Common.OpType.REMOVE_FROM_LIST)
        .setKey("name")
        .setVal(ByteString.copyFrom("esa", Charset.defaultCharset()))
        .build();
    storage.apply(op1);
    storage.apply(op2);
    storage.apply(op3);
    storage.apply(op4);

    List<byte[]> names = ByteUtil.decodeListWithSeparator(storage.get("name"));
    assertThat(names.size()).isEqualTo(2);
    assertThat(
        Arrays.equals(names.get(0),"issa".getBytes()) ||
            Arrays.equals(names.get(1),"issa".getBytes())
    ).isTrue();
    assertThat(
        Arrays.equals(names.get(0),"isa".getBytes()) ||
            Arrays.equals(names.get(1),"isa".getBytes())
    ).isTrue();
  }

  @Test
  public void listRemoveMiddleTest(){
    Common.Operation op1 = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("esa", Charset.defaultCharset()))
        .build();
    Common.Operation op2 = Common.Operation.newBuilder()
        .setType(Common.OpType.ADD_TO_LIST)
        .setKey("name")
        .setVal(ByteString.copyFrom("isa", Charset.defaultCharset()))
        .build();
    Common.Operation op3 = Common.Operation.newBuilder()
        .setType(Common.OpType.ADD_TO_LIST)
        .setKey("name")
        .setVal(ByteString.copyFrom("issa", Charset.defaultCharset()))
        .build();
    Common.Operation op4 = Common.Operation.newBuilder()
        .setType(Common.OpType.REMOVE_FROM_LIST)
        .setKey("name")
        .setVal(ByteString.copyFrom("isa", Charset.defaultCharset()))
        .build();
    storage.apply(op1);
    storage.apply(op2);
    storage.apply(op3);
    storage.apply(op4);

    List<byte[]> names = ByteUtil.decodeListWithSeparator(storage.get("name"));
    assertThat(names.size()).isEqualTo(2);
    assertThat(
        Arrays.equals(names.get(0),"esa".getBytes()) ||
            Arrays.equals(names.get(1),"esa".getBytes())
    ).isTrue();
    assertThat(
        Arrays.equals(names.get(0),"issa".getBytes()) ||
            Arrays.equals(names.get(1),"issa".getBytes())
    ).isTrue();
  }
}