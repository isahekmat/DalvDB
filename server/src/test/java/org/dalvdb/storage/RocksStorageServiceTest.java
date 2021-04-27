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

package org.dalvdb.storage;

import com.google.protobuf.ByteString;
import dalv.common.Common;
import org.dalvdb.DalvConfig;
import org.dalvdb.common.util.OpUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class RocksStorageServiceTest {

  private static RocksStorageService storageService;

  @BeforeClass
  public static void setUp() {
    DalvConfig.set(DalvConfig.DATA_DIR, UUID.randomUUID().toString());
    storageService = new RocksStorageService();
  }

  @After
  public void after(){
      storageService.delete("esa");
  }

  @AfterClass
  public static void tearDown() {
    try {
      storageService.close();
      RocksDB.destroyDB(DalvConfig.getStr(DalvConfig.DATA_DIR), new Options());
    } catch (RocksDBException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void simplePutAndGetTest() {
    Common.Operation op1 = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("Isa".getBytes()))
        .build();
    Common.Operation op2 = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("fname")
        .setVal(ByteString.copyFrom("hekmat".getBytes()))
        .build();
    Common.Operation op3 = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("age")
        .setVal(ByteString.copyFrom(ByteBuffer.allocate(4).putInt(30).array()))
        .build();
    storageService.handleOperations("esa", List.of(op1, op2, op3), 0);
    List<Common.Operation> ops = storageService.get("esa", 0);
    assertThat(ops.size()).isEqualTo(3);
    assertThat(ops.get(0)).isEqualTo(op1);
    assertThat(ops.get(1)).isEqualTo(op2);
    assertThat(ops.get(2)).isEqualTo(op3);
  }


  @Test
  public void snapshotTest() {
    Common.Operation op1 = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("esa".getBytes()))
        .build();
    Common.Operation op2 = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("fname")
        .setVal(ByteString.copyFrom("hekmat".getBytes()))
        .build();
    Common.Operation op3 = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("age")
        .setVal(ByteString.copyFrom(ByteBuffer.allocate(4).putInt(32).array()))
        .build();
    storageService.handleOperations("esa", List.of(op1, op2, op3), 0);
    int snapshotId = storageService.snapshot("esa");
    assertThat(snapshotId).isEqualTo(1);
    List<Common.Operation> ops = storageService.get("esa", snapshotId);
    assertThat(ops).isEmpty();

    Common.Operation op4 = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("age")
        .setVal(ByteString.copyFrom(ByteBuffer.allocate(4).putInt(40).array()))
        .build();
    storageService.handleOperations("esa", Collections.singletonList(op4), snapshotId);
    int snapshotId2 = storageService.snapshot("esa");
    assertThat(snapshotId2).isEqualTo(2);
    List<Common.Operation> ops2 = storageService.get("esa", snapshotId);
    assertThat(ops2.size()).isEqualTo(2);
    assertThat(ops2.get(0)).isEqualTo(op4);
    assertThat(ops2.get(1).getType()).isEqualTo(Common.OpType.SNAPSHOT);
  }

  @Test
  public void conflictTest() {
    Common.Operation op1 = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("esa".getBytes()))
        .build();
    Common.Operation op2 = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("last_name")
        .setVal(ByteString.copyFrom("hekmat".getBytes()))
        .build();
    Common.Operation op3 = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("age")
        .setVal(ByteString.copyFrom(ByteBuffer.allocate(4).putInt(32).array()))
        .build();
    boolean firstWrite = storageService.handleOperations("esa", List.of(op1, op2, op3), 0);
    assertThat(firstWrite).isTrue();

    Common.Operation op4 = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("age")
        .setVal(ByteString.copyFrom(ByteBuffer.allocate(4).putInt(40).array()))
        .build();

    boolean secondWrite = storageService.handleOperations("esa", Collections.singletonList(op4), 0);
    assertThat(secondWrite).isFalse();
    List<Common.Operation> ops2 = storageService.get("esa", 0);
    assertThat(ops2.size()).isEqualTo(3);
    assertThat(ops2.get(2)).isEqualTo(op3);
  }

  @Test
  public void testGetValue() {
    storageService.addOperation("esa", Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("esa".getBytes()))
        .build());
    storageService.addOperation("esa", Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("lname")
        .setVal(ByteString.copyFrom("hekmat".getBytes()))
        .build());
    storageService.addOperation("esa", Common.Operation.newBuilder()
        .setType(Common.OpType.DEL)
        .setKey("lname")
        .build());
    storageService.addOperation("esa", Common.Operation.newBuilder()
        .setType(Common.OpType.SNAPSHOT)
        .setSnapshotId(1)
        .build());
    storageService.addOperation("esa", Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("age")
        .setVal(ByteString.copyFrom(ByteBuffer.allocate(4).putInt(30).array()))
        .build());
    ByteString res = storageService.getValue("esa", "name");
    assertThat(ByteBuffer.wrap(res.toByteArray(), 0, 4).getInt()).isEqualTo(3);
    assertThat(res.substring(4).toString(Charset.defaultCharset())).isEqualTo("esa");
    assertThat(storageService.getValue("esa", "lname").isEmpty()).isTrue();
    assertThat(ByteBuffer.wrap(storageService.getValue("esa", "age").toByteArray(), 0, 4)
        .getInt()).isEqualTo(4);
    assertThat(ByteBuffer.wrap(storageService.getValue("esa", "age").toByteArray(), 4, 4)
        .getInt()).isEqualTo(30);
  }

  @Test
  public void compactSeveralPutTest() {
    storageService.addOperation("esa", Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("names")
        .setVal(ByteString.copyFrom("esa1".getBytes()))
        .build());
    storageService.addOperation("esa", Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("names")
        .setVal(ByteString.copyFrom("esa2".getBytes()))
        .build());
    storageService.addOperation("esa", Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("names")
        .setVal(ByteString.copyFrom("esa3".getBytes()))
        .build());
    storageService.compact("esa");
    List<Common.Operation> ops = storageService.get("esa", 0);
    assertThat(ops.size()).isEqualTo(2);
    assertThat(ops.get(0).getVal()).isEqualTo(ByteString.copyFrom("esa3".getBytes()));
    assertThat(ops.get(1).getType()).isEqualTo(Common.OpType.SNAPSHOT);
    assertThat(ops.get(1).getSnapshotId()).isEqualTo(1);
  }

  @Test
  public void compactSeveralPutAndDelTest() {
    storageService.addOperation("esa", Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("names")
        .setVal(ByteString.copyFrom("esa1".getBytes()))
        .build());
    storageService.addOperation("esa", Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("names")
        .setVal(ByteString.copyFrom("esa2".getBytes()))
        .build());
    storageService.addOperation("esa", Common.Operation.newBuilder()
        .setType(Common.OpType.DEL)
        .setKey("names")
        .build());
    storageService.compact("esa");
    List<Common.Operation> ops = storageService.get("esa", 0);
    assertThat(ops.size()).isEqualTo(1);
    assertThat(ops.get(0).getType()).isEqualTo(Common.OpType.SNAPSHOT);
    assertThat(ops.get(0).getSnapshotId()).isEqualTo(1);
  }

  @Test
  public void compactListTest() {
    storageService.addOperation("esa", Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("names")
        .setVal(ByteString.copyFrom("esa1".getBytes()))
        .build());
    storageService.addOperation("esa", Common.Operation.newBuilder()
        .setType(Common.OpType.ADD_TO_LIST)
        .setKey("names")
        .setVal(ByteString.copyFrom("esa2".getBytes()))
        .build());
    storageService.addOperation("esa", Common.Operation.newBuilder()
        .setType(Common.OpType.REMOVE_FROM_LIST)
        .setKey("names")
        .setVal(ByteString.copyFrom("esa2".getBytes()))
        .build());
    storageService.addOperation("esa", Common.Operation.newBuilder()
        .setType(Common.OpType.REMOVE_FROM_LIST)
        .setKey("names")
        .setVal(ByteString.copyFrom("non-exist".getBytes()))
        .build());
    storageService.addOperation("esa", Common.Operation.newBuilder()
        .setType(Common.OpType.ADD_TO_LIST)
        .setKey("names")
        .setVal(ByteString.copyFrom("esa3".getBytes()))
        .build());
    storageService.compact("esa");
    List<Common.Operation> ops = storageService.get("esa", 0);
    assertThat(ops.size()).isEqualTo(3);
    assertThat(ops.get(0).getType()).isEqualTo(Common.OpType.PUT);
    assertThat(ops.get(0).getVal()).isEqualTo(ByteString.copyFrom("esa1".getBytes()));
    assertThat(ops.get(1).getType()).isEqualTo(Common.OpType.ADD_TO_LIST);
    assertThat(ops.get(1).getVal()).isEqualTo(ByteString.copyFrom("esa3".getBytes()));
    assertThat(ops.get(2).getType()).isEqualTo(Common.OpType.SNAPSHOT);
    assertThat(ops.get(2).getSnapshotId()).isEqualTo(1);
  }

  @Test
  public void compactListWithRemoveFirstTest() {
    storageService.addOperation("esa", Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("names")
        .setVal(ByteString.copyFrom("esa1".getBytes()))
        .build());
    storageService.addOperation("esa", Common.Operation.newBuilder()
        .setType(Common.OpType.ADD_TO_LIST)
        .setKey("names")
        .setVal(ByteString.copyFrom("esa2".getBytes()))
        .build());
    storageService.addOperation("esa", Common.Operation.newBuilder()
        .setType(Common.OpType.REMOVE_FROM_LIST)
        .setKey("names")
        .setVal(ByteString.copyFrom("esa1".getBytes()))
        .build());
    storageService.addOperation("esa", Common.Operation.newBuilder()
        .setType(Common.OpType.REMOVE_FROM_LIST)
        .setKey("names")
        .setVal(ByteString.copyFrom("non-exist".getBytes()))
        .build());
    storageService.addOperation("esa", Common.Operation.newBuilder()
        .setType(Common.OpType.ADD_TO_LIST)
        .setKey("names")
        .setVal(ByteString.copyFrom("esa3".getBytes()))
        .build());
    storageService.compact("esa");
    List<Common.Operation> ops = storageService.get("esa", 0);
    assertThat(ops.size()).isEqualTo(3);
    assertThat(ops.get(0).getType()).isEqualTo(Common.OpType.ADD_TO_LIST);
    assertThat(ops.get(0).getVal()).isEqualTo(ByteString.copyFrom("esa2".getBytes()));
    assertThat(ops.get(1).getType()).isEqualTo(Common.OpType.ADD_TO_LIST);
    assertThat(ops.get(1).getVal()).isEqualTo(ByteString.copyFrom("esa3".getBytes()));
    assertThat(ops.get(2).getType()).isEqualTo(Common.OpType.SNAPSHOT);
    assertThat(ops.get(2).getSnapshotId()).isEqualTo(1);
  }


  @Test
  public void severalSnapshotInTheCompaction(){
    storageService.addOperation("esa", Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("theme")
        .setVal(ByteString.copyFrom("blue".getBytes()))
        .build());
    storageService.snapshot("esa");
    storageService.addOperation("esa", Common.Operation.newBuilder()
        .setType(Common.OpType.DEL)
        .setKey("theme")
        .build());
    storageService.snapshot("esa");
    storageService.compact("esa");
    List<Common.Operation> ops = storageService.get("esa", 1);
    assertThat(ops.size()).isEqualTo(2);
    assertThat(ops.get(0).getType()).isEqualTo(OpUtil.REMOVE_ALL_OP.getType());
    assertThat(ops.get(0).getKey()).isEqualTo(OpUtil.REMOVE_ALL_OP.getKey());
    assertThat(ops.get(1).getType()).isEqualTo(Common.OpType.SNAPSHOT);
    assertThat(ops.get(1).getSnapshotId()).isEqualTo(2);
  }

  @Test
  public void severalSnapshotAndListInTheCompaction(){
    storageService.addOperation("esa", Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("theme")
        .setVal(ByteString.copyFrom("blue".getBytes()))
        .build());
    assertThat(storageService.snapshot("esa")).isEqualTo(1);
    storageService.addOperation("esa", Common.Operation.newBuilder()
        .setType(Common.OpType.ADD_TO_LIST)
        .setKey("theme")
        .setVal(ByteString.copyFrom("red".getBytes()))
        .build());
    assertThat(storageService.snapshot("esa")).isEqualTo(2);
    storageService.addOperation("esa", Common.Operation.newBuilder()
        .setType(Common.OpType.REMOVE_FROM_LIST)
        .setKey("theme")
        .setVal(ByteString.copyFrom("red".getBytes()))
        .build());
    storageService.compact("esa");
    List<Common.Operation> ops = storageService.get("esa", 1);
    assertThat(ops.size()).isEqualTo(3);
    assertThat(ops.get(0).getType()).isEqualTo(OpUtil.REMOVE_ALL_OP.getType());
    assertThat(ops.get(0).getKey()).isEqualTo(OpUtil.REMOVE_ALL_OP.getKey());
    assertThat(ops.get(1).getType()).isEqualTo(Common.OpType.PUT);
    assertThat(ops.get(1).getKey()).isEqualTo("theme");
    assertThat(ops.get(1).getVal()).isEqualTo(ByteString.copyFrom("blue".getBytes()));
    assertThat(ops.get(2).getType()).isEqualTo(Common.OpType.SNAPSHOT);
    assertThat(ops.get(2).getSnapshotId()).isEqualTo(3);
  }
}