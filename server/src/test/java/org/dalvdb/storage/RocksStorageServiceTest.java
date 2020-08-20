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
import org.dalvdb.DalvConfig;
import org.dalvdb.proto.ClientProto;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class RocksStorageServiceTest {

  private static RocksStorageService storageService;

  @BeforeClass
  public static void setUp() {
    DalvConfig.setStr(DalvConfig.DATA_DIR, UUID.randomUUID().toString());
    storageService = new RocksStorageService();
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
    ClientProto.Operation op1 = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("Isa".getBytes()))
        .build();
    ClientProto.Operation op2 = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.PUT)
        .setKey("fname")
        .setVal(ByteString.copyFrom("hekmat".getBytes()))
        .build();
    ClientProto.Operation op3 = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.PUT)
        .setKey("age")
        .setVal(ByteString.copyFrom(ByteBuffer.allocate(4).putInt(30).array()))
        .build();
    storageService.handleOperations("esa", List.of(op1, op2, op3), 0);
    List<ClientProto.Operation> ops = storageService.get("esa", 0);
    assertThat(ops.size()).isEqualTo(3);
    assertThat(ops.get(0)).isEqualTo(op1);
    assertThat(ops.get(1)).isEqualTo(op2);
    assertThat(ops.get(2)).isEqualTo(op3);
    storageService.delete("esa");
  }


  @Test
  public void snapshotTest() {
    ClientProto.Operation op1 = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("ali".getBytes()))
        .build();
    ClientProto.Operation op2 = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.PUT)
        .setKey("fname")
        .setVal(ByteString.copyFrom("karimin".getBytes()))
        .build();
    ClientProto.Operation op3 = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.PUT)
        .setKey("age")
        .setVal(ByteString.copyFrom(ByteBuffer.allocate(4).putInt(32).array()))
        .build();
    storageService.handleOperations("ali", List.of(op1, op2, op3), 0);
    int snapshotId = storageService.snapshot("ali");
    assertThat(snapshotId).isEqualTo(1);
    List<ClientProto.Operation> ops = storageService.get("ali", snapshotId);
    assertThat(ops).isEmpty();

    ClientProto.Operation op4 = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.PUT)
        .setKey("age")
        .setVal(ByteString.copyFrom(ByteBuffer.allocate(4).putInt(40).array()))
        .build();
    storageService.handleOperations("ali", Collections.singletonList(op4), snapshotId);
    int snapshotId2 = storageService.snapshot("ali");
    assertThat(snapshotId2).isEqualTo(2);
    List<ClientProto.Operation> ops2 = storageService.get("ali", snapshotId);
    assertThat(ops2.size()).isEqualTo(2);
    assertThat(ops2.get(0)).isEqualTo(op4);
    assertThat(ops2.get(1).getType()).isEqualTo(ClientProto.OpType.SNAPSHOT);
    storageService.delete("ali");
  }

  @Test
  public void conflictTest() {
    ClientProto.Operation op1 = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("ali".getBytes()))
        .build();
    ClientProto.Operation op2 = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.PUT)
        .setKey("last_name")
        .setVal(ByteString.copyFrom("karimin".getBytes()))
        .build();
    ClientProto.Operation op3 = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.PUT)
        .setKey("age")
        .setVal(ByteString.copyFrom(ByteBuffer.allocate(4).putInt(32).array()))
        .build();
    boolean firstWrite = storageService.handleOperations("ali", List.of(op1, op2, op3), 0);
    assertThat(firstWrite).isTrue();

    ClientProto.Operation op4 = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.PUT)
        .setKey("age")
        .setVal(ByteString.copyFrom(ByteBuffer.allocate(4).putInt(40).array()))
        .build();

    boolean secondWrite = storageService.handleOperations("ali", Collections.singletonList(op4), 0);
    assertThat(secondWrite).isFalse();
    List<ClientProto.Operation> ops2 = storageService.get("ali", 0);
    assertThat(ops2.size()).isEqualTo(3);
    assertThat(ops2.get(2)).isEqualTo(op3);
    storageService.delete("ali");
  }
}