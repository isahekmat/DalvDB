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
import org.dalvdb.client.conflict.resolver.AcceptServerResolver;
import org.dalvdb.proto.ClientProto;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

public class DalvClientTest {

  private DalvClient client;
  private DalvConnector mockConnector;
  private Storage mockStorage;

  @Before
  public void setUp() {
    mockConnector = Mockito.mock(DalvConnector.class);
    mockStorage = Mockito.mock(Storage.class);
    client = new DalvClient(Collections.singletonList(mockConnector), mockStorage);
  }

  @Test
  public void syncWithoutConflictTest() {
    Common.Operation op = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("ali".getBytes()))
        .build();
    ClientProto.SyncResponse response = ClientProto.SyncResponse.newBuilder()
        .setSnapshotId(1)
        .addOps(op)
        .setSyncResponse(Common.RepType.OK)
        .build();

    when(mockConnector.sync(ArgumentMatchers.<Common.Operation>anyList(), anyInt()))
        .thenReturn(response);
    SyncResponse res = client.sync();
    assertThat(res.hasConflict()).isFalse();
    assertThat(res.getResolvedSnapShot()).isEqualTo(1);
    verify(mockStorage).apply(Collections.singletonList(op), 1);
  }

  @Test
  public void syncWithConflictTest() {
    Common.Operation opOnServer = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("ali".getBytes()))
        .build();
    Common.Operation opOnClient = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("esa".getBytes()))
        .build();

    ClientProto.SyncResponse response = ClientProto.SyncResponse.newBuilder()
        .setSnapshotId(1)
        .addOps(opOnServer)
        .setSyncResponse(Common.RepType.NOK)
        .build();
    when(mockConnector.sync(ArgumentMatchers.<Common.Operation>anyList(), anyInt()))
        .thenReturn(response);
    when(mockStorage.getUnsyncOps()).thenReturn(Collections.singletonList(opOnClient));
    SyncResponse res = client.sync();
    assertThat(res.hasConflict()).isTrue();
    assertThat(res.getOperationsWithoutConflict().isEmpty()).isTrue();
    assertThat(res.getConflicts().size()).isEqualTo(1);
    assertThat(res.getConflicts().get(0).getServerOps()).isEqualTo(Collections.singletonList(opOnServer));
    assertThat(res.getConflicts().get(0).getClientOps()).isEqualTo(Collections.singletonList(opOnClient));
    assertThat(res.getConflicts().get(0).getKey()).isEqualTo("name");
    verify(mockStorage, never()).apply(ArgumentMatchers.<Common.Operation>anyList(), anyInt());
  }

  @Test
  public void syncWithSeveralConflictTest() {
    Common.Operation op1OnServer = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("ali".getBytes()))
        .build();
    Common.Operation op2OnServer = Common.Operation.newBuilder()
        .setType(Common.OpType.DEL)
        .setKey("name")
        .build();
    Common.Operation op1OnClient = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("esa".getBytes()))
        .build();
    Common.Operation op2OnClient = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("esa2".getBytes()))
        .build();
    List<Common.Operation> clientOps = new LinkedList<>();
    clientOps.add(op1OnClient);
    clientOps.add(op2OnClient);

    ClientProto.SyncResponse response = ClientProto.SyncResponse.newBuilder()
        .setSnapshotId(1)
        .addOps(op1OnServer)
        .addOps(op2OnServer)
        .setSyncResponse(Common.RepType.NOK)
        .build();
    when(mockConnector.sync(ArgumentMatchers.<Common.Operation>anyList(), anyInt()))
        .thenReturn(response);
    when(mockStorage.getUnsyncOps()).thenReturn(clientOps);
    SyncResponse res = client.sync();
    assertThat(res.hasConflict()).isTrue();
    assertThat(res.getOperationsWithoutConflict().isEmpty()).isTrue();
    assertThat(res.getConflicts().size()).isEqualTo(1);
    assertThat(res.getConflicts().get(0).getServerOps()).isEqualTo(response.getOpsList());
    assertThat(res.getConflicts().get(0).getClientOps()).isEqualTo(clientOps);
    assertThat(res.getConflicts().get(0).getKey()).isEqualTo("name");
    verify(mockStorage, never()).apply(ArgumentMatchers.<Common.Operation>anyList(), anyInt());
  }

  @Test
  public void syncWithConflictAndWithoutConflictTest() {
    Common.Operation op1OnServer = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("ali".getBytes()))
        .build();
    Common.Operation op2OnServer = Common.Operation.newBuilder()
        .setType(Common.OpType.DEL)
        .setKey("age")
        .build();
    Common.Operation op1OnClient = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("esa".getBytes()))
        .build();
    Common.Operation op2OnClient = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("last_name")
        .setVal(ByteString.copyFrom("hekmat".getBytes()))
        .build();
    List<Common.Operation> clientOps = new LinkedList<>();
    clientOps.add(op1OnClient);
    clientOps.add(op2OnClient);

    ClientProto.SyncResponse response = ClientProto.SyncResponse.newBuilder()
        .setSnapshotId(1)
        .addOps(op1OnServer)
        .addOps(op2OnServer)
        .setSyncResponse(Common.RepType.NOK)
        .build();
    when(mockConnector.sync(ArgumentMatchers.<Common.Operation>anyList(), anyInt()))
        .thenReturn(response);
    when(mockStorage.getUnsyncOps()).thenReturn(clientOps);
    SyncResponse res = client.sync();
    assertThat(res.hasConflict()).isTrue();
    assertThat(res.getOperationsWithoutConflict().size()).isEqualTo(2);
    assertThat(res.getOperationsWithoutConflict()).contains(op2OnClient).contains(op2OnServer);
    assertThat(res.getConflicts().size()).isEqualTo(1);
    assertThat(res.getConflicts().get(0).getServerOps()).isEqualTo(Collections.singletonList(op1OnServer));
    assertThat(res.getConflicts().get(0).getClientOps()).isEqualTo(Collections.singletonList(op1OnClient));
    assertThat(res.getConflicts().get(0).getKey()).isEqualTo("name");
    verify(mockStorage, never()).apply(ArgumentMatchers.<Common.Operation>anyList(), anyInt());
  }

  @Test
  public void syncWithResolverTest() {
    Common.Operation op1OnServer = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("ali".getBytes()))
        .build();
    Common.Operation op2OnServer = Common.Operation.newBuilder()
        .setType(Common.OpType.DEL)
        .setKey("age")
        .build();
    Common.Operation op1OnClient = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("esa".getBytes()))
        .build();
    Common.Operation op2OnClient = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("last_name")
        .setVal(ByteString.copyFrom("hekmat".getBytes()))
        .build();
    List<Common.Operation> clientOps = new LinkedList<>();
    clientOps.add(op1OnClient);
    clientOps.add(op2OnClient);

    ClientProto.SyncResponse response = ClientProto.SyncResponse.newBuilder()
        .setSnapshotId(1)
        .addOps(op1OnServer)
        .addOps(op2OnServer)
        .setSyncResponse(Common.RepType.NOK)
        .build();
    when(mockConnector.sync(ArgumentMatchers.<Common.Operation>anyList(), eq(0)))
        .thenReturn(response);
    ClientProto.SyncResponse response2 = ClientProto.SyncResponse.newBuilder()
        .setSnapshotId(2)
        .setSyncResponse(Common.RepType.OK)
        .build();
    when(mockConnector.sync(ArgumentMatchers.<Common.Operation>anyList(), eq(1)))
        .thenReturn(response2);
    List<Common.Operation> resolveOps = new LinkedList<>();
    resolveOps.add(op1OnServer);
    resolveOps.add(op2OnServer);
    resolveOps.add(op2OnClient);
    when(mockStorage.getUnsyncOps()).thenReturn(clientOps, resolveOps);
    when(mockStorage.getLastSnapshotId()).thenReturn(0, 1);
    client.sync(new AcceptServerResolver());

    verify(mockStorage).resolveConflict(1, resolveOps);
    verify(mockConnector, atLeastOnce()).sync(clientOps, 0);
    verify(mockConnector, atLeastOnce()).sync(resolveOps, 1);
  }


  @Test
  public void putTest() {
    Common.Operation op = Common.Operation.newBuilder()
        .setType(Common.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("esa".getBytes()))
        .build();
    client.put("name", "esa".getBytes());
    Mockito.verify(mockStorage).apply(Mockito.eq(op));
  }

  @Test
  public void deleteTest() {
    Common.Operation op = Common.Operation.newBuilder()
        .setType(Common.OpType.DEL)
        .setKey("name")
        .build();
    client.delete("name");
    Mockito.verify(mockStorage).apply(Mockito.eq(op));
  }

  @Test
  public void getTest() {
    client.get("name");
    Mockito.verify(mockStorage).get("name");
  }
}