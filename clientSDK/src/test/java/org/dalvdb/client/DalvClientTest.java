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
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
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
    ClientProto.Operation op = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("ali".getBytes()))
        .build();
    ClientProto.SyncResponse response = ClientProto.SyncResponse.newBuilder()
        .setSnapshotId(1)
        .addOps(op)
        .setSyncResponse(ClientProto.RepType.OK)
        .build();

    when(mockConnector.sync(ArgumentMatchers.<ClientProto.Operation>anyList(), anyInt()))
        .thenReturn(response);
    SyncResponse res = client.sync();
    assertThat(res.hasConflict()).isFalse();
    assertThat(res.getResolvedSnapShot()).isEqualTo(1);
    verify(mockStorage).apply(Collections.singletonList(op), 1);
  }

  @Test
  public void syncWithConflictTest() {
    ClientProto.Operation opOnServer = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("ali".getBytes()))
        .build();
    ClientProto.Operation opOnClient = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("esa".getBytes()))
        .build();

    ClientProto.SyncResponse response = ClientProto.SyncResponse.newBuilder()
        .setSnapshotId(1)
        .addOps(opOnServer)
        .setSyncResponse(ClientProto.RepType.NOK)
        .build();
    when(mockConnector.sync(ArgumentMatchers.<ClientProto.Operation>anyList(), anyInt()))
        .thenReturn(response);
    when(mockStorage.getUnsyncOps()).thenReturn(Collections.singletonList(opOnClient));
    SyncResponse res = client.sync();
    assertThat(res.hasConflict()).isTrue();
    assertThat(res.getOperationsWithoutConflict().isEmpty()).isTrue();
    assertThat(res.getConflicts().size()).isEqualTo(1);
    assertThat(res.getConflicts().get(0).getServerOps()).isEqualTo(Collections.singletonList(opOnServer));
    assertThat(res.getConflicts().get(0).getClientOps()).isEqualTo(Collections.singletonList(opOnClient));
    assertThat(res.getConflicts().get(0).getKey()).isEqualTo("name");
    verify(mockStorage,never()).apply(ArgumentMatchers.<ClientProto.Operation>anyList(),anyInt());
  }

  @Test
  public void syncWithSeveralConflictTest() {
    ClientProto.Operation op1OnServer = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("ali".getBytes()))
        .build();
    ClientProto.Operation op2OnServer = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.DEL)
        .setKey("name")
        .build();
    ClientProto.Operation op1OnClient = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("esa".getBytes()))
        .build();
    ClientProto.Operation op2OnClient = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("esa2".getBytes()))
        .build();
    List<ClientProto.Operation> clientOps = new LinkedList<>();
    clientOps.add(op1OnClient);
    clientOps.add(op2OnClient);

    ClientProto.SyncResponse response = ClientProto.SyncResponse.newBuilder()
        .setSnapshotId(1)
        .addOps(op1OnServer)
        .addOps(op2OnServer)
        .setSyncResponse(ClientProto.RepType.NOK)
        .build();
    when(mockConnector.sync(ArgumentMatchers.<ClientProto.Operation>anyList(), anyInt()))
        .thenReturn(response);
    when(mockStorage.getUnsyncOps()).thenReturn(clientOps);
    SyncResponse res = client.sync();
    assertThat(res.hasConflict()).isTrue();
    assertThat(res.getOperationsWithoutConflict().isEmpty()).isTrue();
    assertThat(res.getConflicts().size()).isEqualTo(1);
    assertThat(res.getConflicts().get(0).getServerOps()).isEqualTo(response.getOpsList());
    assertThat(res.getConflicts().get(0).getClientOps()).isEqualTo(clientOps);
    assertThat(res.getConflicts().get(0).getKey()).isEqualTo("name");
    verify(mockStorage,never()).apply(ArgumentMatchers.<ClientProto.Operation>anyList(),anyInt());
  }

  @Test
  public void syncWithConflictAndWithoutConflictTest() {
    ClientProto.Operation op1OnServer = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("ali".getBytes()))
        .build();
    ClientProto.Operation op2OnServer = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.DEL)
        .setKey("age")
        .build();
    ClientProto.Operation op1OnClient = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("esa".getBytes()))
        .build();
    ClientProto.Operation op2OnClient = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.PUT)
        .setKey("last_name")
        .setVal(ByteString.copyFrom("hekmat".getBytes()))
        .build();
    List<ClientProto.Operation> clientOps = new LinkedList<>();
    clientOps.add(op1OnClient);
    clientOps.add(op2OnClient);

    ClientProto.SyncResponse response = ClientProto.SyncResponse.newBuilder()
        .setSnapshotId(1)
        .addOps(op1OnServer)
        .addOps(op2OnServer)
        .setSyncResponse(ClientProto.RepType.NOK)
        .build();
    when(mockConnector.sync(ArgumentMatchers.<ClientProto.Operation>anyList(), anyInt()))
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
    verify(mockStorage,never()).apply(ArgumentMatchers.<ClientProto.Operation>anyList(),anyInt());
  }


  @Test
  public void putTest() {
    ClientProto.Operation op = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.PUT)
        .setKey("name")
        .setVal(ByteString.copyFrom("esa".getBytes()))
        .build();
    client.put("name", "esa".getBytes());
    Mockito.verify(mockStorage).put(Mockito.eq(op));
  }

  @Test
  public void deleteTest() {
    ClientProto.Operation op = ClientProto.Operation.newBuilder()
        .setType(ClientProto.OpType.DEL)
        .setKey("name")
        .build();
    client.delete("name");
    Mockito.verify(mockStorage).put(Mockito.eq(op));
  }

  @Test
  public void getTest() {
    client.get("name");
    Mockito.verify(mockStorage).get("name");
  }
}