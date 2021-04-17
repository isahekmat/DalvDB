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

package org.dalvdb.watch;

import com.google.protobuf.ByteString;
import dalv.common.Common;
import io.grpc.stub.StreamObserver;
import org.dalvdb.proto.BackendProto;
import org.dalvdb.proto.ClientProto;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class InMemoryWatchManagerTest {

  @Test
  public void simpleBackendWatchTest() throws InterruptedException {
    WatchManager wm = new InMemoryWatchManager();
    MockStreamObserver<BackendProto.WatchResponse> mockBack = new MockStreamObserver<>();
    wm.addBackendWatch("testKey", mockBack);
    wm.notifyChange("someUser", Common.Operation.newBuilder()
        .setKey("testKey")
        .setType(Common.OpType.PUT)
        .setVal(ByteString.EMPTY)
        .build());
    mockBack.waitUntilChange();
    assertThat(mockBack.onNextCall.get()).isEqualTo(1);
    assertThat(mockBack.onErrorCall.get()).isEqualTo(0);
    assertThat(mockBack.onCompleteCall.get()).isEqualTo(0);
  }

  @Test
  public void simpleClientWatchTest() throws InterruptedException {
    WatchManager wm = new InMemoryWatchManager();
    MockStreamObserver<ClientProto.WatchResponse> mockClient = new MockStreamObserver<>();
    wm.addClientWatch("someUser", "testKey", mockClient);
    wm.notifyChange("someUser", Common.Operation.newBuilder()
        .setKey("testKey")
        .setType(Common.OpType.PUT)
        .setVal(ByteString.EMPTY)
        .build());
    mockClient.waitUntilChange();
    assertThat(mockClient.onNextCall.get()).isEqualTo(1);
    assertThat(mockClient.onErrorCall.get()).isEqualTo(0);
    assertThat(mockClient.onCompleteCall.get()).isEqualTo(0);
  }

  @Test
  public void anotherClientWatchTest() throws InterruptedException {
    WatchManager wm = new InMemoryWatchManager();
    MockStreamObserver<ClientProto.WatchResponse> mockClient = new MockStreamObserver<>();
    wm.addClientWatch("someUser", "testKey", mockClient);
    wm.notifyChange("anotherUser", Common.Operation.newBuilder()
        .setKey("testKey")
        .setType(Common.OpType.PUT)
        .setVal(ByteString.EMPTY)
        .build());
    mockClient.waitUntilChange();
    assertThat(mockClient.onNextCall.get()).isEqualTo(0);
    assertThat(mockClient.onErrorCall.get()).isEqualTo(0);
    assertThat(mockClient.onCompleteCall.get()).isEqualTo(0);
  }

  @Test
  public void closeTest() throws InterruptedException {
    WatchManager wm = new InMemoryWatchManager();
    MockStreamObserver<ClientProto.WatchResponse> mockClient = new MockStreamObserver<>();
    MockStreamObserver<BackendProto.WatchResponse> mockBack = new MockStreamObserver<>();
    wm.addClientWatch("someUser", "testKey", mockClient);
    wm.addBackendWatch("testKey", mockBack);
    wm.close();
    mockClient.waitUntilChange();

    assertThat(mockClient.onNextCall.get()).isEqualTo(0);
    assertThat(mockClient.onErrorCall.get()).isEqualTo(0);
    assertThat(mockClient.onCompleteCall.get()).isEqualTo(1);

    assertThat(mockBack.onNextCall.get()).isEqualTo(0);
    assertThat(mockBack.onErrorCall.get()).isEqualTo(0);
    assertThat(mockBack.onCompleteCall.get()).isEqualTo(1);
  }

  private static class MockStreamObserver<T> implements StreamObserver<T> {
    final AtomicInteger onNextCall = new AtomicInteger();
    final AtomicInteger onErrorCall = new AtomicInteger();
    final AtomicInteger onCompleteCall = new AtomicInteger();
    final Object monitor = new Object();

    @Override
    public void onNext(T value) {
      onNextCall.incrementAndGet();
      synchronized (monitor) {
        monitor.notify();
      }
    }

    @Override
    public void onError(Throwable t) {
      onErrorCall.incrementAndGet();
      synchronized (monitor) {
        monitor.notify();
      }
    }

    @Override
    public void onCompleted() {
      onCompleteCall.incrementAndGet();
      synchronized (monitor) {
        monitor.notify();
      }
    }

    private void waitUntilChange() throws InterruptedException {
      synchronized (monitor) {
        monitor.wait(20);
      }
    }
  }

}
