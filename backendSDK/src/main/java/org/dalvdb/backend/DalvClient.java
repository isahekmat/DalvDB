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

package org.dalvdb.backend;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.dalvdb.common.util.ByteUtil;
import org.dalvdb.common.watch.WatchEvent;
import org.dalvdb.common.watch.Watcher;
import org.dalvdb.proto.BackendProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class DalvClient implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(DalvClient.class);
  private final List<DalvConnector> connectors;

  private DalvConnector currentConnector;
  private int currentConnectorIndex;

  public DalvClient(String[] addressArr) {
    List<DalvConnector> connectors = new LinkedList<>();
    for (String address : addressArr)
      connectors.add(new DalvConnector(address));
    if (connectors.size() < 1)
      throw new IllegalArgumentException("At least one connector should be existed");
    this.connectors = connectors;
    this.currentConnectorIndex = 0;
    this.currentConnector = connectors.get(0);
  }

  public void cancelWatch(String key) {
    currentConnector.cancelWatch(key);
  }

  public void cancelAllWatch() {
    currentConnector.cancelAllWatch();
  }

  public void watch(String key, Watcher watcher) {
    currentConnector.watch(key, new StreamObserver<BackendProto.WatchResponse>() {
      @Override
      public void onNext(BackendProto.WatchResponse value) {
        WatchEvent event = new WatchEvent()
            .setUserId(value.getUserId())
            .setNewValue(value.getOperation().getVal().toByteArray())
            .setOperationType(value.getOperation().getType())
            .setKey(key);
        watcher.process(event);
      }

      @Override
      public void onError(Throwable t) {
        logger.error("error occurred while processing watch event over key {}", key, t);
      }

      @Override
      public void onCompleted() {
        logger.info("watch stream closed for key {}", key);
      }
    });
  }

  public void put(String userId, String key, byte[] value) {
    currentConnector.put(userId, key, ByteString.copyFrom(value));
  }

  public void del(String userId, String key) {
    currentConnector.del(userId, key);
  }

  public List<byte[]> getAsList(String userId, String key) {
    BackendProto.GetResponse res = currentConnector.get(userId, key);
    return ByteUtil.decodeList(res.getValue().toByteArray());
  }

  public byte[] get(String userId, String key) {
    List<byte[]> single = getAsList(userId, key);
    if (single == null || single.size() != 1) return null;
    return single.get(0);
  }

  public void addToList(String userId, String listKey, byte[] value) {
    currentConnector.addToList(userId, listKey, ByteString.copyFrom(value));
  }

  public void removeFromList(String userId, String listKey, byte[] value) {
    currentConnector.removeFromList(userId, listKey, ByteString.copyFrom(value));
  }

  @Override
  public void close() throws IOException {
    cancelAllWatch();
    for (DalvConnector connector : this.connectors) {
      connector.close();
    }
  }
}
