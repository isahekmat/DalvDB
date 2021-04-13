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
import dalv.common.Common;
import io.grpc.stub.StreamObserver;
import org.dalvdb.backend.watch.WatchEvent;
import org.dalvdb.backend.watch.Watcher;
import org.dalvdb.common.util.ByteUtil;
import org.dalvdb.proto.BackendProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

public class DalvClient {
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

  public boolean put(String userId, String key, byte[] value) {
    return currentConnector.put(userId, key, ByteString.copyFrom(value)).getRepType() == Common.RepType.OK;
  }

  public boolean del(String userId, String key) {
    return currentConnector.del(userId, key).getRepType() == Common.RepType.OK;
  }

  public List<byte[]> getAsList(String userId, String key) {
    BackendProto.GetResponse res = currentConnector.get(userId, key);
    if (res.getRepType() != Common.RepType.OK)
      return null;
    return ByteUtil.decodeList(res.getValue().toByteArray());
  }

  public byte[] get(String userId, String key) {
    List<byte[]> single = getAsList(userId, key);
    if (single == null || single.size() != 1) return null;
    return single.get(0);
  }

  public boolean addToList(String userId, String listKey, byte[] value) {
    return currentConnector.addToList(userId, listKey, ByteString.copyFrom(value)).getRepType() == Common.RepType.OK;
  }

  public boolean removeFromList(String userId, String listKey, byte[] value) {
    return currentConnector.removeFromList(userId, listKey, ByteString.copyFrom(value)).getRepType() == Common.RepType.OK;
  }
}
