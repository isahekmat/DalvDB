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

import com.google.common.util.concurrent.ListenableFuture;
import org.dalvdb.proto.ClientProto;

import java.util.concurrent.ExecutionException;

public class DalvClient {
  private final String[] addressArr;
  private int nextAddressIndex = 0;
  private final String accessToken;
  private DalvConnector connector;

  public DalvClient(String serverAddresses, String accessToken) {
    this.accessToken = accessToken;
    addressArr = serverAddresses.split(",");
    createNextConnector();
  }

  private void createNextConnector() {
    connector = new DalvConnector(addressArr[nextAddressIndex++], accessToken);
  }

  public static void main(String[] args) throws InterruptedException, ExecutionException {
    DalvClient client = new DalvClient("localhost:7472",
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOiJlc2EifQ.Gd3BoQIu3tAX2rxlKsgUMJkG38MbDZxoYmKOQfJ9N4g");
    ListenableFuture<ClientProto.SyncResponse> res = client.connector.sync(0);
    ClientProto.SyncResponse response = res.get();
    System.out.println("Done");
    System.out.println(response);
  }

}
