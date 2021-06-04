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

package org.dalvdb.cluster;

import java.util.Objects;

public class Node implements Comparable<Node> {
  private final String host;
  private final int port;
  private final int ringPosition;

  public Node(String host, int port, int ringPosition) {
    this.host = host;
    this.port = port;
    this.ringPosition = ringPosition;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public int getRingPosition() {
    return ringPosition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Node node = (Node) o;
    return port == node.port &&
        host.equals(node.host);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port);
  }

  @Override
  public int compareTo(Node o) {
    return this.ringPosition - o.ringPosition;
  }
}
