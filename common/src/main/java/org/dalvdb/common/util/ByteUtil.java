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

package org.dalvdb.common.util;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import dalv.common.Common;

import java.nio.ByteBuffer;
import java.util.*;

public class ByteUtil {

  public static LinkedList<Common.Operation> byteToOps(byte[] bytes) throws InvalidProtocolBufferException {
    LinkedList<Common.Operation> ops = new LinkedList<>();
    if (bytes == null) return ops;
    int offset = bytes.length;
    while (offset > 0) {
      int len = ByteBuffer.wrap(bytes, offset - 4, 4).getInt();
      offset -= len + 4;
      Common.Operation op = Common.Operation.parseFrom(ByteString.copyFrom(bytes, offset, len));
      ops.addFirst(op);
      offset--;
    }
    return ops;
  }

  public static byte[] opToByte(Common.Operation op) {
    byte[] oprBytes = op.toByteArray();
    byte[] result = new byte[oprBytes.length + 4];
    byte[] lenArr = ByteBuffer.allocate(4).putInt(oprBytes.length).array();
    System.arraycopy(oprBytes, 0, result, 0, oprBytes.length);
    System.arraycopy(lenArr, 0, result, oprBytes.length, 4);
    return result;
  }

  private static List<byte[]> decodeList(byte[] bytes, boolean hasSeparator) {
    if (bytes == null || bytes.length == 0) return null;
    List<byte[]> list = new ArrayList<>();
    int currentIndex = 0;
    while (currentIndex < bytes.length) {
      int len = ByteBuffer.wrap(bytes, currentIndex, 4).getInt();
      currentIndex += 4;
      list.add(Arrays.copyOfRange(bytes, currentIndex, currentIndex + len));
      currentIndex += len;
      if (hasSeparator) currentIndex++;
    }
    return list;
  }

  public static List<byte[]> decodeList(byte[] bytes) {
    return decodeList(bytes, false);
  }

  public static List<byte[]> decodeListWithSeparator(byte[] bytes) {
    return decodeList(bytes, true);
  }

  public static Iterator<Common.Operation> getReverseIterator(byte[] recordsBytes) {
    return new OperatorsReverseIterator(recordsBytes);
  }

  public static class OperatorsReverseIterator implements Iterator<Common.Operation> {
    private final byte[] recordsBytes;
    private int currentIndex;

    public OperatorsReverseIterator(byte[] recordsBytes) {
      this.recordsBytes = recordsBytes;
      this.currentIndex = recordsBytes == null ? 0 : recordsBytes.length;
    }

    @Override
    public boolean hasNext() {
      return currentIndex > 0;
    }

    @Override
    public Common.Operation next() {
      int len = ByteBuffer.wrap(recordsBytes, currentIndex - 4, 4).getInt();
      try {
        Common.Operation op =
            Common.Operation.parseFrom(ByteString.copyFrom(recordsBytes, currentIndex - (4 + len), len));
        currentIndex -= len + 5;
        return op;
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public void remove() {
      throw new IllegalStateException("Operation not supported");
    }
  }
}
