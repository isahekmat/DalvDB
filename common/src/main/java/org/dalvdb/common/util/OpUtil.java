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
import java.nio.charset.Charset;
import java.util.Iterator;

public class OpUtil {
  public final static Common.Operation REMOVE_ALL_OP = Common.Operation.newBuilder()
      .setType(Common.OpType.DEL)
      .setVal(ByteString.copyFrom(".all", Charset.defaultCharset()))
      .build();

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
