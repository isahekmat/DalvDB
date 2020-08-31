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
import org.dalvdb.proto.ClientProto;

import java.nio.ByteBuffer;
import java.util.LinkedList;

public class ByteUtil {

  public static LinkedList<Common.Operation> byteToOps(byte[] bytes) throws InvalidProtocolBufferException {
    LinkedList<Common.Operation> ops = new LinkedList<>();
    int offset = 0;
    if (bytes == null) return ops;
    while (offset < bytes.length) {
      int len = ByteBuffer.wrap(bytes, offset, 4).getInt();
      offset += 5;
      Common.Operation op = Common.Operation.parseFrom(ByteString.copyFrom(bytes, offset, len));
      offset += len;
      ops.add(op);
      offset++;
    }
    return ops;
  }

  public static byte[] opToByte(Common.Operation op) {
    byte[] oprBytes = op.toByteArray();
    byte[] result = new byte[5 + oprBytes.length];
    System.arraycopy(ByteBuffer.allocate(4).putInt(oprBytes.length).array(), 0, result, 0, 4);
    result[4] = (char) 0;
    System.arraycopy(oprBytes, 0, result, 5, oprBytes.length);
    return result;
  }
}
