/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.schema;

import java.util.HashMap;
import java.util.Map;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Type for a column, it is used by Schema Service.
 * By default, it is None for a column.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum ColumnType {

  // just use 1 byte to store it, instead of using Bytes.toByte(Int), which consumes 4 bytes
  NONE(new byte[] { (byte) 0 }),
  BYTE(new byte[] { (byte) 1 }),
  BOOL(new byte[] { (byte) 2 }),
  CHAR(new byte[] { (byte) 3 }),
  SHORT(new byte[] { (byte) 4 }),
  INT(new byte[] { (byte) 5 }),
  LONG(new byte[] { (byte) 6 }),
  FLOAT(new byte[] { (byte) 7 }),
  DOUBLE(new byte[] { (byte) 8 }),
  STRING(new byte[] { (byte) 9 }),
  OTHER(new byte[] { (byte) 127})
  ;

  static {
    Map<Integer, ColumnType> map = new HashMap<>();
    for (ColumnType type : ColumnType.values()) {
      map.put((int) type.code[0], type);
    }
    FAST_MAP = map;
  }

  private final byte[] code;
  private static final Map<Integer, ColumnType> FAST_MAP;

  ColumnType(byte[] code) {
    this.code = code;
  }

  public byte[] getCode() {
    return code;
  }

  public static ColumnType parseType(int code) {
    return FAST_MAP.get(code);
  }

  public static ColumnType parseType(byte[] code) {
    return parseType(code[0]);
  }

}
