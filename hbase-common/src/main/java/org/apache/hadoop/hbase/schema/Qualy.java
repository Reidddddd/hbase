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

import java.util.Comparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceStability.Evolving
@InterfaceAudience.Public
public class Qualy implements Comparable<Qualy>, Comparator<Qualy> {

  private final byte[] qualifier;

  // we ignore this parameter for any comparison and hashcode on purpose
  private ColumnType type;

  public Qualy(String qual) {
    this(Bytes.toBytes(qual));
  }

  public Qualy(byte[] qual) {
    qualifier = qual;
    type = ColumnType.NONE;
  }

  public byte[] getQualifier() {
    return qualifier;
  }

  public void updateType(ColumnType type) {
    this.type = type;
  }

  public ColumnType getType() {
    return type;
  }

  @Override
  public int hashCode() {
    // can't add type for hash calculation
    return Bytes.hashCode(qualifier);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Qualy)) {
      return false;
    }
    return Bytes.equals(qualifier, ((Qualy) other).qualifier);
  }

  @Override
  public int compareTo(Qualy other) {
    if (this == other) {
      return 0;
    }
    return Bytes.compareTo(qualifier, other.qualifier);
  }

  @Override
  public int compare(Qualy o1, Qualy o2) {
    return o1.compareTo(o2);
  }

  @Override
  public String toString() {
    return Bytes.toString(qualifier) + ": " + type.name();
  }

}
