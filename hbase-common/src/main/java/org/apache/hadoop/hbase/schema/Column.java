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
public class Column implements Comparable<Column>, Comparator<Column> {

  private final Famy family;
  private final Qualy qualifier;
  private final int hashCode;

  public Column(Famy famy, Qualy qualy) {
    family = famy;
    qualifier = qualy;

    // calculate hashcode at the beginning, to avoid frequent copying
    byte[] f = famy.getFamily();
    byte[] q = qualy.getQualifier();
    byte[] column = new byte[f.length + q.length];
    System.arraycopy(f, 0, column, 0, f.length);
    System.arraycopy(q, 0, column, f.length, q.length);
    hashCode = Bytes.hashCode(column);
  }

  public Famy getFamy() {
    return family;
  }

  public Qualy getQualy() {
    return qualifier;
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Column)) {
      return false;
    }
    Column column = (Column) other;
    return Bytes.equals(family.getFamily(), column.getFamy().getFamily()) &&
           Bytes.equals(qualifier.getQualifier(), column.getQualy().getQualifier());
  }

  @Override
  public int compareTo(Column other) {
    if (this == other) {
      return 0;
    }
    int res = Bytes.compareTo(family.getFamily(), other.getFamy().getFamily());
    if (res == 0) {
      return Bytes.compareTo(qualifier.getQualifier(), other.getQualy().getQualifier());
    }
    return res;
  }

  @Override
  public int compare(Column o1, Column o2) {
    return o1.compareTo(o2);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("column: {family: ").append(Bytes.toString(family.getFamily())).append(", ")
      .append("qualifier: ").append(Bytes.toString(qualifier.getQualifier())).append("}");
    return sb.toString();
  }

}
