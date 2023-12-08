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

/**
 * Part of Schema Service. A column is comprised of a family and a qualifier.
 * <p>
 * Any change of this object on client side has no effect on server side but updating column type.
 * Need to call as following in order to make it take effect:
 * <pre>
 *   Schema schema = admin.getSchemaOf(TableName);
 *   Column column = schema.getColumn(Famy, Qualy);
 *   column.updateType(ColumnType.ANY_TYPE);
 *   admin.publishSchema(Schema);
 *   ...
 * </pre>
 */
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
    byte[] column = new byte[famy.getLength() + qualy.getLength()];
    System.arraycopy(famy.getBytes(), famy.getOffset(), column, 0, famy.getLength());
    System.arraycopy(qualy.getBytes(), qualy.getOffset(), column, famy.getLength(),
      qualy.getLength());
    hashCode = Bytes.hashCode(column);
  }

  public Famy getFamy() {
    return family;
  }

  public Qualy getQualy() {
    return qualifier;
  }

  public void updateType(ColumnType type) {
    qualifier.updateType(type);
  }

  public ColumnType getType() {
    return qualifier.getType();
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
    return family.equals(((Column) other).family) &&
           qualifier.equals(((Column) other).qualifier);
  }

  @Override
  public int compareTo(Column other) {
    if (this == other) {
      return 0;
    }
    int res = family.compareTo(other.getFamy());
    if (res == 0) {
      return qualifier.compareTo(other.getQualy());
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
    sb.append("column: {family: ").append(family.toString()).append(", ")
      .append("qualifier: ").append(qualifier.toString()).append(", ")
      .append("type: ").append(qualifier.getType().name())
      .append("}");
    return sb.toString();
  }

}
