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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceStability.Evolving
@InterfaceAudience.Public
public class Schema implements Iterable<byte[]> {

  private final Map<Famy, Set<Qualy>> columns = new ConcurrentHashMap<>();

  private final TableName table;

  public Schema(TableName table) {
    this.table = table;
  }

  public boolean containFamily(byte[] family) {
    return containFamily(new Famy(family));
  }

  public boolean containFamily(Famy family) {
    return columns.containsKey(family);
  }

  public boolean containColumn(byte[] family, byte[] qualifier) {
    return containColumn(new Famy(family), new Qualy(qualifier));
  }

  public boolean containColumn(Famy family, Qualy qualifier) {
    if (!containFamily(family)) {
      return false;
    }
    return columns.get(family).contains(qualifier);
  }

  public boolean addFamily(byte[] family) {
    return addFamily(new Famy(family));
  }

  public boolean addFamily(Famy family) {
    return columns.putIfAbsent(family, new ConcurrentSkipListSet<>()) == null;
  }

  public boolean addColumn(byte[] family, byte[] qualifier) {
    return addColumn(new Famy(family), new Qualy(qualifier));
  }

  public boolean addColumn(Famy family, Qualy qualifier) {
    addFamily(family);
    return columns.get(family).add(qualifier);
  }

  public void clear() {
    columns.clear();
  }

  @Override
  public Iterator<byte[]> iterator() {
    return new SchemaIterator(table, columns.values());
  }

  private class SchemaIterator implements Iterator<byte[]> {

    private final List<Qualy> qualifiers;
    private final byte[] table;
    private int index = 0;
    private boolean useTableNameAsRow = false;

    public SchemaIterator(TableName table, Collection<Set<Qualy>> values) {
      this.table = table.getName();

      Set<Qualy> set = new HashSet<>();
      for (Set<Qualy> value : values) {
        for (Qualy qualy : value) {
          set.add(qualy);
        }
      }
      this.qualifiers = new ArrayList<>(set);
      Collections.sort(qualifiers);
    }

    @Override
    public boolean hasNext() {
      return index != qualifiers.size();
    }

    @Override
    public byte[] next() {
      if (!useTableNameAsRow) {
        useTableNameAsRow = true;
        return table;
      }
      byte[] qualy = qualifiers.get(index++).getQualifier();
      byte[] rowkey = new byte[table.length + qualy.length];
      System.arraycopy(table, 0, rowkey, 0, table.length);
      System.arraycopy(qualy, 0, rowkey, table.length, qualy.length);
      return rowkey;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("table: ").append(this.table.toString()).append("\n");
    for (Map.Entry<Famy, Set<Qualy>> entry : columns.entrySet()) {
      sb.append("family: ").append(entry.getKey().toString()).append("\t").append("qualifier: ")
        .append(entry.getValue().toString()).append("\n");
    }
    return sb.toString();
  }
}
