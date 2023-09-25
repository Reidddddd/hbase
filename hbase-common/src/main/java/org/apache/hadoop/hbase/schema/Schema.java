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
import java.util.Collections;
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
public class Schema implements Iterable<Column> {

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

  public TableName getTable() {
    return table;
  }

  /**
   * A column = family + qualifier
   * It will iterate all columns in ascending order
   */
  @Override
  public Iterator<Column> iterator() {
    return new SchemaIterator(columns);
  }

  private class SchemaIterator implements Iterator<Column> {
    private final Map<Famy, Set<Qualy>> columns;
    private final List<Famy> families;
    private int familyIndex = 0;
    private Famy currentFamily;
    private Iterator<Qualy> columnIter = null;

    public SchemaIterator(Map<Famy, Set<Qualy>> columns) {
      this.columns = columns;
      families = new ArrayList<>(columns.keySet());
      Collections.sort(families);
    }

    @Override
    public boolean hasNext() {
      if (columnIter != null) {
        if (columnIter.hasNext()) {
          return true;
        } // else, finish iterating this family, iterate next family if any
      }

      if (familyIndex >= families.size()) {
        // finished all families, just return false
        return false;
      }

      currentFamily = families.get(familyIndex++);
      columnIter = columns.get(currentFamily).iterator();
      return columnIter.hasNext();
    }

    @Override
    public Column next() {
      return new Column(currentFamily, columnIter.next());
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("table: ").append(this.table.toString());
    for (Column column : this) {
      sb.append("\n").append(column);
    }
    return sb.toString();
  }
}
