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

package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.schema.Column;
import org.apache.hadoop.hbase.schema.ColumnType;
import org.apache.hadoop.hbase.schema.Famy;
import org.apache.hadoop.hbase.schema.Qualy;
import org.apache.hadoop.hbase.schema.Schema;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The hbase:schema architecture is:
 * Row key               |    q      |
 *                       | a | b | c |
 * table_name            | _ | _ |   |
 * table_nameQualifier_1 | 1 |   |   |
 * table_nameQualifier_2 |   | 2     |
 * table_name1           |   |   | 3 |
 *
 * First of all, the family char is q in hbase:schema.
 * Qualifier will be families of each table, value is no need here.
 * Row key will be table name of a table or table_name + qualifier of a table.
 *
 * For example, from above, we can tell:
 * Table 'table_name' has two families, one is 'a', the other is 'b',
 * then 'table_name' has a 'Qualifier_1' which under family 'a',
 * and 'Qualifier_2' under family 'b'.
 *
 * The value of each cell is a byte code which represent its type,
 * except for the table row whose cell values are empty.
 * Byte code's details can be referred from {@link ColumnType}
 *
 * With design, we can:
 * 1. avoid fat row or fat cell
 * 2. scalable with more qualifiers, e.g, put the type of a column into the value
 * 3. Easy to achieve a table's information with prefix scan or start/stop row scan
 */
@InterfaceAudience.Private
public final class SchemaTableAccessor {

  private static final byte[] SCHEMA_TABLE_CF = Bytes.toBytes("q");

  private SchemaTableAccessor() {}

  /**
   * Check whether schema service is on
   */
  public static boolean schemaServiceIsOff(Connection connection) throws IOException {
    try (Admin admin = connection.getAdmin()) {
      return !admin.isTableAvailable(TableName.SCHEMA_TABLE_NAME);
    }
  }

  /**
   * If table has data, and schema service is on, it must contain a row 'tablename',
   * otherwise, it has no data
   */
  public static boolean checkSchemaExistence(Connection connection, TableName table)
      throws IOException {
    Result result;
    try (Table schemaTable = connection.getTable(TableName.SCHEMA_TABLE_NAME)) {
      Get get = new Get(table.getName());
      get.setCheckExistenceOnly(true);
      result = schemaTable.get(get);
    }
    return result != null && result.getExists() != null && result.getExists();
  }

  public static Schema getSchemaOf(TableName table, Connection connection)
      throws IOException {
    try (Table schemaTable = connection.getTable(TableName.SCHEMA_TABLE_NAME)) {
      Schema schema = new Schema(table);
      ResultScanner scanner = schemaTable.getScanner(createSchemaScanFor(table));
      for (Result res : scanner) {
        parseResult(schema, res);
      }
      return schema;
    }
  }

  public static void publishSchema(Schema schema, Connection connection) throws IOException {
    try (Table schemaTable = connection.getTable(TableName.SCHEMA_TABLE_NAME)) {
      byte[] t = schema.getTable().getName();
      List<Put> puts = new ArrayList<>();

      for (Column column : schema.getUpdatedColumns()) {
        if (puts.size() == 0) {
          // add this table row at the beginning for easier ACL check
          puts.add(new Put(t).addColumn(SCHEMA_TABLE_CF,
                                        column.getFamy().getFamily(),
                                        HConstants.EMPTY_BYTE_ARRAY));
        }

        byte[] qualifier = column.getQualy().getQualifier();
        byte[] row = new byte[t.length + qualifier.length];
        System.arraycopy(t, 0, row, 0, t.length);
        System.arraycopy(qualifier, 0, row, t.length, qualifier.length);
        Put put = new Put(row);
        put.addColumn(SCHEMA_TABLE_CF,
                      column.getFamy().getFamily(),
                      column.getType().getCode());
        puts.add(put);

        if (puts.size() == 100) {
          schemaTable.put(puts);
          puts = new ArrayList<>();
        }
      }

      // flush remaining
      if (puts.size() > 0) {
        schemaTable.put(puts);
      }
    }
  }

  /**
   * Create a scan for get all schema for a table.
   * Start key is 'tablename', end key is 'tablenamf', (eg. e + 1 => f).
   * Setting a small batch size, in case of it is fat columns table, so we can process in gentle.
   */
  public static Scan createSchemaScanFor(TableName table) {
    Scan scan = new Scan();
    byte[] startRow = table.getName();
    byte[] stopRow = new byte[startRow.length];
    System.arraycopy(startRow, 0, stopRow, 0, startRow.length);
    stopRow[stopRow.length - 1]++;
    scan.withStartRow(startRow);
    scan.withStopRow(stopRow);
    scan.setCacheBlocks(false);
    scan.setBatch(100);
    return scan;
  }

  private static void parseResult(Schema schema, Result res) throws IOException {
    byte[] table = schema.getTable().getName();
    if (Bytes.equals(table, res.getRow())) {
      // this is the 1st row, rowkey is exactly the table name
      // its qualifiers are families
      CellScanner cells = res.cellScanner();
      while (cells.advance()) {
        Cell cell = cells.current();
        schema.addFamily(CellUtil.cloneQualifier(cell));
      }
    } else {
      // this is a tablequalifier row
      byte[] row = res.getRow();
      byte[] qualifier = new byte[row.length - table.length];
      System.arraycopy(row, table.length, qualifier, 0, qualifier.length);
      CellScanner cells = res.cellScanner();
      while (cells.advance()) {
        Cell cell = cells.current();

        Famy famy = new Famy(CellUtil.cloneQualifier(cell));
        Qualy qualy = new Qualy(qualifier);
        qualy.updateType(ColumnType.parseType(CellUtil.cloneValue(cell)));
        schema.addColumn(famy, qualy);
      }
    }
  }

}
