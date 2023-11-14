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
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.schema.Schema;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The hbase:schema architecture is:
 * Row key               |    q      |
 *                       | a | b | c |
 * table_name            | _ | _ |   |
 * table_nameQualifier_1 | _ |   |   |
 * table_nameQualifier_2 |   | _     |
 * table_name1           |   |   | _ |
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
 * The value of each cell will be type of a column in the future. For example:
 * string/int/long, can be put into the value to indicate the type
 *
 * With design, we can:
 * 1. avoid fat row or fat cell
 * 2. scalable with more qualifiers, e.g, put the type of a column into the value
 * 3. Easy to achieve a table's information with prefix scan
 */
@InterfaceAudience.Private
public final class SchemaTableAccessor {

  private SchemaTableAccessor() {}

  /**
   * Check whether schema service is on
   */
  public static boolean isSchemaServiceRunning(Connection connection) throws IOException {
    try (Admin admin = connection.getAdmin()) {
      return admin.isTableAvailable(TableName.SCHEMA_TABLE_NAME);
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
        schema.addColumn(CellUtil.cloneQualifier(cell), qualifier);
      }
    }
  }

}
