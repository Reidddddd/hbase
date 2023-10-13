/*
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

import static org.apache.hadoop.hbase.HConstants.EMPTY_BYTE_ARRAY;
import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY;
import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.REGION_COPROCESSOR_CONF_KEY;
import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestSchema {
  private static final Log LOG = LogFactory.getLog(TestSchema.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final byte[] TEST_ROW = Bytes.toBytes("testRow");
  private static final byte[] TEST_VALUE = Bytes.toBytes("testValue");
  private static final byte[] TEST_FAMILY_1 = Bytes.toBytes("testFamily1");
  private static final byte[] TEST_FAMILY_2 = Bytes.toBytes("testFamily2");
  private static final byte[] TEST_FAMILY_3 = Bytes.toBytes("testFamily3");
  private static final byte[] TEST_QUALIFIER_ONE = Bytes.toBytes("q1");
  private static final byte[] TEST_QUALIFIER_TWO = Bytes.toBytes("q2");
  private static final byte[] TEST_QUALIFIER_THREE = Bytes.toBytes("q3");
  private static final byte[] APPEND_DATA = Bytes.toBytes("I am data");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.getConfiguration().set(REGION_COPROCESSOR_CONF_KEY,
      "org.apache.hadoop.hbase.schema.SchemaService");
    UTIL.getConfiguration().set(MASTER_COPROCESSOR_CONF_KEY,
      "org.apache.hadoop.hbase.schema.SchemaService");
    UTIL.getConfiguration().setInt("hbase.schema.updater.threads", 1);

    UTIL.startMiniCluster();
    // Wait until all initialized.
    UTIL.waitTableAvailable(TableName.SCHEMA_TABLE_NAME, 300000);
  }

  @AfterClass
  public static void shutdown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void TestSchemaTableInitialization() throws IOException {
    // The schema table should be created.
    Assert.assertTrue(UTIL.getHBaseAdmin().tableExists(TableName.SCHEMA_TABLE_NAME));
  }

  @Test
  public void TestSchemaRecord() throws IOException, InterruptedException {
    TableName tableName = TableName.valueOf("TestSchemaRecord");
    byte[][] families = new byte[3][];
    families[0] = TEST_FAMILY_1;
    families[1] = TEST_FAMILY_2;
    families[2] = TEST_FAMILY_3;
    Table table = UTIL.createTable(tableName, families);
    UTIL.waitTableAvailable(tableName);
    Put put = new Put(TEST_ROW);
    put.addColumn(TEST_FAMILY_1, TEST_QUALIFIER_ONE, TEST_VALUE);
    table.put(put);
    Put put2 = new Put(TEST_ROW);
    put2.addColumn(TEST_FAMILY_2, TEST_QUALIFIER_TWO, TEST_VALUE);
    table.put(put2);
    Increment increment1 = new Increment(TEST_ROW);
    increment1.addColumn(TEST_FAMILY_3, TEST_QUALIFIER_THREE, 1);
    table.increment(increment1);
    Append append1 = new Append(TEST_ROW);
    append1.add(TEST_FAMILY_3, TEST_QUALIFIER_THREE, APPEND_DATA);
    table.append(append1);

    // Wait for the async record.
    Thread.sleep(1000);

    Table schemaTable = UTIL.getConnection().getTable(TableName.SCHEMA_TABLE_NAME);
    byte[] tableNameBytes = tableName.getName();
    Scan scan = new Scan();
    scan.withStartRow(tableNameBytes);
    scan.setFilter(new PrefixFilter(tableNameBytes));

    ResultScanner scanner = schemaTable.getScanner(scan);
    Iterator<Result> iterator = scanner.iterator();

    // the first row is tableNameBytes, it has two qualifiers because this table has 3 families
    Assert.assertTrue(iterator.hasNext());
    Result res = iterator.next();
    Assert.assertTrue(Bytes.equals(tableNameBytes, res.getRow()));
    Assert.assertTrue(res.containsColumn(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_1));
    Assert.assertTrue(res.containsColumn(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_2));
    Assert.assertTrue(res.containsColumn(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_3));
    Assert.assertTrue(Bytes.equals(EMPTY_BYTE_ARRAY,
                                   res.getValue(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_1)));
    Assert.assertTrue(Bytes.equals(EMPTY_BYTE_ARRAY,
                                   res.getValue(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_2)));
    Assert.assertTrue(Bytes.equals(EMPTY_BYTE_ARRAY,
                                   res.getValue(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_3)));

    // the second row is tableNameBytesTEST_QUALIFIER_ONE, under family TEST_FAMILY_1
    Assert.assertTrue(iterator.hasNext());
    res = iterator.next();
    byte[] secondRow = new byte[tableNameBytes.length + TEST_QUALIFIER_ONE.length];
    System.arraycopy(tableNameBytes, 0, secondRow, 0, tableNameBytes.length);
    System.arraycopy(TEST_QUALIFIER_ONE, 0, secondRow,
        tableNameBytes.length, TEST_QUALIFIER_ONE.length);
    LOG.info("Row key of Result" + Bytes.toString(res.getRow()));
    Assert.assertTrue(Bytes.equals(secondRow, res.getRow()));
    Assert.assertTrue(res.containsColumn(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_1));
    Assert.assertFalse(res.containsColumn(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_2));
    Assert.assertFalse(res.containsColumn(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_3));
    Assert.assertTrue(Bytes.equals(ColumnType.NONE.getCode(),
                                   res.getValue(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_1)));
    byte[] x = res.getValue(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_3);

    // the third row is tableNameBytesTEST_QUALIFIER_TWO, under family TEST_FAMILY_2
    Assert.assertTrue(iterator.hasNext());
    res = iterator.next();
    byte[] thirdRow = new byte[tableNameBytes.length + TEST_QUALIFIER_TWO.length];
    System.arraycopy(tableNameBytes, 0, thirdRow, 0, tableNameBytes.length);
    System.arraycopy(TEST_QUALIFIER_TWO, 0, thirdRow,
        tableNameBytes.length, TEST_QUALIFIER_TWO.length);
    Assert.assertTrue(Bytes.equals(thirdRow, res.getRow()));
    Assert.assertFalse(res.containsColumn(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_1));
    Assert.assertTrue(res.containsColumn(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_2));
    Assert.assertFalse(res.containsColumn(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_3));
    Assert.assertTrue(Bytes.equals(ColumnType.NONE.getCode(),
                                   res.getValue(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_2)));

    // the fourth row is tableNameBytesTEST_QUALIFIER_THREE, under family TEST_FAMILY_3
    Assert.assertTrue(iterator.hasNext());
    res = iterator.next();
    byte[] fourthRow = new byte[tableNameBytes.length + TEST_QUALIFIER_THREE.length];
    System.arraycopy(tableNameBytes, 0, fourthRow, 0, tableNameBytes.length);
    System.arraycopy(TEST_QUALIFIER_THREE, 0, fourthRow,
      tableNameBytes.length, TEST_QUALIFIER_THREE.length);
    Assert.assertTrue(Bytes.equals(fourthRow, res.getRow()));
    Assert.assertFalse(res.containsColumn(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_1));
    Assert.assertFalse(res.containsColumn(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_2));
    Assert.assertTrue(res.containsColumn(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_3));
    Assert.assertTrue(Bytes.equals(ColumnType.NONE.getCode(),
                                   res.getValue(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_3)));

    // end
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void TestSchemaViaAdminAPI() throws Exception {
    TableName tableName = TableName.valueOf("TestSchemaViaAdminAPI");
    byte[][] families = new byte[3][];
    families[0] = TEST_FAMILY_1;
    families[1] = TEST_FAMILY_2;
    families[2] = TEST_FAMILY_3;
    Table table = UTIL.createTable(tableName, families);
    UTIL.waitTableAvailable(tableName);
    Put put = new Put(TEST_ROW);
    put.addColumn(TEST_FAMILY_1, TEST_QUALIFIER_ONE, TEST_VALUE);
    table.put(put);
    Put put2 = new Put(TEST_ROW);
    put2.addColumn(TEST_FAMILY_2, TEST_QUALIFIER_TWO, TEST_VALUE);
    table.put(put2);
    Increment increment1 = new Increment(TEST_ROW);
    increment1.addColumn(TEST_FAMILY_3, TEST_QUALIFIER_THREE, 1);
    table.increment(increment1);
    Append append1 = new Append(TEST_ROW);
    append1.add(TEST_FAMILY_3, TEST_QUALIFIER_THREE, APPEND_DATA);
    table.append(append1);

    // Slightly waiting for records being processed.
    Thread.sleep(1000);

    Admin admin = UTIL.getHBaseAdmin();
    Schema schema = admin.getSchemaOf(tableName);
    Assert.assertTrue(schema.containFamily(TEST_FAMILY_1));
    Assert.assertTrue(schema.containFamily(TEST_FAMILY_2));
    Assert.assertTrue(schema.containFamily(TEST_FAMILY_3));
    Assert.assertFalse(schema.containFamily(Bytes.toBytes("no_such_family")));

    Assert.assertTrue(schema.containColumn(TEST_FAMILY_1, TEST_QUALIFIER_ONE));
    Assert.assertTrue(schema.containColumn(TEST_FAMILY_2, TEST_QUALIFIER_TWO));
    Assert.assertTrue(schema.containColumn(TEST_FAMILY_3, TEST_QUALIFIER_THREE));
    Assert.assertFalse(schema.containColumn(TEST_FAMILY_3, Bytes.toBytes("no_such_column")));
  }

  @Test
  public void TestDeleteTable() throws IOException, InterruptedException {
    TableName tableName = TableName.valueOf("TestDeleteTable");
    Table table = UTIL.createTable(tableName, TEST_FAMILY_1);
    UTIL.waitTableAvailable(tableName);
    Put put = new Put(TEST_ROW);
    put.addColumn(TEST_FAMILY_1, TEST_QUALIFIER_ONE, TEST_VALUE);
    table.put(put);
    // Wait for the async schema recording.
    Thread.sleep(1000);

    Table schemaTable = UTIL.getConnection().getTable(TableName.SCHEMA_TABLE_NAME);
    byte[] tableNameBytes = tableName.getName();
    Scan scan = new Scan();
    scan.withStartRow(tableNameBytes);
    scan.setFilter(new PrefixFilter(tableNameBytes));

    ResultScanner scanner = schemaTable.getScanner(scan);
    Iterator<Result> iterator = scanner.iterator();

    // the first row is tableNameBytes
    Assert.assertTrue(iterator.hasNext());
    Result res = iterator.next();
    Assert.assertTrue(Bytes.equals(tableNameBytes, res.getRow()));
    Assert.assertEquals(1,
        res.getColumnCells(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_1).size());
    Assert.assertTrue(res.containsColumn(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_1));
    Assert.assertTrue(Bytes.equals(EMPTY_BYTE_ARRAY,
                                   res.getValue(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_1)));

    // the second row is tableNameBytesTEST_QUALIFIER_ONE, under family TEST_FAMILY_1
    Assert.assertTrue(iterator.hasNext());
    res = iterator.next();
    byte[] secondRow = new byte[tableNameBytes.length + TEST_QUALIFIER_ONE.length];
    System.arraycopy(tableNameBytes, 0, secondRow, 0, tableNameBytes.length);
    System.arraycopy(TEST_QUALIFIER_ONE, 0, secondRow,
        tableNameBytes.length, TEST_QUALIFIER_ONE.length);
    Assert.assertTrue(Bytes.equals(secondRow, res.getRow()));
    Assert.assertTrue(res.containsColumn(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_1));
    Assert.assertEquals(1,
        res.getColumnCells(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_1).size());
    Assert.assertTrue(Bytes.equals(ColumnType.NONE.getCode(),
                                   res.getValue(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_1)));

    // delete the table, the schema will be cleaned
    UTIL.deleteTable(tableName);
    // Wait for the async schema recording.
    UTIL.waitUntilNoRegionsInTransition();
    Assert.assertFalse(UTIL.getHBaseAdmin().tableExists(tableNameBytes));

    // will get empty result
    scanner = schemaTable.getScanner(scan);
    iterator = scanner.iterator();
    Assert.assertFalse(iterator.hasNext());

    checkCacheCleaned(tableName);
  }

  @Test
  public void TestTruncateTable() throws IOException, InterruptedException {
    TableName tableName = TableName.valueOf("TestTruncateTable");
    Table table = UTIL.createTable(tableName, TEST_FAMILY_1);
    UTIL.waitTableAvailable(tableName);
    Put put = new Put(TEST_ROW);
    put.addColumn(TEST_FAMILY_1, TEST_QUALIFIER_ONE, TEST_VALUE);
    table.put(put);
    // Wait for the async schema recording.
    Thread.sleep(1000);

    Table schemaTable = UTIL.getConnection().getTable(TableName.SCHEMA_TABLE_NAME);
    byte[] tableNameBytes = tableName.getName();
    Scan scan = new Scan();
    scan.withStartRow(tableNameBytes);
    scan.setFilter(new PrefixFilter(tableNameBytes));

    ResultScanner scanner = schemaTable.getScanner(scan);
    Iterator<Result> iterator = scanner.iterator();

    // the first row is tableNameBytes, it has two qualifiers because this table has 2 families
    Assert.assertTrue(iterator.hasNext());
    Result res = iterator.next();
    Assert.assertTrue(Bytes.equals(tableNameBytes, res.getRow()));
    Assert.assertEquals(1,
        res.getColumnCells(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_1).size());
    Assert.assertTrue(res.containsColumn(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_1));
    Assert.assertTrue(Bytes.equals(EMPTY_BYTE_ARRAY,
                                   res.getValue(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_1)));

    // the second row is tableNameBytesTEST_QUALIFIER_ONE, under family TEST_FAMILY_1
    Assert.assertTrue(iterator.hasNext());
    res = iterator.next();
    byte[] secondRow = new byte[tableNameBytes.length + TEST_QUALIFIER_ONE.length];
    System.arraycopy(tableNameBytes, 0, secondRow, 0, tableNameBytes.length);
    System.arraycopy(TEST_QUALIFIER_ONE, 0, secondRow,
        tableNameBytes.length, TEST_QUALIFIER_ONE.length);
    Assert.assertTrue(Bytes.equals(secondRow, res.getRow()));
    Assert.assertTrue(res.containsColumn(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_1));
    Assert.assertEquals(1,
        res.getColumnCells(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_1).size());
    Assert.assertTrue(Bytes.equals(ColumnType.NONE.getCode(),
                                   res.getValue(SchemaService.SCHEMA_TABLE_CF, TEST_FAMILY_1)));

    UTIL.truncateTable(tableName);
    // Wait for the async schema recording.
    Thread.sleep(1000);

    // will get empty result
    scanner = schemaTable.getScanner(scan);
    iterator = scanner.iterator();
    Assert.assertFalse(iterator.hasNext());

    checkCacheCleaned(tableName);
  }

  @Test
  public void TestUpdateColumnTypeOnClientSide() throws IOException, InterruptedException {
    // Init
    TableName tableName = TableName.valueOf("TestUpdateColumnTypeOnClientSide");
    byte[][] families = new byte[1][];
    families[0] = TEST_FAMILY_1;
    Table table = UTIL.createTable(tableName, families);
    UTIL.waitTableAvailable(tableName);

    // Put data
    Put put = new Put(TEST_ROW);
    put.addColumn(TEST_FAMILY_1, TEST_QUALIFIER_ONE, TEST_VALUE);
    table.put(put);
    Put put2 = new Put(TEST_ROW);
    put2.addColumn(TEST_FAMILY_1, TEST_QUALIFIER_TWO, TEST_VALUE);
    table.put(put2);
    // Wait for the schema recording.
    Thread.sleep(1000);

    // Get schema via API and did some simple verification
    Admin admin = UTIL.getHBaseAdmin();
    Schema schema = admin.getSchemaOf(tableName);
    Assert.assertTrue(schema.containFamily(TEST_FAMILY_1));
    Assert.assertTrue(schema.containColumn(TEST_FAMILY_1, TEST_QUALIFIER_ONE));
    Assert.assertTrue(schema.containColumn(TEST_FAMILY_1, TEST_QUALIFIER_TWO));
    Column column1 = schema.getColumn(TEST_FAMILY_1, TEST_QUALIFIER_ONE);
    Assert.assertTrue(Bytes.equals(TEST_FAMILY_1, column1.getFamy().getFamily()));
    Assert.assertTrue(Bytes.equals(TEST_QUALIFIER_ONE, column1.getQualy().getQualifier()));
    Assert.assertEquals(ColumnType.NONE, column1.getType());
    Column column2 = schema.getColumn(TEST_FAMILY_1, TEST_QUALIFIER_TWO);
    Assert.assertTrue(Bytes.equals(TEST_FAMILY_1, column2.getFamy().getFamily()));
    Assert.assertTrue(Bytes.equals(TEST_QUALIFIER_TWO, column2.getQualy().getQualifier()));
    Assert.assertEquals(ColumnType.NONE, column2.getType());

    // Update schema
    column1.updateType(ColumnType.INT);
    column2.updateType(ColumnType.STRING);
    admin.publishSchema(schema);
    // Wait for the update.
    Thread.sleep(1000);
    // Get schema via API and check again
    schema = admin.getSchemaOf(tableName);
    column1 = schema.getColumn(TEST_FAMILY_1, TEST_QUALIFIER_ONE);
    Assert.assertEquals(ColumnType.INT, column1.getType());
    column2 = schema.getColumn(TEST_FAMILY_1, TEST_QUALIFIER_TWO);
    Assert.assertEquals(ColumnType.STRING, column2.getType());
  }

  @Test
  public void testMaxColumnConstrain() throws Exception {
    TableName tableName = TableName.valueOf("testMaxColumnConstrain");
    byte[][] families = new byte[1][];
    families[0] = TEST_FAMILY_1;
    Table table = UTIL.createTable(tableName, families);
    UTIL.waitTableAvailable(tableName);

    // Default upper bound is 1000.
    for (int i = 0; i < 1005; i++) {
      Put put = new Put(TEST_ROW);
      put.addColumn(TEST_FAMILY_1, Bytes.toBytes(i), EMPTY_BYTE_ARRAY);
      table.put(put);
    }

    Thread.sleep(1000);

    byte[] metaFamily = Bytes.toBytes("m");
    byte[] countQualifier = Bytes.toBytes("c");
    Get get = new Get(tableName.getName());
    get.addColumn(metaFamily, countQualifier);

    Table schemaTable = UTIL.getConnection().getTable(TableName.SCHEMA_TABLE_NAME);
    schemaTable.get(get);
    Assert.assertTrue(1000 <=
      Bytes.toLong(schemaTable.get(get).getValue(metaFamily, countQualifier)));

    // Check we have only recorded the max size columns.
    Admin admin = UTIL.getHBaseAdmin();
    Schema schema = admin.getSchemaOf(tableName);
    Assert.assertEquals(1000, schema.numberOfColumns());
  }

  private static void checkCacheCleaned(TableName tableName) {
    SchemaProcessor schemaProcessor = SchemaProcessor.getInstance();
    Assert.assertTrue(schemaProcessor.isTableCleaned(tableName));
  }

}
