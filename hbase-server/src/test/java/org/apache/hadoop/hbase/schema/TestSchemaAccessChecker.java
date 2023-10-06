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

import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY;
import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.REGION_COPROCESSOR_CONF_KEY;
import static org.apache.hadoop.hbase.security.Superusers.SUPERUSER_CONF_KEY;
import static org.apache.hadoop.hbase.security.User.HBASE_SECURITY_AUTHORIZATION_CONF_KEY;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessController;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.SecureTestUtil;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestSchemaAccessChecker {
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final byte[] TEST_ROW = Bytes.toBytes("testRow");
  private static final byte[] TEST_VALUE = Bytes.toBytes("testValue");
  private static final byte[] TEST_FAMILY_1 = Bytes.toBytes("testFamily1");
  private static final byte[] TEST_FAMILY_2 = Bytes.toBytes("testFamily2");
  private static final byte[] TEST_FAMILY_3 = Bytes.toBytes("testFamily3");
  private static final byte[] TEST_QUALIFIER_ONE = Bytes.toBytes("q1");
  private static final byte[] TEST_QUALIFIER_TWO = Bytes.toBytes("q2");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.getConfiguration().set(REGION_COPROCESSOR_CONF_KEY,
      SchemaService.class.getName() + "," + AccessController.class.getName());
    UTIL.getConfiguration().set(MASTER_COPROCESSOR_CONF_KEY,
        SchemaService.class.getName() + "," + AccessController.class.getName());
    UTIL.getConfiguration().setInt("hbase.schema.updater.threads", 1);
    UTIL.getConfiguration().setBoolean(HBASE_SECURITY_AUTHORIZATION_CONF_KEY, true);
    UTIL.getConfiguration().set(SUPERUSER_CONF_KEY, User.getCurrent().getShortName());

    UTIL.startMiniCluster();
    // Wait until all initialized.
    UTIL.waitUntilNoRegionsInTransition(300000);
  }

  @AfterClass
  public static void shutdown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testNormalSchemaAccess() throws Exception {
    TableName tableName = TableName.valueOf("TestNormalSchemaAccess");
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
    // Slightly waiting for records being processed.
    Thread.sleep(1000);

    // 1. denied at getSchema
    User foo = User.createUserForTesting(UTIL.getConfiguration(), "foo", new String[0]);
    foo.runAs((PrivilegedAction<Schema>) () -> {
      Admin admin = null;
      try {
        Connection connection = ConnectionFactory.createConnection(UTIL.getConfiguration(), foo);
        admin = connection.getAdmin();
        Schema schema = admin.getSchemaOf(tableName);
        fail("Expected exception was not thrown for user '" + foo.getShortName() + "'");
        return schema;
      } catch (IOException e) {
        assertTrue(e instanceof AccessDeniedException);
      }
      return null;
    });

    // 2. grant read permission
    SecureTestUtil.grantOnTable(UTIL, foo.getShortName(), tableName,
        null, null, Permission.Action.READ);

    // 3. should pass get schema
    final Schema schema = foo.runAs((PrivilegedAction<Schema>) () -> {
      Admin admin = null;
      try {
        Connection connection = ConnectionFactory.createConnection(UTIL.getConfiguration(), foo);
        admin = connection.getAdmin();
        Schema s = admin.getSchemaOf(tableName);
        return s;
      } catch (IOException e) {
        fail("Unexpected exception was thrown for user '" + foo.getShortName() + "' " + e);
      }
      return null;
    });
    Assert.assertTrue(schema.containFamily(TEST_FAMILY_1));
    Assert.assertTrue(schema.containFamily(TEST_FAMILY_2));
    // because we didn't put a cell under it
    Assert.assertFalse(schema.containFamily(TEST_FAMILY_3));
    Assert.assertTrue(schema.containColumn(TEST_FAMILY_1, TEST_QUALIFIER_ONE));
    Assert.assertTrue(schema.containColumn(TEST_FAMILY_2, TEST_QUALIFIER_TWO));

    // 4. test update schema and get AccessDenied
    Column column = schema.getColumn(TEST_FAMILY_1, TEST_QUALIFIER_ONE);
    column.updateType(ColumnType.INT);
    foo.runAs((PrivilegedAction<Schema>) () -> {
      Admin admin = null;
      try {
        Connection connection = ConnectionFactory.createConnection(UTIL.getConfiguration(), foo);
        admin = connection.getAdmin();
        admin.publishSchema(schema);
        fail("Expected exception was not thrown for user '" + foo.getShortName() + "'");
      } catch (IOException e) {
        assertTrue(e instanceof RetriesExhaustedWithDetailsException);
        Throwable t = ((RetriesExhaustedWithDetailsException) e).getCause(0);
        assertTrue(t instanceof AccessDeniedException);
      }
      return null;
    });

    // 5. grant write permission
    SecureTestUtil.grantOnTable(UTIL, foo.getShortName(), tableName,
        null, null, Permission.Action.WRITE);
    foo.runAs((PrivilegedAction<Void>) () -> {
      Admin admin = null;
      try {
        Connection connection = ConnectionFactory.createConnection(UTIL.getConfiguration(), foo);
        admin = connection.getAdmin();
        admin.publishSchema(schema);
      } catch (IOException e) {
        fail("Unexpected exception was thrown for user '" + foo.getShortName() + "' " + e);
      }
      return null;
    });
  }

  @Test
  public void testAbnormalGetSchema() throws Exception {
    TableName tableName = TableName.valueOf("TestAbnormalGetSchema");
    byte[][] families = new byte[3][];
    families[0] = TEST_FAMILY_1;
    families[1] = TEST_FAMILY_2;
    families[2] = TEST_FAMILY_3;
    UTIL.createTable(tableName, families);
    UTIL.waitTableAvailable(tableName);

    User foo = User.createUserForTesting(UTIL.getConfiguration(), "foo", new String[0]);
    foo.runAs((PrivilegedAction<Schema>) () -> {
      Table table = null;
      try {
        Connection connection = ConnectionFactory.createConnection(UTIL.getConfiguration(), foo);
        Admin admin = connection.getAdmin();
        table = admin.getConnection().getTable(TableName.SCHEMA_TABLE_NAME);
      } catch (Throwable t) {
        fail("should not reach here at init stage");
      }

      // 1. Try to get it directly via Get API
      try {
        table.get(new Get(tableName.getName()));
        fail("Expected exception was not thrown for user '" + foo.getShortName() + "'");
      } catch (IOException e) {
        assertTrue(e instanceof AccessDeniedException);
      }
      // 2. Try to get a non-existed table directly via Get API
      try {
        table.get(new Get(Bytes.toBytes("not_exist_table_name")).setCheckExistenceOnly(true));
        fail("Expected exception was not thrown for user '" + foo.getShortName() + "'");
      } catch (IOException e) {
        assertTrue(e instanceof AccessDeniedException);
      }
      // 3. Try to get a system table directly via Get API
      try {
        table.get(new Get(Bytes.toBytes("hbase:acl")).setCheckExistenceOnly(true));
        fail("Expected exception was not thrown for user '" + foo.getShortName() + "'");
      } catch (IOException e) {
        assertTrue(e instanceof AccessDeniedException);
      }
      // 4. Try to scan directly via API
      try {
        Scan scan = new Scan();
        scan.withStartRow(tableName.getName());
        scan.setFilter(new PrefixFilter(tableName.getName()));
        table.getScanner(scan);
      } catch (IOException e) {
        assertTrue(e instanceof AccessDeniedException);
      }
      // 5. Try to scan directly via API, but not a valid table
      try {
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes("not_exist_table_name"));
        scan.setFilter(new PrefixFilter(Bytes.toBytes("not_exist_table_name")));
        table.getScanner(scan);
      } catch (IOException e) {
        assertTrue(e instanceof AccessDeniedException);
      }
      return null;
    });
  }

  @Test
  public void testAbnormalWrite2Schema() throws Exception {
    TableName tableName = TableName.valueOf("TestAbnormalWrite2Schema");
    byte[][] families = new byte[3][];
    families[0] = TEST_FAMILY_1;
    families[1] = TEST_FAMILY_2;
    families[2] = TEST_FAMILY_3;
    UTIL.createTable(tableName, families);
    UTIL.waitTableAvailable(tableName);

    User foo = User.createUserForTesting(UTIL.getConfiguration(), "foo", new String[0]);
    // 1. directly put without permission
    foo.runAs((PrivilegedAction<Void>) () -> {
      Table table = null;
      try {
        Connection connection = ConnectionFactory.createConnection(UTIL.getConfiguration(), foo);
        Admin admin = connection.getAdmin();
        table = admin.getConnection().getTable(TableName.SCHEMA_TABLE_NAME);
      } catch (Throwable t) {
        fail("should not reach here at init stage");
      }

      try {
        Put put = new Put(tableName.getName());
        put.addColumn(TEST_FAMILY_1, TEST_QUALIFIER_ONE, Bytes.toBytes("useless"));
        table.put(put);
      } catch (IOException e) {
        assertTrue(e instanceof AccessDeniedException);
      }
      return null;
    });

    // grant permission
    SecureTestUtil.grantOnTable(UTIL, foo.getShortName(), tableName,
        null, null, Permission.Action.WRITE);
    // 2. Mix some abnormal put into normal puts
    foo.runAs((PrivilegedAction<Void>) () -> {
      Table table = null;
      try {
        Connection connection = ConnectionFactory.createConnection(UTIL.getConfiguration(), foo);
        Admin admin = connection.getAdmin();
        table = admin.getConnection().getTable(TableName.SCHEMA_TABLE_NAME);
      } catch (Throwable t) {
        fail("should not reach here at init stage");
      }

      List<Put> puts = new ArrayList<>();
      try {
        Put put = new Put(tableName.getName());
        put.addColumn(TEST_FAMILY_1, TEST_QUALIFIER_ONE, Bytes.toBytes("useless"));
        puts.add(put);
        Put putQ = new Put(Bytes.toBytes("TestAbnormalWrite2Schemaq1"));
        putQ.addColumn(TEST_FAMILY_1, TEST_QUALIFIER_ONE, Bytes.toBytes("useless"));
        puts.add(putQ);
        Put abnormalPut = new Put(Bytes.toBytes("Not a same table"));
        abnormalPut.addColumn(TEST_FAMILY_1, TEST_QUALIFIER_ONE, Bytes.toBytes("useless"));
        puts.add(abnormalPut);
        table.put(puts);
      } catch (IOException e) {
        assertTrue(e instanceof RetriesExhaustedWithDetailsException);
        Throwable t = ((RetriesExhaustedWithDetailsException) e).getCause(0);
        assertTrue(t instanceof AccessDeniedException);
      }
      return null;
    });
  }
}
