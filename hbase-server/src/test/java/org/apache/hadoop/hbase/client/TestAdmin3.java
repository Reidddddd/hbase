/**
 *
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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.secret.crypto.SecretCryptoType;
import org.apache.hadoop.hbase.security.authentication.SecretAdmin;
import org.apache.hadoop.hbase.security.authentication.SecretCryptor;
import org.apache.hadoop.hbase.security.authentication.SecretTableAccessor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKTableStateClientSideReader;
import org.apache.hadoop.hbase.zookeeper.ZNodeInfo;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestAdmin3 extends TestAdminBase {
  private static final Log LOG = LogFactory.getLog(TestAdmin3.class);

  @Test(timeout=300000)
  public void testDisableAndEnableTable() throws IOException {
    final byte [] row = Bytes.toBytes("row");
    final byte [] qualifier = Bytes.toBytes("qualifier");
    final byte [] value = Bytes.toBytes("value");
    final TableName table = TableName.valueOf("testDisableAndEnableTable");
    Table ht = TEST_UTIL.createTable(table, HConstants.CATALOG_FAMILY);
    Put put = new Put(row);
    put.add(HConstants.CATALOG_FAMILY, qualifier, value);
    ht.put(put);
    Get get = new Get(row);
    get.addColumn(HConstants.CATALOG_FAMILY, qualifier);
    ht.get(get);

    admin.disableTable(ht.getName());
    assertTrue("Table must be disabled.", TEST_UTIL.getHBaseCluster()
        .getMaster().getAssignmentManager().getTableStateManager().isTableState(
            ht.getName(), ZooKeeperProtos.Table.State.DISABLED));

    // Test that table is disabled
    get = new Get(row);
    get.addColumn(HConstants.CATALOG_FAMILY, qualifier);
    boolean ok = false;
    try {
      ht.get(get);
    } catch (TableNotEnabledException e) {
      ok = true;
    }
    ok = false;
    // verify that scan encounters correct exception
    Scan scan = new Scan();
    try {
      ResultScanner scanner = ht.getScanner(scan);
      Result res = null;
      do {
        res = scanner.next();
      } while (res != null);
    } catch (TableNotEnabledException e) {
      ok = true;
    }
    assertTrue(ok);
    admin.enableTable(table);
    assertTrue("Table must be enabled.", TEST_UTIL.getHBaseCluster()
        .getMaster().getAssignmentManager().getTableStateManager().isTableState(
            ht.getName(), ZooKeeperProtos.Table.State.ENABLED));

    // Test that table is enabled
    try {
      ht.get(get);
    } catch (RetriesExhaustedException e) {
      ok = false;
    }
    assertTrue(ok);
    ht.close();
  }

  @Test (timeout=300000)
  public void testDisableAndEnableTables() throws IOException {
    final byte [] row = Bytes.toBytes("row");
    final byte [] qualifier = Bytes.toBytes("qualifier");
    final byte [] value = Bytes.toBytes("value");
    final TableName table1 = TableName.valueOf("testDisableAndEnableTable1");
    final TableName table2 = TableName.valueOf("testDisableAndEnableTable2");
    Table ht1 = TEST_UTIL.createTable(table1, HConstants.CATALOG_FAMILY);
    Table ht2 = TEST_UTIL.createTable(table2, HConstants.CATALOG_FAMILY);
    Put put = new Put(row);
    put.add(HConstants.CATALOG_FAMILY, qualifier, value);
    ht1.put(put);
    ht2.put(put);
    Get get = new Get(row);
    get.addColumn(HConstants.CATALOG_FAMILY, qualifier);
    ht1.get(get);
    ht2.get(get);

    admin.disableTables("testDisableAndEnableTable.*");

    // Test that tables are disabled
    get = new Get(row);
    get.addColumn(HConstants.CATALOG_FAMILY, qualifier);
    boolean ok = false;
    try {
      ht1.get(get);
      ht2.get(get);
    } catch (org.apache.hadoop.hbase.DoNotRetryIOException e) {
      ok = true;
    }

    assertTrue(ok);
    admin.enableTables("testDisableAndEnableTable.*");

    // Test that tables are enabled
    try {
      ht1.get(get);
    } catch (IOException e) {
      ok = false;
    }
    try {
      ht2.get(get);
    } catch (IOException e) {
      ok = false;
    }
    assertTrue(ok);

    ht1.close();
    ht2.close();
  }

  /**
   * Test retain assignment on enableTable.
   *
   * @throws IOException
   */
  @Test (timeout=300000)
  public void testEnableTableRetainAssignment() throws IOException {
    final TableName tableName = TableName.valueOf("testEnableTableAssignment");
    byte[][] splitKeys = { new byte[] { 1, 1, 1 }, new byte[] { 2, 2, 2 },
      new byte[] { 3, 3, 3 }, new byte[] { 4, 4, 4 }, new byte[] { 5, 5, 5 },
      new byte[] { 6, 6, 6 }, new byte[] { 7, 7, 7 }, new byte[] { 8, 8, 8 },
      new byte[] { 9, 9, 9 } };
    int expectedRegions = splitKeys.length + 1;
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc, splitKeys);
    HTable ht = new HTable(TEST_UTIL.getConfiguration(), tableName);
    Map<HRegionInfo, ServerName> regions = ht.getRegionLocations();
    assertEquals("Tried to create " + expectedRegions + " regions "
        + "but only found " + regions.size(), expectedRegions, regions.size());
    // Disable table.
    admin.disableTable(tableName);
    // Enable table, use retain assignment to assign regions.
    admin.enableTable(tableName);
    Map<HRegionInfo, ServerName> regions2 = ht.getRegionLocations();

    // Check the assignment.
    assertEquals(regions.size(), regions2.size());
    for (Map.Entry<HRegionInfo, ServerName> entry : regions.entrySet()) {
      assertEquals(regions2.get(entry.getKey()), entry.getValue());
    }
  }

  @Test (timeout=300000)
  public void testEnableDisableAddColumnDeleteColumn() throws Exception {
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(TEST_UTIL);
    TableName tableName = TableName.valueOf("testEnableDisableAddColumnDeleteColumn");
    TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY).close();
    while (!ZKTableStateClientSideReader.isEnabledTable(zkw,
        TableName.valueOf("testEnableDisableAddColumnDeleteColumn"))) {
      Thread.sleep(10);
    }
    admin.disableTable(tableName);
    try {
      new HTable(TEST_UTIL.getConfiguration(), tableName);
    } catch (org.apache.hadoop.hbase.DoNotRetryIOException e) {
      //expected
    }

    admin.addColumn(tableName, new HColumnDescriptor("col2"));
    admin.enableTable(tableName);
    try {
      admin.deleteColumn(tableName, Bytes.toBytes("col2"));
    } catch (TableNotDisabledException e) {
      LOG.info(e);
    }
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  @Test (timeout=300000)
  public void testGetTableDescriptor() throws IOException {
    HColumnDescriptor fam1 = new HColumnDescriptor("fam1");
    HColumnDescriptor fam2 = new HColumnDescriptor("fam2");
    HColumnDescriptor fam3 = new HColumnDescriptor("fam3");
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("myTestTable"));
    htd.addFamily(fam1);
    htd.addFamily(fam2);
    htd.addFamily(fam3);
    admin.createTable(htd);
    Table table = new HTable(TEST_UTIL.getConfiguration(), htd.getTableName());
    HTableDescriptor confirmedHtd = table.getTableDescriptor();
    assertEquals(htd.compareTo(confirmedHtd), 0);
    table.close();
  }

  @Test (timeout=300000)
  public void testDeleteLastColumnFamily() throws Exception {
    TableName tableName = TableName.valueOf("testDeleteLastColumnFamily");
    TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY).close();
    while (!admin.isTableEnabled(TableName.valueOf("testDeleteLastColumnFamily"))) {
      Thread.sleep(10);
    }

    // test for enabled table
    try {
      admin.deleteColumn(tableName, HConstants.CATALOG_FAMILY);
      fail("Should have failed to delete the only column family of a table");
    } catch (InvalidFamilyOperationException ex) {
      // expected
    }

    // test for disabled table
    admin.disableTable(tableName);

    try {
      admin.deleteColumn(tableName, HConstants.CATALOG_FAMILY);
      fail("Should have failed to delete the only column family of a table");
    } catch (InvalidFamilyOperationException ex) {
      // expected
    }

    admin.deleteTable(tableName);
  }

  @Test (timeout=300000)
  public void testDeleteEditUnknownColumnFamilyAndOrTable() throws IOException {
    // Test we get exception if we try to
    final TableName nonexistentTable = TableName.valueOf("nonexistent");
    final byte[] nonexistentColumn = Bytes.toBytes("nonexistent");
    HColumnDescriptor nonexistentHcd = new HColumnDescriptor(nonexistentColumn);
    Exception exception = null;
    try {
      admin.addColumn(nonexistentTable, nonexistentHcd);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      admin.deleteTable(nonexistentTable);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      admin.deleteColumn(nonexistentTable, nonexistentColumn);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      admin.disableTable(nonexistentTable);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      admin.enableTable(nonexistentTable);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      admin.modifyColumn(nonexistentTable, nonexistentHcd);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      HTableDescriptor htd = new HTableDescriptor(nonexistentTable);
      htd.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
      admin.modifyTable(htd.getTableName(), htd);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    // Now make it so at least the table exists and then do tests against a
    // nonexistent column family -- see if we get right exceptions.
    final TableName tableName =
        TableName.valueOf("testDeleteEditUnknownColumnFamilyAndOrTable" + System.currentTimeMillis());
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor("cf"));
    admin.createTable(htd);
    try {
      exception = null;
      try {
        admin.deleteColumn(htd.getTableName(), nonexistentHcd.getName());
      } catch (IOException e) {
        exception = e;
      }
      assertNotNull(exception);
      assertTrue("found=" + exception.getClass().getName(),
          exception instanceof InvalidFamilyOperationException);

      exception = null;
      try {
        admin.modifyColumn(htd.getTableName(), nonexistentHcd);
      } catch (IOException e) {
        exception = e;
      }
      assertNotNull(exception);
      assertTrue("found=" + exception.getClass().getName(),
          exception instanceof InvalidFamilyOperationException);
    } finally {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
  }

  @Test(timeout = 300000)
  public void testGetZNodeCount() throws IOException {
    List<ZNodeInfo> zNodeInfoList = admin.getZNodeCount("/hbase");
    assertNotNull(zNodeInfoList);
    Map<String, Integer> collect = zNodeInfoList.stream().collect(
            Collectors.toMap(node -> node.getPath(), node -> node.getCount()));
    assertEquals(Integer.valueOf(1), collect.get("/hbase/master"));
  }

  @Test(timeout = 300000)
  public void testInitSecretAdminWithoutSecretTable() throws IOException {
    TableName secretTable = SecretTableAccessor.getSecretTableName();
    // If hbase:secret exist, delete it and refresh the admin to guarantee the inner cryptor is not
    // initialized.
    if (admin.tableExists(secretTable)) {
      TEST_UTIL.deleteTable(secretTable);
    }
    try {
      SecretAdmin secretAdmin = createSecretAdmin(TEST_UTIL.getConnection());
      fail("We should raise IOException when secretTable does not exist.");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof IOException);
      assertTrue(e.getCause().getMessage().contains("Try to initialize cryptor the cluster is not "
        + "initialized with secret key"));
    }
  }

  @Test(timeout = 300000)
  public void testUpdateAndRemoveAccountPassword()
    throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException,
    IllegalAccessException {
    createSecretTableIfNotExist();

    String username = "username";
    String password = "password";
    SecretAdmin secretAdmin = createSecretAdmin(TEST_UTIL.getConnection());
    secretAdmin.setAccountPassword(username, password);
    Table secretTable =
      TEST_UTIL.getConnection().getTable(SecretTableAccessor.getSecretTableName());
    SecretCryptor secretCryptor = new SecretCryptor();
    secretCryptor.initCryptos(secretTable, "i", "p");

    byte[] secret = SecretTableAccessor.getUserPassword(
      Bytes.toBytes(Hex.encodeHexString(Encryption.hash256(username))), secretTable);
    byte[] plainPasswordBytes = secretCryptor.decryptSecret(secret);
    assertEquals(Bytes.toString(plainPasswordBytes), password);

    // Remove the account.
    secretAdmin.removeAccount(username);
    secret = SecretTableAccessor.getUserPassword(
      Bytes.toBytes(Hex.encodeHexString(Encryption.hash256(username))), secretTable);
    assertNull(secret);
  }

  @Test(timeout = 300000)
  public void testUpdateAccountAllowFallback()
    throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException,
    IllegalAccessException {
    createSecretTableIfNotExist();
    String username = "username";
    SecretAdmin secretAdmin = createSecretAdmin(TEST_UTIL.getConnection());

    secretAdmin.allowAccountFallback(username);
    Table secretTable =
      TEST_UTIL.getConnection().getTable(SecretTableAccessor.getSecretTableName());
    assertTrue(SecretTableAccessor.allowFallback(Hex.encodeHexString(Encryption.hash256(username)),
      secretTable));

    secretAdmin.disallowAccountFallback(username);
    assertFalse(SecretTableAccessor.allowFallback(Hex.encodeHexString(Encryption.hash256(username)),
      secretTable));
  }

  private void createSecretTableIfNotExist() throws IOException {
    if (!admin.tableExists(SecretTableAccessor.getSecretTableName())) {
      Table secretTable = TEST_UTIL.createTable(SecretTableAccessor.getSecretTableName(),
        SecretTableAccessor.getSecretTableColumnFamily());
      SecureRandom rand = new SecureRandom(Bytes.toBytes(System.currentTimeMillis()));
      Arrays.stream(SecretCryptoType.values()).forEach((SecretCryptoType type) -> {
        try {
          // If there is no secret key for this algo, generate a new one.
          if (secretTable.get(new Get(type.getHashedName())).isEmpty()) {
            byte[] newKey = new byte[type.getKeyLength()];
            rand.nextBytes(newKey);
            SecretTableAccessor.insertSecretKey(type.getHashedName(),
              Bytes.toBytes(Base64.encodeBytes(newKey)), secretTable);
          }
        } catch (IOException e) {
          LOG.error(e);
        }
      });
    }
  }

  private SecretAdmin createSecretAdmin(Connection connection)
      throws NoSuchMethodException, InvocationTargetException, InstantiationException,
      IllegalAccessException {
    Constructor<SecretAdmin> constructor =
      SecretAdmin.class.getDeclaredConstructor(Connection.class, String.class,
        String.class, String.class, String.class);
    SecretAdmin admin;
    try {
      constructor.setAccessible(true);
      admin = constructor.newInstance(connection, "hbase:secret", "i", "p", "a");
    } finally {
      constructor.setAccessible(false);
    }
    return admin;
  }
}
