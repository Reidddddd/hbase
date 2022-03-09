/*
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
package org.apache.hadoop.hbase.security.authentication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.IOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestSecretTableAccessor {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final String TEST_USERNAME = "testuser";
  private static final String FAKE_USERNAME = "fake_username";
  private static final String TEST_PASSWORD = "password";
  private static final String FAKE_PASSWORD = "fake_password";
  private static Table authTable;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster();
    authTable = TEST_UTIL.createTable(TableName.valueOf("hbase:secret"), "i");
    Put initPut = new Put(Bytes.toBytes(TEST_USERNAME));
    initPut.addColumn(Bytes.toBytes("i"), Bytes.toBytes("p"), Bytes.toBytes(TEST_PASSWORD));
    authTable.put(initPut);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.cleanupTestDir();
  }

  @Test
  public void testAuthenticate() throws IOException {
    assertTrue(SecretTableAccessor.authenticate(TEST_USERNAME, TEST_PASSWORD, authTable));
    assertFalse(SecretTableAccessor.authenticate(TEST_USERNAME, FAKE_PASSWORD, authTable));
    assertFalse(SecretTableAccessor.authenticate(FAKE_USERNAME, FAKE_PASSWORD, authTable));
  }

  @Test
  public void testGetPassword() throws IOException {
    byte[] realUserBytes = Bytes.toBytes(TEST_USERNAME);
    byte[] fakeUserBytes = Bytes.toBytes(FAKE_USERNAME);
    byte[] realPasswordBytes = Bytes.toBytes(TEST_PASSWORD);

    assertEquals(0, Bytes.compareTo(realPasswordBytes,
        SecretTableAccessor.getUserPassword(realUserBytes, authTable)));
    assertNull(SecretTableAccessor.getUserPassword(fakeUserBytes, authTable));
  }

  @Test
  public void testAllowFallback() throws IOException {
    assertTrue(SecretTableAccessor.allowFallback(TEST_USERNAME, authTable));

    Put put = new Put(Bytes.toBytes(TEST_USERNAME));
    put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("a"), Bytes.toBytes(false));
    authTable.put(put);
    assertFalse(SecretTableAccessor.allowFallback(TEST_USERNAME, authTable));

    put = new Put(Bytes.toBytes(TEST_USERNAME));
    put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("a"), Bytes.toBytes(true));
    authTable.put(put);
    assertTrue(SecretTableAccessor.allowFallback(TEST_USERNAME, authTable));
  }

  @Test
  public void testSanityCheck() {
    try {
      SecretTableAccessor.getUserPassword(null, null);
      fail("Should catch IllegalArgumentException here.");
    } catch (Exception e) {
      assertTrue(e instanceof IllegalArgumentException);
    }

    try {
      Table tempTable = TEST_UTIL.createTable(TableName.valueOf("tempTable"), "cf");
      SecretTableAccessor.getUserPassword(null, tempTable);
      fail("Should catch IllegalArgumentException here.");
    } catch (Exception e) {
      assertTrue(e instanceof IllegalArgumentException);
    }
  }
}
