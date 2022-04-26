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
import static org.junit.Assert.assertTrue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestSecretTableManager {
  private static final Log LOG = LogFactory.getLog(TestSecretTableManager.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.cleanupTestDir();
  }

  @Test
  public void testSecretTableInitialization() throws Exception {
    TableName secretTableName = SecretTableAccessor.getSecretTableName();
    // username: testuser
    // password: 123456
    String credential =
        "U0hCYXMAAABAYWU1ZGViODIyZTBkNzE5OTI5MDA0NzFhNzE5OWQwZDk1YjhlN2M5ZDA1YzQwYTgyND"
            + "VhMjgxZmQyYzFkNjY4NDEyMzQ1Ng==";
    UserGroupInformation.setLoginUser(
        UserGroupInformation.createUserForTesting("testuser", new String[] {"testusergroup"}));

    TEST_UTIL.startMiniCluster();
    Admin admin = TEST_UTIL.getHBaseAdmin();
    assertFalse(admin.tableExists(secretTableName));
    TEST_UTIL.shutdownMiniCluster();

    TEST_UTIL.getConfiguration().set(User.HBASE_SECURITY_CONF_KEY, "digest");
    TEST_UTIL.getConfiguration().set(User.DIGEST_PASSWORD_KEY, credential);

    TEST_UTIL.startMiniCluster();
    admin = TEST_UTIL.getHBaseAdmin();
    
    // Wait one second to let master complete the table creation.
    Threads.sleep(10000);
    assertTrue(admin.tableExists(secretTableName));

    HTableDescriptor desc = admin.getTableDescriptor(secretTableName);
    assertEquals(1, desc.getFamilies().size());
    HColumnDescriptor realFamily = SecretTableAccessor.getSecretTableColumn();
    assertEquals(realFamily, desc.getColumnFamilies()[0]);

    Scan scan = new Scan();
    ResultScanner res = TEST_UTIL.getConnection().getTable(SecretTableAccessor.
        getSecretTableName()).getScanner(scan);
    int num = 0;
    for(Result r : res) {
      num ++;
    }
    assertEquals(SecretEncryptionType.values().length, num);
    TEST_UTIL.shutdownMiniCluster();
  }
}
