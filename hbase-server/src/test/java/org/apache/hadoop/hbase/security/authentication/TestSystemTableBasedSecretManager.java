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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier;
import org.apache.hadoop.hbase.security.token.SystemTableBasedSecretManager;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestSystemTableBasedSecretManager {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final String VALID_USERNAME = "testuser";
  private static final String VALID_GROUP = "testgroup";
  private static final String VALID_PASSWORD = "123456";
  private static final String VALID_CREDENTIAL =
      "U0hCYXMAAABAYWU1ZGViODIyZTBkNzE5OTI5MDA0NzFhNzE5OWQwZDk1Y"
          + "jhlN2M5ZDA1YzQwYTgyNDVhMjgxZmQyYzFkNjY4NDEyMzQ1Ng==";

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.cleanupTestDir();
  }

  @Test
  public void testEmptyLocalCredential() {
    UserGroupInformation.setLoginUser(
        UserGroupInformation.createUserForTesting(VALID_USERNAME, new String[] {VALID_GROUP}));
    DummyServer server = new DummyServer();

    SystemTableBasedSecretManager secretManager = new SystemTableBasedSecretManager(server);
    assertTrue(server.isAborted());
    assertTrue(server.getAbortCause() instanceof InvalidToken);
  }

  @Test
  public void testValidCredentialDecode() throws IOException {
    UserGroupInformation.setLoginUser(
        UserGroupInformation.createUserForTesting(VALID_USERNAME, new String[] {VALID_GROUP}));
    Server server = new DummyServer();
    server.getConfiguration().set(User.DIGEST_PASSWORD_KEY, VALID_CREDENTIAL);

    SystemTableBasedSecretManager secretManager = new SystemTableBasedSecretManager(server);
    User currentUser = User.getCurrent();

    assertNotNull(currentUser);
    assertEquals(1, currentUser.getTokens().size());

    Token<? extends TokenIdentifier> authToken = CollectionUtils.getFirst(currentUser.getTokens());
    AuthenticationTokenIdentifier identifier = secretManager.createIdentifier();
    identifier.readFields(new DataInputStream(new ByteArrayInputStream(authToken.getIdentifier())));

    assertEquals(identifier.getUsername(), VALID_USERNAME);
    assertEquals(0, Bytes.compareTo(Bytes.toBytes(VALID_PASSWORD), authToken.getPassword()));
  }

  @Test
  public void testInvalidCredentialDecode() throws IOException {
    UserGroupInformation.setLoginUser(
        UserGroupInformation.createUserForTesting(VALID_USERNAME, new String[] {VALID_GROUP}));
    DummyServer server = new DummyServer();
    server.getConfiguration().set(User.DIGEST_PASSWORD_KEY, "A random string");

    SystemTableBasedSecretManager secretManager = new SystemTableBasedSecretManager(server);
    assertTrue(server.isAborted());
    assertTrue(server.getAbortCause() instanceof InvalidToken);
  }

  @Test
  public void testAllowFallback() throws Exception {
    UserGroupInformation.setLoginUser(
        UserGroupInformation.createUserForTesting(VALID_USERNAME, new String[] {VALID_GROUP}));

    TEST_UTIL.getConfiguration().set(User.HBASE_SECURITY_CONF_KEY, "digest");
    TEST_UTIL.getConfiguration().set(User.DIGEST_PASSWORD_KEY, VALID_CREDENTIAL);

    TEST_UTIL.startMiniCluster();
    SystemTableBasedSecretManager secretManager =
        new SystemTableBasedSecretManager(TEST_UTIL.getHBaseCluster().getRegionServer(0));

    // Wait for the initialization of the secret table.
    Threads.sleep(10000);

    assertTrue(secretManager.isAllowedFallback(VALID_USERNAME));

    TableName tableName = SecretTableAccessor.getSecretTableName();
    Table authTable = TEST_UTIL.getConnection().getTable(tableName);
    Put put = new Put(encryptUsername(VALID_USERNAME));
    put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("a"), Bytes.toBytes(false));
    authTable.put(put);

    assertFalse(secretManager.isAllowedFallback(VALID_USERNAME));

    put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("a"), Bytes.toBytes(true));
    authTable.put(put);

    assertTrue(secretManager.isAllowedFallback(VALID_USERNAME));
  }

  private byte[] encryptUsername(String username) {
    return Encryption.hash256Hex(username);
  }

  static class DummyServer implements Server {
    private final Configuration conf;

    private Throwable t;
    private boolean aborted;

    DummyServer() {
      conf = HBaseConfiguration.create();
    }

    @Override
    public Configuration getConfiguration() {
      return conf;
    }

    @Override
    public ZooKeeperWatcher getZooKeeper() {
      return null;
    }

    @Override
    public CoordinatedStateManager getCoordinatedStateManager() {
      return null;
    }

    @Override
    public ClusterConnection getConnection() {
      return null;
    }

    @Override
    public MetaTableLocator getMetaTableLocator() {
      return null;
    }

    @Override
    public ServerName getServerName() {
      return null;
    }

    @Override
    public void abort(String why, Throwable e) {
      aborted = true;
      t = e;
    }

    @Override
    public boolean isAborted() {
      return aborted;
    }

    @Override
    public void stop(String why) {
    }

    @Override
    public boolean isStopped() {
      return false;
    }

    @Override
    public ChoreService getChoreService() {
      return null;
    }

    public Throwable getAbortCause() {
      return t;
    }
  }
}
