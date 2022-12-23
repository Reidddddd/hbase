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
package org.apache.hadoop.hbase.security.authentication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestCredentialCache {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Log LOG = LogFactory.getLog(TestCredentialCache.class);

  private static final String SUPER_USERNAME = "testuser";
  private static final String SUPER_GROUP = "testgroup";
  private static final String SUPER_PASSWORD = "123456";
  private static final String LOCAL_CREDENTIAL =
    "U0hCYXMAAABAYWU1ZGViODIyZTBkNzE5OTI5MDA0NzFhNzE5OWQwZDk1Y"
      + "jhlN2M5ZDA1YzQwYTgyNDVhMjgxZmQyYzFkNjY4NDEyMzQ1Ng==";

  private static final String VALID_USERNAME = "testuser1";
  private static final String FIRST_PASSWORD = "password1";
  private static final String SECOND_PASSWORD = "password2";
  private static final String FAKE_USER = "fakeuser";

  @BeforeClass
  public static void setup() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    UserGroupInformation.setLoginUser(
      UserGroupInformation.createUserForTesting(SUPER_USERNAME, new String[] {SUPER_GROUP}));
    conf.set(User.HBASE_SECURITY_CONF_KEY, "digest");
    conf.set(User.DIGEST_PASSWORD_KEY, LOCAL_CREDENTIAL);
    conf.setInt("hbase.secret.refresh.period", 5000); // 5 seconds.
    conf.setInt("hbase.secret.expire.time", 1000);
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.cleanupTestDir();
  }

  @Test
  public void testRefresh() throws IOException, InterruptedException {
    HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
    CredentialCache cc = new CredentialCache(rs, SUPER_USERNAME);
    DummyCryptor decryptor = new DummyCryptor();
    decryptor.setInitialized(false);
    cc.setDecryptor(decryptor);

    Table secretTable = rs.getConnection().getTable(SecretTableAccessor.getSecretTableName());
    Put firstPut = new Put(getHashedUsername(VALID_USERNAME));
    firstPut.addColumn(Bytes.toBytes("i"), Bytes.toBytes("p"), Bytes.toBytes(FIRST_PASSWORD));
    secretTable.put(firstPut);

    // If the decryptor is not initialized, the first time checked password should be byte[0]
    byte[] fp1 = cc.getPassword(VALID_USERNAME);
    assertEquals(0, fp1.length);

    decryptor.setInitialized(true);
    fp1 = cc.getPassword(VALID_USERNAME);

    LOG.info("Fp1 is " + Bytes.toString(fp1));
    assertEquals(0, Bytes.compareTo(fp1, Bytes.toBytes(FIRST_PASSWORD)));

    Put secondPut = new Put(getHashedUsername(VALID_USERNAME));
    secondPut.addColumn(Bytes.toBytes("i"), Bytes.toBytes("p"),
        Bytes.toBytes(SECOND_PASSWORD));
    secretTable.put(secondPut);

    byte[] fp2 = cc.getPassword(VALID_USERNAME);
    LOG.info("Fp2 first is " + Bytes.toString(fp2));
    assertEquals(0, Bytes.compareTo(fp2, Bytes.toBytes(FIRST_PASSWORD)));

    Thread.sleep(20000);

    fp2 = cc.getPassword(VALID_USERNAME);
    LOG.info("Fp2 second is " + Bytes.toString(fp2));
    assertEquals(0, Bytes.compareTo(fp2, Bytes.toBytes(SECOND_PASSWORD)));

    cc.insertLocalCredential(SUPER_USERNAME, Bytes.toBytes(SUPER_PASSWORD), true);

    Put thirdPut = new Put(getHashedUsername(SUPER_USERNAME));
    thirdPut.addColumn(Bytes.toBytes("i"), Bytes.toBytes("p"), Bytes.toBytes(FIRST_PASSWORD));

    Thread.sleep(10000);
    byte[] superPassword = cc.getPassword(SUPER_USERNAME);
    assertEquals(0, Bytes.compareTo(superPassword, Bytes.toBytes(SUPER_PASSWORD)));
  }

  @Test
  public void testExpiration() throws IOException, InterruptedException {
    HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
    CredentialCache cc = new CredentialCache(rs, SUPER_USERNAME);

    DummyCryptor decryptor = new DummyCryptor();
    decryptor.setInitialized(true);
    cc.setDecryptor(decryptor);

    Table secretTable = rs.getConnection().getTable(SecretTableAccessor.getSecretTableName());
    Put firstPut = new Put(getHashedUsername(VALID_USERNAME));
    firstPut.addColumn(Bytes.toBytes("i"), Bytes.toBytes("p"), Bytes.toBytes(FIRST_PASSWORD));
    secretTable.put(firstPut);

    assertFalse(cc.isValid(VALID_USERNAME));

    cc.getPassword(VALID_USERNAME);
    assertTrue(cc.isValid(VALID_USERNAME));

    Thread.sleep(2000);
    assertFalse(cc.isValid(VALID_USERNAME));
  }


  @Test
  public void testInvalidUserAccess() throws IOException {
    HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
    CredentialCache cc = new CredentialCache(rs, SUPER_USERNAME);

    DummyCryptor decryptor = new DummyCryptor();
    decryptor.setInitialized(true);
    cc.setDecryptor(decryptor);

    Table secretTable = rs.getConnection().getTable(SecretTableAccessor.getSecretTableName());
    Put firstPut = new Put(getHashedUsername(VALID_USERNAME));
    firstPut.addColumn(Bytes.toBytes("i"), Bytes.toBytes("p"), Bytes.toBytes(FIRST_PASSWORD));
    firstPut.addColumn(Bytes.toBytes("i"), Bytes.toBytes("a"), Bytes.toBytes(false));
    secretTable.put(firstPut);

    byte[] validPassword = cc.getPassword(VALID_USERNAME);
    assertNotEquals(0, validPassword.length);
    assertFalse(cc.getAllowFallback(VALID_USERNAME));

    byte[] invalidPassword = cc.getPassword(FAKE_USER);
    assertNotNull(invalidPassword);
    assertEquals(0, invalidPassword.length);
    assertTrue(cc.getAllowFallback(FAKE_USER));

  }

  private byte[] getHashedUsername(String username) {
    return Encryption.hash256Hex(username);
  }

  static class DummyCryptor extends SecretCryptor {
    private boolean initialized = false;

    @Override
    public byte[] decryptSecret(byte[] secret) throws IOException {
      return secret;
    }

    @Override
    public boolean isInitialized() {
      return initialized;
    }

    public void setInitialized(boolean value) {
      initialized = value;
    }
  }
}
