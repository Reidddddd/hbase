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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import junit.framework.TestCase;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MediumTests.class})
public class TestConnectionCache extends TestCase {
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  /**
   * test for ConnectionCache cleaning expired HConnection
   */
  @Test
  public void testConnectionChore() throws Exception {
    UTIL.startMiniCluster();

    //1s for clean interval & 5s for maxIdleTime
    ConnectionCache cache = new ConnectionCache(UTIL.getConfiguration(),
        UserProvider.instantiate(UTIL.getConfiguration()), 1000, 5000);
    ConnectionCache.ConnectionInfo info = cache.getCurrentConnection();

    assertEquals(false, info.connection.isClosed());

    Thread.sleep(7000);

    assertEquals(true, info.connection.isClosed());
    UTIL.shutdownMiniCluster();
  }

  /**
   * Test for ConnectionCacheWithAuthToken setting authToken.
   */
  public void testConnectionSetToken() throws IOException {
    String username = "testuser";
    String password = "password";
    UTIL.getConfiguration().set(User.DIGEST_PASSWORD_KEY, "digest");
    UserProvider provider = UserProvider.instantiate(UTIL.getConfiguration());

    ConnectionCacheWithAuthToken cache = new ConnectionCacheWithAuthToken(UTIL.getConfiguration(),
        provider, 1000, 5000);
    cache.setEffectiveUser(username);
    cache.setPassword(password);

    User actualUser = cache.getConnectionUser(username);
    assertEquals(username, actualUser.getShortName());

    Token<? extends TokenIdentifier> authToken = CollectionUtils.getFirst(actualUser.getTokens());
    assertNotNull(authToken);
    assertNotNull(authToken.getPassword());
    assertEquals(0, Bytes.compareTo(authToken.getPassword(), Bytes.toBytes(password)));
  }

  public void testGetConnectionWithDifferentPassword() throws IOException {
    String username = "testuser";
    String password = "password";
    String secondPassword = "invalidpassword";

    UTIL.getConfiguration().set(User.DIGEST_PASSWORD_KEY, "digest");
    UserProvider provider = UserProvider.instantiate(UTIL.getConfiguration());

    ConnectionCacheWithAuthToken cache = new ConnectionCacheWithAuthToken(UTIL.getConfiguration(),
        provider, 1000, 5000);
    cache.setEffectiveUser(username);
    cache.setPassword(password);

    ConnectionCache.ConnectionInfo connectionInfoOne = cache.getCurrentConnection();
    cache.setPassword(secondPassword);

    ConnectionCache.ConnectionInfo connectionInfoTwo = cache.getCurrentConnection();
    assertNotSame(connectionInfoOne, connectionInfoTwo);
  }
}

