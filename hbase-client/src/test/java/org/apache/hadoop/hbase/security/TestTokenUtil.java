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
package org.apache.hadoop.hbase.security;

import static org.junit.Assert.assertEquals;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier;
import org.apache.hadoop.hbase.security.token.TokenUtil;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestTokenUtil {

  @Test
  public void testSetPassword() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    String username = "testuser";
    String password = "password";
    User user = User.createUserForTesting(conf, username, new String[]{"testgroup"});

    TokenUtil.setUserPassword(user, password);

    assertEquals(1, user.getTokens().size());

    for (Token<? extends TokenIdentifier> token : user.getTokens()) {
      AuthenticationTokenIdentifier actualIdentifier = new AuthenticationTokenIdentifier();
      actualIdentifier.readFields(
          new DataInputStream(new ByteArrayInputStream(token.getIdentifier())));

      assertEquals(username, actualIdentifier.getUsername());
      assertEquals(AuthenticationTokenIdentifier.AUTH_TOKEN_TYPE, token.getKind());
      assertEquals(0, Bytes.compareTo(Bytes.toBytes(password), token.getPassword()));
    }
  }

  @Test
  public void testSetPasswordTwice() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    String username = "testuser";
    String password = "password";
    String newPassword = "newPassword";
    User user = User.createUserForTesting(conf, username, new String[]{"testgroup"});

    TokenUtil.setUserPassword(user, password);
    TokenUtil.setUserPassword(user, newPassword);

    assertEquals(1, user.getTokens().size());

    for (Token<? extends TokenIdentifier> token : user.getTokens()) {
      AuthenticationTokenIdentifier actualIdentifier = new AuthenticationTokenIdentifier();
      actualIdentifier.readFields(
          new DataInputStream(new ByteArrayInputStream(token.getIdentifier())));

      assertEquals(username, actualIdentifier.getUsername());
      assertEquals(AuthenticationTokenIdentifier.AUTH_TOKEN_TYPE, token.getKind());
      assertEquals(0, Bytes.compareTo(Bytes.toBytes(password), token.getPassword()));
    }
  }
}
