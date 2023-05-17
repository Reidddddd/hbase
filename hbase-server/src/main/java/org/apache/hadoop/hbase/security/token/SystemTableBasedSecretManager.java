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
package org.apache.hadoop.hbase.security.token;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.authentication.CredentialCache;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages an internal list of secret keys used to sign new authentication
 * tokens as they are generated, and to valid existing tokens used for
 * authentication.
 * Replace the ZK implementation in {@link AuthenticationTokenSecretManager}
 * with the mechanism based on system table 'hbase:secret'.
 */
@InterfaceAudience.Private
public class SystemTableBasedSecretManager extends AbstractAuthenticationSecretManager {
  private static final Logger LOG = LoggerFactory.getLogger(SystemTableBasedSecretManager.class);
  private static final byte[] CREDENTIAL_MAGIC_WORD = {'S', 'H', 'B', 'a', 's'};

  private CredentialCache credentialCache;

  public SystemTableBasedSecretManager(final Server server) {
    User adminUser;
    try {
      adminUser = UserProvider.instantiate(server.getConfiguration()).getCurrent();
    } catch (IOException e) {
      server.abort("Failed getting login user.", e);
      return;
    }
    try {
      String localCredential = server.getConfiguration().get(User.DIGEST_PASSWORD_KEY);
      credentialCache = new CredentialCache(server, adminUser.getShortName());
      setLocalCredential(adminUser, localCredential);
    } catch (InvalidToken e) {
      server.abort("Invalid local credential detected. Regionserver abort. ", e);
    }
  }

  /**
   * As we store user secret info in a system table, this method is not needed.
   */
  @Override
  protected byte[] createPassword(AuthenticationTokenIdentifier authenticationTokenIdentifier) {
    return new byte[0];
  }

  /**
   * Returns a password of a given user in bytes.
   */
  @InterfaceAudience.Private
  @Override
  public byte[] retrievePassword(AuthenticationTokenIdentifier authenticationTokenIdentifier)
    throws InvalidToken {
    String username = authenticationTokenIdentifier.getUsername();
    if (username == null || username.isEmpty()) {
      throw new InvalidToken("Username was not given when do authentication.");
    }
    byte[] result = credentialCache.getPassword(username);
    if (result == null || result.length == 0) {
      throw new InvalidToken("User: " + username + " is an invalid user.");
    }
    return result;
  }

  private void setLocalCredential(User user, String code) throws InvalidToken {
    if (code == null || code.isEmpty()) {
      throw new InvalidToken("The local credential for user :" + user.getShortName()
        + " is null. ");
    }

    byte[] decodedBytes;
    try {
      decodedBytes = Base64.getDecoder().decode(code);
    } catch (IllegalArgumentException e) {
      throw new InvalidToken(e.getMessage());
    }

    if (!isValidCredential(decodedBytes)) {
      throw new InvalidToken("The local credential for user :" + user.getShortName()
        + " is not valid. ");
    }

    ByteBuffer buffer = ByteBuffer.wrap(decodedBytes);
    buffer.position(CREDENTIAL_MAGIC_WORD.length);

    int nameLen = buffer.getInt();
    byte[] username = new byte[nameLen];
    buffer.get(username);

    if (0 != Bytes.compareTo(Encryption.hash256Hex(user.getShortName()), username)) {
      throw new InvalidToken("The local credential for user :" + user.getShortName()
        + " is not valid. ");
    }

    byte[] password = new byte[buffer.remaining()];
    buffer.get(password);

    AuthenticationTokenIdentifier identifier =
      new AuthenticationTokenIdentifier(user.getShortName());
    Token<? extends TokenIdentifier> adminToken = new Token<>();
    adminToken.setID(identifier.getBytes());
    adminToken.setPassword(password);
    adminToken.setService(new Text(HConstants.CLUSTER_ID_DEFAULT));
    adminToken.setKind(identifier.getKind());

    user.addToken(adminToken);
    credentialCache.insertLocalCredential(user.getShortName(), password, true);
  }

  private boolean isValidCredential(byte[] code) {
    int len = CREDENTIAL_MAGIC_WORD.length;
    return code.length >= len && 0 == Bytes.compareTo(code, 0, len, CREDENTIAL_MAGIC_WORD, 0, len);
  }
  /**
   * Return a new created empty identifier.
   */
  @Override
  public AuthenticationTokenIdentifier createIdentifier() {
    return new AuthenticationTokenIdentifier();
  }

  /**
   * Returns whether a user is allowed to fallback to SIMPLE authentication.
   */
  public boolean isAllowedFallback(String username) {
    if (username == null || username.isEmpty()) {
      LOG.warn("No valid username is found when doing allowFallback check. ");
      return false;
    }

    return credentialCache.getAllowFallback(username);
  }

  @Override
  public void start() {
  }

  @Override
  public void stop() {
  }
}
