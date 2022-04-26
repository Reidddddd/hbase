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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.authentication.SecretDecryptor;
import org.apache.hadoop.hbase.security.authentication.SecretTableAccessor;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Manages an internal list of secret keys used to sign new authentication
 * tokens as they are generated, and to valid existing tokens used for
 * authentication.
 * Replace the ZK implementation in {@link AuthenticationTokenSecretManager}
 * with the mechanism based on system table 'hbase:secret'.
 */
@InterfaceAudience.Private
public class SystemTableBasedSecretManager extends AbstractAuthenticationSecretManager {
  private static final Log LOG = LogFactory.getLog(SystemTableBasedSecretManager.class);
  private static final byte[] CREDENTIAL_MAGIC_WORD = {'S', 'H', 'B', 'a', 's'};

  private final Server server;
  private final SecretDecryptor decryptor;
  private final ThreadLocal<Table> authTable = new ThreadLocal<>();

  private final transient Token<? extends TokenIdentifier> adminToken = new Token<>();

  public SystemTableBasedSecretManager(final Server server) {
    User adminUser;
    this.server = server;
    this.decryptor = new SecretDecryptor();
    try {
      adminUser = UserProvider.instantiate(server.getConfiguration()).getCurrent();
    } catch (IOException e) {
      server.abort("Failed getting login user.", e);
      return;
    }
    try {
      String localCredential = server.getConfiguration().get(User.DIGEST_PASSWORD_KEY);
      setLocalCredential(adminUser, localCredential);
    } catch (InvalidToken e) {
      server.abort("Invalid local credential detected. Regionserver abort. ", e);
    }
    Thread decrytorInitThread = new Thread(new Runnable() {
      @Override
      public void run() {
        String msg = "Secret decryptor initialization failed. ";
        while (server.getConnection() == null) {
          // Wait for the cluster connection available.
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            server.abort(msg, new IllegalStateException(msg));
          }
        }
        try {
          // Wait for the secret table initialization.
          ClusterConnection conn = server.getConnection();
          while (!conn.isTableAvailable(SecretTableAccessor.getSecretTableName())) {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              // Do nothing.
            }
          }
          Thread.sleep(1000);
          decryptor.initDecryption(getAuthTable(), server);
        } catch (IOException | InterruptedException e) {
          server.abort(e.getMessage(), e);
        }
        if (!decryptor.isInitialized()) {
          server.abort(msg, new IllegalStateException(msg));
        }
      }
    });
    decrytorInitThread.setDaemon(true);
    decrytorInitThread.start();
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
  @Override
  public byte[] retrievePassword(AuthenticationTokenIdentifier authenticationTokenIdentifier)
      throws InvalidToken {
    String username = authenticationTokenIdentifier.getUsername();
    if (username == null || username.isEmpty()) {
      throw new InvalidToken("Username was not given when do authentication.");
    }
    if (Bytes.compareTo(authenticationTokenIdentifier.getBytes(),
        adminToken.getIdentifier()) == 0) {
      return adminToken.getPassword();
    }

    byte[] password = null;
    try {
      byte[] cipherPassword =
          SecretTableAccessor.getUserPassword(getHashedUsername(username), getAuthTable());
      password = decryptor.decryptSecret(cipherPassword);
      if (password == null) {
        LOG.warn("Password of given user " + username + " is null. ");
      }
    } catch (IOException e) {
      LOG.error("Failed retrieving password for user " + username + "\n", e);
    }
    return password == null ? new byte[0] : password;
  }

  private void setLocalCredential(User user, String code) throws InvalidToken {
    if (code == null || code.isEmpty()) {
      throw new InvalidToken("The local credential for user :" + user.getShortName()
          + " is null. ");
    }
    byte[] decodedBytes = Base64.decode(code);

    if (!isValidCredential(decodedBytes)) {
      throw new InvalidToken("The local credential for user :" + user.getShortName()
          + " is not valid. ");
    }

    ByteBuffer buffer = ByteBuffer.wrap(decodedBytes);
    buffer.position(CREDENTIAL_MAGIC_WORD.length);

    int nameLen = buffer.getInt();
    byte[] username = new byte[nameLen];
    buffer.get(username);

    if (0 != Bytes.compareTo(getHashedUsername(user.getShortName()), username)) {
      throw new InvalidToken("The local credential for user :" + user.getShortName()
          + " is not valid. ");
    }

    byte[] password = new byte[buffer.remaining()];
    buffer.get(password);

    AuthenticationTokenIdentifier identifier =
        new AuthenticationTokenIdentifier(user.getShortName());
    adminToken.setID(identifier.getBytes());
    adminToken.setPassword(password);
    adminToken.setService(new Text(HConstants.CLUSTER_ID_DEFAULT));
    adminToken.setKind(identifier.getKind());

    user.addToken(adminToken);
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

    try {
      return SecretTableAccessor.allowFallback(getHashedUsername(username), getAuthTable());
    } catch (IOException e) {
      LOG.warn("Error occurs when check allowFallback for user " + username + " \n", e);
      return false;
    }
  }

  private Table getAuthTable() throws IOException {
    if (authTable.get() == null) {
      ClusterConnection connection = server.getConnection();
      if (connection == null) {
        // We should never go here.
        throw new IllegalStateException("The ClusterConnection in region server "
            + "is not initialized.");
      }
      authTable.set(connection.getTable(SecretTableAccessor.getSecretTableName()));
    }
    return authTable.get();
  }

  private byte[] getHashedUsername(String username) {
    return Encryption.hash256Hex(username);
  }

  @Override
  public void start() {
  }

  @Override
  public void stop() {
  }
}
