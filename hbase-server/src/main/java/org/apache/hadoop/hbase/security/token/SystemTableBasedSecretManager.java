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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.authentication.SecretTableAccessor;
import org.apache.hadoop.hbase.util.Bytes;
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

  private final Server server;
  private final ThreadLocal<Table> authTable = new ThreadLocal<>();

  public SystemTableBasedSecretManager(Server server) {
    this.server = server;
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

    if (authTable.get() == null) {
      ClusterConnection connection = server.getConnection();
      if (connection == null) {
        // We should never go here.
        throw new IllegalStateException("The ClusterConnection in region server "
            + "is not initialized.");
      }

      try {
        authTable.set(connection.
            getTable(TableName.valueOf(SecretTableAccessor.SECRET_TABLE_NAME)));
      } catch (IOException e) {
        LOG.warn("Error detected when access hbase:secret table. ", e);
        return new byte[0];
      }
    }

    byte[] password = null;
    try {
      password = SecretTableAccessor.getUserPassword(Bytes.toBytes(username), authTable.get());
      if (password == null) {
        LOG.warn("Password is null");
      }
    } catch (IOException e) {
      LOG.error("Failed retrieving password for user " + username + "\n", e);
    }
    return password == null ? new byte[0] : password;
  }

  /**
   * Return a new created empty identifier.
   */
  @Override
  public AuthenticationTokenIdentifier createIdentifier() {
    return new AuthenticationTokenIdentifier();
  }

  @Override
  public void start() {
  }

  @Override
  public void stop() {
  }
}
