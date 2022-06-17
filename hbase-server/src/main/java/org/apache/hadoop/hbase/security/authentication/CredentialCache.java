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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A map to store user credentials.
 * The purpose of this class is to reduce the dependency between RPC authentication and
 * secret table. This class will periodically refresh the cache from secret table.
 */
@InterfaceAudience.Private
public class CredentialCache {
  private static final Log LOG = LogFactory.getLog(CredentialCache.class);

  private static final String CREDENTIAL_REFRESH_PERIOD = "hbase.secret.refresh.period";
  private static final int CREDENTIAL_REFRESH_PERIOD_DEFAULT = 600000;  // In milliseconds

  private SecretDecryptor decryptor = new SecretDecryptor();
  private final ThreadLocal<Table> authTable = new ThreadLocal<>();

  private final ConcurrentMap<String, CredentialEntry> credentialMap = new ConcurrentHashMap<>();
  private final Server server;
  private final String loginUser;

  public CredentialCache(final Server server, final String loginUser) {
    this.server = server;
    this.loginUser = loginUser;

    Configuration conf = server.getConfiguration();
    // Refresh the cache every 10 minutes in default.
    ScheduledChore task = new ScheduledChore("CredentialRefresher", server,
        conf.getInt(CREDENTIAL_REFRESH_PERIOD, CREDENTIAL_REFRESH_PERIOD_DEFAULT),
        conf.getLong(CREDENTIAL_REFRESH_PERIOD, CREDENTIAL_REFRESH_PERIOD_DEFAULT)) {
      @Override
      protected void chore() {
        try {
          refreshCredential();
        } catch (Exception e) {
          LOG.warn("Refresh credential failed with exception. ", e);
        }
      }
    };
    if (server.isAborted() || server.isStopped()) {
      throw new
        IllegalStateException("Try to initial credential cache but region server is not running");
    }
    server.getChoreService().scheduleChore(task);

    initializeDecryptor();
  }

  /**
   * Init decryptor asynchronously.
   */
  private void initializeDecryptor() {
    Thread decrytorInitThread = new Thread(new Runnable() {
      @Override
      public void run() {
        String msg = "Secret decryptor initialization failed. ";
        while (server.getConnection() == null) {
          // Wait for the cluster connection available.
          try {
            // 5 seconds is only for pass UT.
            Thread.sleep(5000);
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

  private void refreshCredential() throws IOException {
    LOG.info("Start refresh credential. ");
    ClusterConnection conn = this.server.getConnection();
    if (conn == null || conn.isAborted() || conn.isClosed()) {
      throw new IllegalStateException("The internal cluster connection is not open. ");
    }

    // batch update credentials.
    Map<String, CredentialEntry> tmpMap = new HashMap<>(credentialMap.size());

    for (String account : credentialMap.keySet()) {
      updateAccountCredential(account, tmpMap);
    }

    credentialMap.putAll(tmpMap);
  }

  private boolean isLoginUser(String account) {
    return account.equals(loginUser);
  }

  private void updateAccountCredential(String account, Map<String, CredentialEntry> map)
    throws IOException {
    CredentialEntry entry;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Start update credential of " + account);
    }

    Table table = getAuthTable();
    entry = SecretTableAccessor.getAccountCredential(getHashedUsername(account), table);
    if (entry.getPassword() != null) {
      entry.setPassword(decryptor.decryptSecret(entry.getPassword()));
    }

    // If the account is rs user, just update allowFallback mark directly on credentialMap
    // Otherwise, we may have problem when password of login user is changed.
    if (isLoginUser(account)) {
      credentialMap.get(account).setAllowFallback(entry.allowFallback);
    } else {
      if (entry.password == null) {
        // There is no such an account in our database.
        // Should remove this account if it exists in our cache.
        map.remove(account);
      } else {
        map.put(account, entry);
      }
    }
  }

  private String getHashedUsername(String username) {
    return Hex.encodeHexString(Encryption.hash256(username));
  }

  /**
   * Get ciphered password from cache.
   * Will visit secret table if the cache does not contain the target account.
   */
  public byte[] getPassword(String account) {
    if (account == null) {
      return new byte[0];
    }
    CredentialEntry entry = credentialMap.get(account);
    if (entry == null || entry.getPassword() == null) {
      try {
        updateAccountCredential(account, credentialMap);
      } catch (IOException e) {
        LOG.warn("Get password of account " + account +
          " from secret table. But encountered error:" + "\n", e);
      }
      if (!credentialMap.containsKey(account)) {
        // Update failed.
        return new byte[0];
      }
    }
    return credentialMap.get(account).getPassword();
  }

  /**
   * Get the allow fallback mark from cache.
   * Will visit secret table if the cache does not contain the target account.
   */
  public boolean getAllowFallback(String account) {
    if (account == null) {
      throw new IllegalArgumentException("An account must not be null. ");
    }
    CredentialEntry entry = credentialMap.get(account);
    if (entry == null) {
      try {
        updateAccountCredential(account, credentialMap);
      } catch (IOException e) {
        LOG.warn("Check allowFallback mark of account " + account +
          " from secret table. But encountered error:"+ "\n", e);
      }
      if (!credentialMap.containsKey(account)) {
        // If credential map does not contain this account,
        // there is no such a valid account in secret table.
        // But to guarantee some "illegal" account can access our service, we return true here.
        // TODO: Change this to return false when all illegal accounts are replaced.
        return true;
      }
    }
    return credentialMap.get(account).isAllowFallback();
  }

  /**
   *  Only used for {@link org.apache.hadoop.hbase.security.token.SystemTableBasedSecretManager}
   *  initialization.
   */
  public void insertLocalCredential(String key, byte[] password, boolean allowFallback) {
    CredentialEntry entry = credentialMap.get(key);
    if (entry == null) {
      entry = new CredentialEntry();
      credentialMap.putIfAbsent(key, entry);
    }
    entry.setPassword(password);
    entry.setAllowFallback(allowFallback);
  }

  @VisibleForTesting
  void setDecryptor(SecretDecryptor decryptor) {
    this.decryptor = decryptor;
  }
}
