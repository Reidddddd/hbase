/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.security.authentication;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.secret.crypto.SecretCryptoType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authentication Manager of DIGEST Authentication.
 * It will check if the hbase:secret table already exists in master initialization phase.
 * The secret table will be created if it is not found.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SecretTableManager {
  private static final Logger LOG = LoggerFactory.getLogger(SecretTableManager.class);
  private static final TableName SECRET_TABLE_NAME = SecretTableAccessor.getSecretTableName();

  private final MasterServices masterServices;

  private boolean initialized;

  public boolean isInitialized() {
    return initialized;
  }

  public SecretTableManager(final MasterServices masterServices) {
    this.masterServices = masterServices;
  }

  public void start() throws IOException {
    LOG.info("Initializing DIGEST authentication support");
    // If the state is null, there is no secret table.
    // Create the userinfo table then.
    if (MetaTableAccessor.getTableState(masterServices.getConnection(),
        SECRET_TABLE_NAME) == null) {
      LOG.info("Table " + SECRET_TABLE_NAME + " not found. Creating...");
      long procId = createSecretTable();
      // If the return value is null, this means the procedure is already finished.
      while (masterServices.getMasterProcedureExecutor().getProcedure(procId) != null &&
          !masterServices.getMasterProcedureExecutor().getProcedure(procId).isFinished()) {
        try {
          // Wait for the secret table creation being finished.
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
      initializeSecretKeys();
    }

    initialized = true;
  }

  private long createSecretTable() throws IOException {
    HTableDescriptor desc = new HTableDescriptor(SECRET_TABLE_NAME);

    desc.addFamily(SecretTableAccessor.getSecretTableColumn());
    return masterServices.createSystemTable(desc);
  }

  private void initializeSecretKeys() throws IOException {
    Table table = masterServices.getConnection().getTable(SECRET_TABLE_NAME);
    SecureRandom rand = new SecureRandom(Bytes.toBytes(System.currentTimeMillis()));
    Arrays.stream(SecretCryptoType.values()).forEach((SecretCryptoType type) -> {
      try {
        // If there is no secret key for this algo, generate a new one.
        if (table.get(new Get(type.getHashedName())).isEmpty()) {
          byte[] newKey = new byte[type.getKeyLength()];
          rand.nextBytes(newKey);
          SecretTableAccessor.insertSecretKey(type.getHashedName(),
            Base64.getEncoder().encode(newKey), table);
        }
      } catch (IOException e) {
        LOG.error("Met exception when initialize secret keys.", e);
        masterServices.abort("Secret key initialization failed. ", e);
      }
    });
  }
}
