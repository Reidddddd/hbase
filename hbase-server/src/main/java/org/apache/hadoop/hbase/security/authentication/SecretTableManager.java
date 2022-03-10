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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Authentication Manager of DIGEST Authentication.
 * It will check if the hbase:secret table already exists in master initialization phase.
 * The secret table will be created if it is not found.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SecretTableManager {
  private static final Log LOG = LogFactory.getLog(SecretTableManager.class);

  public static final TableName SECRET_TABLE_NAME = SecretTableAccessor.getSecretTableName();

  private final MasterServices masterServices;

  public boolean isInitialized() {
    return initialized;
  }

  private boolean initialized;

  public SecretTableManager(final MasterServices masterServices) {
    this.masterServices = masterServices;
  }

  public void start() throws IOException {
    LOG.info("Initializing DIGEST authentication support");
    // Create the userinfo table if missing
    if (!MetaTableAccessor.tableExists(masterServices.getConnection(), SECRET_TABLE_NAME)) {
      LOG.info("Table " + SECRET_TABLE_NAME + " not found. Creating...");
      createSecretTable();
    }

    initialized = true;
  }

  private void createSecretTable() throws IOException {
    HTableDescriptor desc = new HTableDescriptor(SECRET_TABLE_NAME);

    desc.addFamily(SecretTableAccessor.getSecretTableColumn());
    masterServices.createSystemTable(desc);
  }
}
