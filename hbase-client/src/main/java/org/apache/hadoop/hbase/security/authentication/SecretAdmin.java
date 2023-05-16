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

import java.io.IOException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class to update user credential.
 * Not thread safe.
 */
@InterfaceAudience.Private
public final class SecretAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(SecretAdmin.class);

  private final Connection connection;
  private final SecretCryptor secretCryptor;
  private final Admin admin;
  private final String tableName;
  private final String columnFamily;
  private final String passwordQualifier;
  private final String fallbackQualifier;
  private final Table secretTable;

  private SecretAdmin(Connection connection, String tableName, String columnFamily,
    String passwordQualifier, String fallbackQualifier) throws IOException {
    this.connection = connection;
    this.tableName = tableName;
    this.columnFamily = columnFamily;
    this.passwordQualifier = passwordQualifier;
    this.fallbackQualifier = fallbackQualifier;
    this.secretCryptor = new SecretCryptor();
    this.admin = this.connection.getAdmin();
    this.secretTable = this.connection.getTable(TableName.valueOf(tableName));
    initCryptor();
  }

  /**
   * Set the password of an account.
   * Will insert a new row in hbase:secret if the given account does not exist.
   */
  public void setAccountPassword(String account, String password) throws IOException {
    checkSecretCryptor();
    byte[] hashedAccount = Bytes.toBytes(Hex.encodeHexString(Encryption.hash256(account)));
    byte[] secret = this.secretCryptor.encryptSecret(Bytes.toBytes(password));

    Put put = new Put(hashedAccount);
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(passwordQualifier), secret);

    secretTable.put(put);
  }

  /**
   * Remove an account.
   */
  public void removeAccount(String account) throws IOException {
    checkSecretCryptor();
    byte[] hashedAccount = Bytes.toBytes(Hex.encodeHexString(Encryption.hash256(account)));

    Delete delete = new Delete(hashedAccount);
    secretTable.delete(delete);
  }

  /**
   * Allow one account to fallback to simple authentication.
   */
  public void allowAccountFallback(String account) throws IOException {
    updateAccountAllowFallback(account, true);
  }

  /**
   * Disallow one account to fallback to simple authentication.
   */
  public void disallowAccountFallback(String account) throws IOException {
    updateAccountAllowFallback(account, false);
  }

  /**
   * Update the allowFallback of one account in hbase:secret.
   * Do nothing if the account does not exist.
   */
  private void updateAccountAllowFallback(String account, boolean allowFallback)
    throws IOException {
    checkSecretCryptor();
    byte[] hashedAccount = Bytes.toBytes(Hex.encodeHexString(Encryption.hash256(account)));
    byte[] fallback = Bytes.toBytes(allowFallback);

    Put put = new Put(hashedAccount);
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(fallbackQualifier), fallback);

    secretTable.put(put);
  }

  private void checkSecretCryptor() throws IOException {
    String errorMessage = null;
    if (!this.secretCryptor.isInitialized()) {
      errorMessage = "The secret cryptor is not initialized.";
    }
    if (errorMessage != null) {
      throw new HBaseIOException(errorMessage);
    }
  }

  /**
   * Init the SecretCryptor.
   */
  private void initCryptor()
    throws IOException {
    if (!admin.tableExists(TableName.valueOf(tableName))) {
      throw new HBaseIOException("Try to initialize cryptor the cluster is not initialized with "
        + "secret key.");
    }
    if (!this.secretCryptor.isInitialized()) {
      this.secretCryptor.initCryptos(secretTable, columnFamily, passwordQualifier);
    }
  }

  /**
   * Release the HTable explicitly.
   */
  public void close() throws IOException {
    this.admin.close();
    this.secretTable.close();
  }
}
