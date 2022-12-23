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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Util class provides method to access secret table.
 * All method should pass a table object.
 * Only table hbase:secret allowed.
 */
@InterfaceAudience.Private
public final class SecretTableAccessor {
  private static final Log LOG = LogFactory.getLog(SecretTableAccessor.class);

  private static final String SECRET_TABLE_NAME = "hbase:secret";
  private static final String SECRET_FAMILY_KEY = "i";                 // First letter in "info"
  private static final String SECRET_COLUMN_PASSWORD_KEY = "p";        // First letter in "password"
  private static final String SECRET_COLUMN_ALLOW_FALLBACK_KEY = "a";  // First letter in "allow"

  private SecretTableAccessor() {
  }

  /**
   * Method to check if a given username and password is valid.
   * @return if the authentication is successful.
   */
  public static boolean authenticate(String username, String password, Table table)
      throws IOException {
    byte[] secretPassword = getUserPassword(Bytes.toBytes(username), table);
    if (secretPassword == null) {
      return false;
    }

    return Bytes.equals(secretPassword, Bytes.toBytes(password));
  }

  /**
   * Return the password of the given user in byte array.
   * @return a byte array of the password.
   */
  public static byte[] getUserPassword(byte[] username, Table table)
      throws IOException {
    sanityCheck(table);
    Result res = table.get(new Get(username).addColumn(Bytes.toBytes(SECRET_FAMILY_KEY),
      Bytes.toBytes(SECRET_COLUMN_PASSWORD_KEY)));
    if (res.isEmpty()) {
      return null;
    }

    return res.value();
  }

  /**
   * Method to check if a user is allowed to SIMPLE authentication.
   * @return the result of the check.
   */
  public static boolean allowFallback(String username, Table table)
      throws IOException {
    if (username == null || username.isEmpty()) {
      LOG.warn("There is user with no name to fallback to simple authentication.");
      return false;
    }
    return allowFallback(Bytes.toBytes(username), table);
  }

  public static boolean allowFallback(byte[] username, Table table) throws IOException {
    sanityCheck(table);

    Result res = table.get(new Get(username).addColumn(Bytes.toBytes(SECRET_FAMILY_KEY),
      Bytes.toBytes(SECRET_COLUMN_ALLOW_FALLBACK_KEY)));
    if (res.isEmpty()) {
      return true;
    }

    byte[] resValue = res.value();
    if (resValue == null || resValue.length == 0) {
      return true;
    }
    return Bytes.toBoolean(resValue);
  }

  /**
   * Method to get the whole credential information of a specific account.
   * @return a {@link CredentialEntry} object.
   */
  public static CredentialEntry getAccountCredential(String account, Table table)
    throws IOException {
    sanityCheck(table);

    Result res = table.get(new Get(Bytes.toBytes(account)));
    CredentialEntry entry = new CredentialEntry();

    byte[] passwordBytes = res.getValue(Bytes.toBytes(SECRET_FAMILY_KEY),
      Bytes.toBytes(SECRET_COLUMN_PASSWORD_KEY));
    entry.setPassword(passwordBytes == null ? new byte[0] : passwordBytes);

    byte[] allowFallback = res.getValue(Bytes.toBytes(SECRET_FAMILY_KEY),
      Bytes.toBytes(SECRET_COLUMN_ALLOW_FALLBACK_KEY));
    entry.setAllowFallback(allowFallback == null || allowFallback.length == 0 ? true :
      Bytes.toBoolean(allowFallback));
    return entry;
  }

  private static void sanityCheck(Table table) {
    if (table == null) {
      throw new IllegalArgumentException("The global system table is not initialized. "
          + "Check the regionserver's status to fix it.");
    }
    if (!table.getName().getNameAsString().equals(SECRET_TABLE_NAME)) {
      throw new IllegalArgumentException("SecretAccessor only accepts table hbase:secret."
          + " But got table with name : " + table.getName().getNameAsString());
    }
  }

  /**
   * Util function for secret table initialization.
   * @return A table name of secret table.
   */
  public static TableName getSecretTableName() {
    return TableName.valueOf(SECRET_TABLE_NAME);
  }

  /**
   * Util function for secret table initialization.
   * @return A column descriptor of secret table.
   */
  public static HColumnDescriptor getSecretTableColumn() {
    return new HColumnDescriptor(SECRET_FAMILY_KEY).setMaxVersions(1).setInMemory(true)
        .setBlockCacheEnabled(true).setBlocksize(8 * 1024).setBloomFilterType(BloomType.NONE)
        .setScope(HConstants.REPLICATION_SCOPE_LOCAL);
  }

  /**
   * Util function only used for secret table initialization.
   */
  @VisibleForTesting
  public static void insertSecretKey(byte[] algoName, byte[] key, Table table) throws IOException {
    Put put = new Put(algoName);
    put.addColumn(Bytes.toBytes(SECRET_FAMILY_KEY), Bytes.toBytes(SECRET_COLUMN_PASSWORD_KEY), key);
    table.put(put);
  }

  /**
   * Return the cf key in byte[].
   */
  public static byte[] getSecretTableColumnFamily() {
    return Bytes.toBytes(SECRET_FAMILY_KEY);
  }

  /**
   * Return the password qualifier in byte[].
   */
  public static byte[] getSecretTablePasswordQualifier() {
    return Bytes.toBytes(SECRET_COLUMN_PASSWORD_KEY);
  }

  /**
   * return the allow fallback key in byte[].
   */
  public static byte[] getSecretTableAllowFallbackQualifier() {
    return Bytes.toBytes(SECRET_COLUMN_ALLOW_FALLBACK_KEY);
  }
}
