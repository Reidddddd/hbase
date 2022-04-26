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
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.AuthenticationFailedException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A util class to decrypt ciphertext of user password.
 * We need to read the secret key for each algorithm after the region server being initialized.
 * So that this class cannot be static.
 */
@InterfaceAudience.Private
public class SecretDecryptor {
  private static final Log LOG = LogFactory.getLog(SecretDecryptor.class);

  private final SecretDecryptionSet secretDecryptionSet;

  private volatile boolean initialized = false;

  public SecretDecryptor() {
    this.secretDecryptionSet = new SecretDecryptionSet();
  }

  public byte[] decryptSecret(byte[] secret) throws IOException {
    // Secret should be Base64 encoded.
    ByteBuffer buff = ByteBuffer.wrap(Base64.decodeBase64(secret));
    // Get the type number, we only use the last half byte to figure the algo out.
    int typeNum = buff.getInt() & 0x0F;
    SecretEncryptionType type = SecretEncryptionType.getType(typeNum);
    if (type == null) {
      // We should never go here.
      throw new AuthenticationFailedException("The internal credential is invalid. ");
    }
    SecretDecryption decryption = secretDecryptionSet.getDecryptionFromType(type);
    LOG.info("Select decryption " + decryption.getClass() + " from type: " + type.getName());
    try {
      return decryption.decryptSecret(buff.array(), buff.position(), buff.remaining());
    } catch (GeneralSecurityException e) {
      throw new IOException(e.getMessage());
    }
  }

  public void initDecryption(Table table, Server server) {
    synchronized (this) {
      if (isInitialized()) {
        return;
      }
      byte[] key = null;
      for (SecretEncryptionType type : SecretEncryptionType.values()) {
        try {
          byte[] keyBytes = SecretTableAccessor.getUserPassword(type.getHashedName(), table);
          key = Base64.decodeBase64(keyBytes);
          if (key == null || key.length != type.getKeyLength()) {
            throw new IllegalArgumentException("Failed to get valid secret key for algo: "
                + type.getName() + " from secret table. ");
          }
          secretDecryptionSet.initOneDecryption(type, key);
        } catch (Throwable t) {
          String msg = "Invalid secret key: " + Arrays.toString(key) +
              " detected for algo " + type.getName() + '\n';
          server.abort(msg, t);
        }
      }
      initialized = true;
    }
  }

  public boolean isInitialized() {
    return initialized;
  }
}
