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
package org.apache.hadoop.hbase.secret.crypto;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class AbstractSecretCryptor {
  private final static Log LOG = LogFactory.getLog(AbstractSecretCryptor.class);

  protected final SecretCryptoSet secretCryptoSet;

  protected volatile boolean initialized = false;

  public AbstractSecretCryptor() {
    this.secretCryptoSet = new SecretCryptoSet();
  }

  public byte[] decryptSecret(byte[] secret) throws IOException {
    // Secret should be Base64 encoded.
    ByteBuffer buff = ByteBuffer.wrap(Base64.decodeBase64(secret));
    // Get the type number, we only use the last half byte to figure the algo out.
    int typeNum = buff.getInt() & 0x0F;
    SecretCryptoType type = SecretCryptoType.getType(typeNum);
    if (type == null) {
      // We should never go here.
      throw new IllegalArgumentIOException("The secret for decryption is invalid. ");
    }
    SecretCrypto decryption = secretCryptoSet.getCryptoFromType(type);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Select decryption " + decryption.getClass() + " from type: " + type.getName());
    }
    try {
      return decryption.decryptSecret(buff.array(), buff.position(), buff.remaining());
    } catch (GeneralSecurityException e) {
      throw new IOException(e.getMessage());
    }
  }

  public byte[] encryptSecret(byte[] secret) throws IOException {
    try {
      int typeNum = (ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE)
        % SecretCryptoType.values().length) + 1;
      SecretCrypto crypto = secretCryptoSet.getCryptoFromType(SecretCryptoType.getType(typeNum));
      return Base64.encodeBase64(crypto.encryptSecret(secret));
    } catch (GeneralSecurityException e) {
      throw new IOException(e.getMessage());
    }
  }

  public String encryptSecretToString(byte[] secret) throws IOException {
    return Bytes.toString(encryptSecret(secret));
  }

  public boolean isInitialized() {
    return initialized;
  }

  public void initCryptos(Object obj, String columnFamily, String passwordQualifier)
    throws IOException {
    initCryptos(obj, Bytes.toBytes(columnFamily), Bytes.toBytes(passwordQualifier));
  }

  public abstract void initCryptos(Object obj, byte[] cf, byte[] passwordQualifier)
    throws IOException;
}
