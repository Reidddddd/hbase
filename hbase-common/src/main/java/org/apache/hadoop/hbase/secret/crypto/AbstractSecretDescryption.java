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

import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Abstract class for different decryption algorithms.
 */
@InterfaceAudience.Private
public abstract class AbstractSecretDescryption implements SecretDecryption {

  // Package javax.crypto.* is not thread safe.
  private final ThreadLocal<Cipher> cipher;
  private final SecretEncryptionType type;

  protected final Key key;

  public AbstractSecretDescryption(SecretEncryptionType type, byte[] key)
      throws IllegalArgumentException {
    this.type = type;
    this.cipher = new ThreadLocal<>();
    this.key = new SecretKeySpec(key, type.getAlgoName());
  }

  @Override
  public abstract byte[] decryptSecret(byte[] secret, int offset, int len)
      throws GeneralSecurityException;

  protected Cipher getCipher()
      throws NoSuchPaddingException, NoSuchAlgorithmException {
    Cipher c = cipher.get();
    if (c == null) {
      c = Cipher.getInstance(type.getCipherName());
      cipher.set(c);
    }
    return c;
  }
}
