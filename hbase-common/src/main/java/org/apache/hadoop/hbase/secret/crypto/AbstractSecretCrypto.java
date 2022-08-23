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
import java.security.GeneralSecurityException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ThreadLocalRandom;
import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.hbase.io.ByteArrayOutputStream;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Abstract class for different decryption algorithms.
 */
@InterfaceAudience.Private
public abstract class AbstractSecretCrypto implements SecretCrypto {

  // Package javax.crypto.* is not thread safe.
  private final ThreadLocal<Cipher> cipher;

  protected final SecretCryptoType type;
  protected final Key key;

  public AbstractSecretCrypto(SecretCryptoType type, byte[] key) throws IllegalArgumentException {
    this.type = type;
    this.cipher = new ThreadLocal<>();
    this.key = new SecretKeySpec(key, type.getAlgoName());
  }

  @Override
  public byte[] decryptSecret(byte[] secret, int offset, int len) throws GeneralSecurityException {
    Cipher cipher = getCipher();
    int ivLength = getIvLength();
    if (ivLength > 0) {
      IvParameterSpec vector = new IvParameterSpec(secret, offset, ivLength);
      cipher.init(Cipher.DECRYPT_MODE, key, vector);
      return cipher.doFinal(secret, offset + ivLength, len - ivLength);
    } else {
      cipher.init(Cipher.DECRYPT_MODE, key);
      return cipher.doFinal(secret, offset, len);
    }
  }

  @Override
  public byte[] encryptSecret(byte[] secret) throws IOException, GeneralSecurityException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream(32);
    int headInt = ThreadLocalRandom.current().nextInt();
    headInt = headInt << 4 | this.type.getNum();
    stream.writeInt(headInt);

    Cipher cipher = getCipher();
    int ivLength = getIvLength();
    if (ivLength > 0) {
      byte[] iv = new byte[ivLength];
      ThreadLocalRandom.current().nextBytes(iv);
      IvParameterSpec vector = new IvParameterSpec(iv);
      cipher.init(Cipher.ENCRYPT_MODE, key, vector);
      stream.write(iv);
    } else {
      cipher.init(Cipher.ENCRYPT_MODE, key);
    }

    stream.write(cipher.doFinal(secret));
    return stream.toByteArray();
  }

  protected abstract int getIvLength();

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
