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
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * AES_CBC decryption.
 */
@InterfaceAudience.Private
public class AESSecretDecryption extends AbstractSecretDescryption {
  private static final int IV_LENGTH = 128 / 8;

  public AESSecretDecryption(SecretEncryptionType type, byte[] key)
      throws IllegalArgumentException {
    super(type, key);
  }

  @Override
  public byte[] decryptSecret(byte[] secret, int offset, int len)
      throws GeneralSecurityException {
    Cipher cipher = getCipher();
    IvParameterSpec vector =
        new IvParameterSpec(secret, offset, IV_LENGTH);
    cipher.init(Cipher.DECRYPT_MODE, key, vector);
    return cipher.doFinal(secret, offset + IV_LENGTH, len - IV_LENGTH);
  }
}
