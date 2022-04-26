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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Return a global unique decryption object.
 */
@InterfaceAudience.Private
public class SecretDecryptionSet {

  private final Map<SecretEncryptionType, SecretDecryption> decryptionMap =
      new ConcurrentHashMap<>(SecretEncryptionType.values().length);

  SecretDecryption getDecryptionFromType(SecretEncryptionType type) {
    return decryptionMap.get(type);
  }

  void initOneDecryption(SecretEncryptionType type, byte[] key)
      throws IllegalArgumentException {
    SecretDecryption res = decryptionMap.get(type);
    if (res == null) {
      switch (type) {
        case AES:
          res = new AESSecretDecryption(type, key);
          break;
        case RC4:
          res = new RC4SecretDecryption(type, key);
          break;
        case BLOW_FISH:
          res = new BFSecretDecryption(type, key);
          break;
        case DES3:
          res = new DES3SecretDescryption(type, key);
          break;
        default:
          throw new IllegalArgumentException("Invalid encryption type "
              + type.getName() + " detected");
      }
      decryptionMap.putIfAbsent(type, res);
    }
  }
}
