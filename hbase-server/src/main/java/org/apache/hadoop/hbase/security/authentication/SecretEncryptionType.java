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

import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public enum SecretEncryptionType {
  AES("AES", 1, "AES/CBC/PKCS5PADDING", "AES", 256 / 8),
  RC4("RC4", 2, "RC4", "RC4", 256 / 8),
  BLOW_FISH("BLOW_FISH", 3, "Blowfish", "Blowfish", 256 / 8),
  DES3("TripleDES", 4, "TripleDES/CBC/PKCS5Padding", "TripleDES", 192 / 8);

  final String name;
  final int num;
  final String cipherName;
  final String algoName;
  final int keyLength;

  SecretEncryptionType(String name, int num, String cipherName, String algoName, int keyLength) {
    this.name = name;
    this.num = num;
    this.cipherName = cipherName;
    this.algoName = algoName;
    this.keyLength = keyLength;
  }

  String getName() {
    return name;
  }

  String getCipherName() {
    return cipherName;
  }

  String getAlgoName() {
    return algoName;
  }

  byte[] getHashedName() {
    return Encryption.hash256Hex(this.name);
  }

  int getKeyLength() {
    return keyLength;
  }

  static SecretEncryptionType getType(int n) {
    for (SecretEncryptionType t : SecretEncryptionType.values()) {
      if (t.num == n) {
        return t;
      }
    }
    return null;
  }
}
