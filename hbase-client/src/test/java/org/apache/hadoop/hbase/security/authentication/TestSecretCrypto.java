/*
 *
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

import static org.junit.Assert.assertEquals;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.secret.crypto.AESSecretCrypto;
import org.apache.hadoop.hbase.secret.crypto.BFSecretCrypto;
import org.apache.hadoop.hbase.secret.crypto.DES3SecretCrypto;
import org.apache.hadoop.hbase.secret.crypto.RC4SecretCrypto;
import org.apache.hadoop.hbase.secret.crypto.SecretCrypto;
import org.apache.hadoop.hbase.secret.crypto.SecretCryptoType;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestSecretCrypto {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSecretCrypto.class);
  private static final String VALID_USER_NAME = "testuser";
  private static final String VALID_USER_PASSWORD = "password0";

  @Test
  public void testAESCrypto() throws GeneralSecurityException {
    SecureRandom rand = new SecureRandom(Bytes.toBytes(System.currentTimeMillis()));
    SecretCryptoType type = SecretCryptoType.AES;
    byte[] key = new byte[type.getKeyLength()];
    rand.nextBytes(key);

    byte[] iv = new byte[128 / 8];
    rand.nextBytes(iv);

    Cipher cipher = Cipher.getInstance(type.getCipherName());
    SecretKeySpec k = new SecretKeySpec(key, type.getAlgoName());
    IvParameterSpec vector = new IvParameterSpec(iv);
    cipher.init(Cipher.ENCRYPT_MODE, k, vector);

    byte[] plainText = Bytes.toBytes(VALID_USER_PASSWORD);
    byte[] cipherText = cipher.doFinal(plainText);

    ByteBuffer buffer = ByteBuffer.allocate(iv.length + cipherText.length);
    buffer.put(iv);
    buffer.put(cipherText);
    byte[] secret = buffer.array();

    SecretCrypto aes = new AESSecretCrypto(type, key);
    byte[] original = aes.decryptSecret(secret, 0, secret.length);
    assertEquals(0, Bytes.compareTo(original, plainText));
  }

  @Test
  public void TestRC4SecretCrypto() throws GeneralSecurityException {
    SecureRandom rand = new SecureRandom(Bytes.toBytes(System.currentTimeMillis()));
    SecretCryptoType type = SecretCryptoType.RC4;
    byte[] key = new byte[type.getKeyLength()];
    rand.nextBytes(key);

    Cipher cipher = Cipher.getInstance(type.getCipherName());
    SecretKeySpec k = new SecretKeySpec(key, type.getAlgoName());
    cipher.init(Cipher.ENCRYPT_MODE, k);

    byte[] plainText = Bytes.toBytes(VALID_USER_PASSWORD);
    byte[] cipherText = cipher.doFinal(plainText);

    SecretCrypto rc4 = new RC4SecretCrypto(type, key);
    byte[] original = rc4.decryptSecret(cipherText, 0, cipherText.length);
    assertEquals(0, Bytes.compareTo(original, plainText));
  }

  @Test
  public void TestBFSecretCrypto() throws GeneralSecurityException {
    SecureRandom rand = new SecureRandom(Bytes.toBytes(System.currentTimeMillis()));
    SecretCryptoType type = SecretCryptoType.BLOW_FISH;
    byte[] key = new byte[type.getKeyLength()];
    rand.nextBytes(key);

    Cipher cipher = Cipher.getInstance(type.getCipherName());
    SecretKeySpec k = new SecretKeySpec(key, type.getAlgoName());
    cipher.init(Cipher.ENCRYPT_MODE, k);

    byte[] plainText = Bytes.toBytes(VALID_USER_PASSWORD);
    byte[] cipherText = cipher.doFinal(plainText);

    SecretCrypto bf = new BFSecretCrypto(type, key);
    byte[] original = bf.decryptSecret(cipherText, 0, cipherText.length);
    assertEquals(0, Bytes.compareTo(original, plainText));
  }

  @Test
  public void testDES3Crypto() throws GeneralSecurityException {
    SecureRandom rand = new SecureRandom(Bytes.toBytes(System.currentTimeMillis()));
    SecretCryptoType type = SecretCryptoType.DES3;
    byte[] key = new byte[type.getKeyLength()];
    rand.nextBytes(key);

    byte[] iv = new byte[64 / 8];
    rand.nextBytes(iv);

    Cipher cipher = Cipher.getInstance(type.getCipherName());
    SecretKeySpec k = new SecretKeySpec(key, type.getAlgoName());
    IvParameterSpec vector = new IvParameterSpec(iv);
    cipher.init(Cipher.ENCRYPT_MODE, k, vector);

    byte[] plainText = Bytes.toBytes(VALID_USER_PASSWORD);
    byte[] cipherText = cipher.doFinal(plainText);

    ByteBuffer buffer = ByteBuffer.allocate(iv.length + cipherText.length);
    buffer.put(iv);
    buffer.put(cipherText);
    byte[] secret = buffer.array();

    SecretCrypto idea = new DES3SecretCrypto(type, key);
    byte[] original = idea.decryptSecret(secret, 0, secret.length);
    assertEquals(0, Bytes.compareTo(original, plainText));
  }

  @Test
  public void testAES() throws GeneralSecurityException {
    String input = "AAAAABB0XTDsnk2tKjK19WWAFFLlMWOfVV2Hg5bkX/TO79nS";
    String key = "YOIcO7ntndlBtPVssvufgBQssS5M0/biccD4835aQkM=";
    byte[] data = Base64.getDecoder().decode(input);
    byte[] secretKey = Base64.getDecoder().decode(key);
    AESSecretCrypto decryption = new AESSecretCrypto(SecretCryptoType.AES, secretKey);
    byte[] plain = decryption.decryptSecret(data, 4, data.length - 4);
    assertEquals(0, Bytes.compareTo(plain, Bytes.toBytes("123456")));
  }
}
