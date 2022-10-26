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

import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestSecretCryptor {
  private static final Log LOG = LogFactory.getLog(TestSecretCryptor.class);
  private static final String PLAIN_TEXT = "plainText";

  @Test
  public void testCorrectness() throws IOException {
    TestCryptor cryptor = new TestCryptor();
    cryptor.initCryptos(null, (byte[])null, (byte[])null);

    // Here we repeat 10 times to try to cover all 4 types of encryption.
    for (int i = 0; i < 10; i++) {
      byte[] secret = cryptor.encryptSecret(Bytes.toBytes(PLAIN_TEXT));
      byte[] newPlainText = cryptor.decryptSecret(secret);
      assertEquals(0, Bytes.compareTo(newPlainText, Bytes.toBytes(PLAIN_TEXT)));
    }
  }

  static class TestCryptor extends AbstractSecretCryptor {

    @Override
    public void initCryptos(Object obj, byte[] cf, byte[] passwordQualifier) {
      SecretCryptoType[] types = SecretCryptoType.values();
      for (SecretCryptoType type : types) {
        byte[] key = new byte[type.getKeyLength()];
        ThreadLocalRandom.current().nextBytes(key);
        this.secretCryptoSet.initOneDecryption(type, key);
        initialized = true;
      }
    }
  }

}
