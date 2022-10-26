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
import java.util.Arrays;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.secret.crypto.AbstractSecretCryptor;
import org.apache.hadoop.hbase.secret.crypto.SecretCryptoType;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A util class to decrypt ciphertext of user password.
 * We need to read the secret key for each algorithm after the region server being initialized.
 * So that this class cannot be static.
 */
@InterfaceAudience.Private
public class SecretCryptor extends AbstractSecretCryptor {
  private static final Log LOG = LogFactory.getLog(SecretCryptor.class);

  public SecretCryptor() {
    super();
  }

  @Override
  public void initCryptos(Object obj, byte[] cf, byte[] passwordQualifier) throws IOException {
    if (!(obj instanceof Table)) {
      throw new IllegalArgumentIOException("There should be a Table object to initialize server "
        + "side decryptor, but got: " + obj.getClass().getName());
    }
    Table table = (Table) obj;
    synchronized (this) {
      if (isInitialized()) {
        return;
      }
      byte[] key = null;
      for (SecretCryptoType type : SecretCryptoType.values()) {
        try {
          Get get = new Get(type.getHashedName());
          get.addColumn(cf, passwordQualifier);
          byte[] keyBytes = table.get(get).value();
          key = Base64.decodeBase64(keyBytes);
          if (key == null || key.length != type.getKeyLength()) {
            throw new IllegalArgumentException("Failed to get valid secret key for algo: "
              + type.getName() + " from secret table. ");
          }
          secretCryptoSet.initOneDecryption(type, key);
        } catch (Throwable t) {
          String msg = "Invalid secret key: " + Arrays.toString(key) +
            " detected for algo " + type.getName() + '\n';
          LOG.warn(msg, t);
          throw t;
        }
      }
      initialized = true;
    }
  }
}
