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
package org.apache.hadoop.hbase.security.token;

import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Manages an internal list of secret keys used to sign new authentication
 * tokens as they are generated, and to valid existing tokens used for
 * authentication.
 * Replace the ZK implementation in {@link AuthenticationTokenSecretManager}
 * with the mechanism based on system table 'hbase:secret'.
 */
@InterfaceAudience.Private
public class AuthenticationTokenSecretManagerV2
    extends SecretManager<AuthenticationTokenIdentifier> {

  @Override
  protected byte[] createPassword(AuthenticationTokenIdentifier authenticationTokenIdentifier) {
    return new byte[0];
  }

  @Override
  public byte[] retrievePassword(AuthenticationTokenIdentifier authenticationTokenIdentifier)
      throws InvalidToken {
    return new byte[0];
  }

  @Override
  public AuthenticationTokenIdentifier createIdentifier() {
    return null;
  }
}
