/**
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
package org.apache.hadoop.hbase.thrift.ldap;

import java.security.Provider;
import java.util.Map;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * The customized {@link SaslServer} for LDAP authentication.
 * It acquires the username and password from SASL PLAIN mechanism and use LDAP to authenticate.
 */
@InterfaceAudience.Private
public class LdapSaslServer implements SaslServer {

  private CallbackHandler callbackHandler;
  private boolean completed;
  private String authz;

  LdapSaslServer(CallbackHandler callback) {
    this.callbackHandler = callback;
  }

  public String getMechanismName() {
    return "LDAP";
  }

  public byte[] evaluateResponse(byte[] response) throws SaslException {
    if (this.completed) {
      throw new IllegalStateException("LDAP authentication has completed");
    } else if (response == null) {
      throw new IllegalArgumentException("Received null response");
    } else {
      try {
        String payload;
        try {
          payload = new String(response, "UTF-8");
        } catch (Exception e) {
          throw new IllegalArgumentException("Received corrupt response", e);
        }

        // Deserialize the authentication token, username and password
        String[] parts = payload.split("\u0000", 3);
        if (parts.length != 3) {
          throw new IllegalArgumentException("Received corrupt response");
        }

        // If not use authentication token, part[0] will be assigned as username
        if (parts[0].isEmpty()) {
          parts[0] = parts[1];
        }

        NameCallback nameCallback = new NameCallback("SASL LDAP");
        nameCallback.setName(parts[1]);
        PasswordCallback passwordCallback = new PasswordCallback("SASL LDAP", false);
        passwordCallback.setPassword(parts[2].toCharArray());
        AuthorizeCallback authorizeCallback = new AuthorizeCallback(parts[1], parts[0]);
        this.callbackHandler.handle(new Callback[] { nameCallback, passwordCallback, authorizeCallback });
        if (authorizeCallback.isAuthorized()) {
          this.authz = authorizeCallback.getAuthenticationID();
        } else {
          throw new SaslException("LDAP auth failed, please input valid username and password.");
        }
      } catch (Exception e) {
        throw new SaslException("LDAP auth failed: " + e.toString(), e);
      } finally {
        this.completed = true;
      }

      return null;
    }
  }

  private void throwIfNotComplete() {
    if (!this.completed) {
      throw new IllegalStateException("LDAP authentication not completed");
    }
  }

  public boolean isComplete() {
    return this.completed;
  }

  public String getAuthorizationID() {
    this.throwIfNotComplete();
    return this.authz;
  }

  public Object getNegotiatedProperty(String propName) {
    this.throwIfNotComplete();
    return "javax.security.sasl.qop".equals(propName) ? "auth" : null;
  }

  public byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException {
    this.throwIfNotComplete();
    throw new IllegalStateException("LDAP supports neither integrity nor privacy");
  }

  public byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException {
    this.throwIfNotComplete();
    throw new IllegalStateException("LDAP supports neither integrity nor privacy");
  }

  public void dispose() throws SaslException {
    this.callbackHandler = null;
    this.authz = null;
  }

  public static class LdapSaslServerFactory implements SaslServerFactory {
    public LdapSaslServerFactory() {
    }

    public SaslServer createSaslServer(String mechanism, String protocol,
        String serverName, Map<String, ?> props, CallbackHandler callbackHandler) {
      return "LDAP".equals(mechanism) ? new LdapSaslServer(callbackHandler) : null;
    }

    public String[] getMechanismNames(Map<String, ?> props) {
      return new String[] { "LDAP" };
    }
  }

  public static class SecurityProvider extends Provider {
    public SecurityProvider() {
      super("LdapSaslServer", 1.0D, "SASL LDAP Authentication Server");
      this.put("SaslServerFactory.LDAP", LdapSaslServer.LdapSaslServerFactory.class.getName());
    }
  }
}
