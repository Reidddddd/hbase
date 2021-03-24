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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentMap;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.SaslException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Customized TSaslServerTransport for LDAP authentication.
 * The concurrent map is used to trace the underlying transport.
 * When the underlying transport is closed, the object will also be removed from the map
 * to let the GC release the memory.
 */
@InterfaceAudience.Private
public class TLdapTransport extends TSaslServerTransport {

  private static final Log LOG = LogFactory.getLog(TLdapTransport.class);
  private static final LdapSaslServer.LdapSaslServerFactory factory = new LdapSaslServer.LdapSaslServerFactory();

  private final String ldapUrl;
  private final String domainFormat;
  private final ConcurrentMap<TTransport, TLdapTransport> transportMap;

  public TLdapTransport(TTransport transport, String ldapUrl,
      ConcurrentMap<TTransport, TLdapTransport> map, String domainFormat) {
    super(transport);
    this.ldapUrl = ldapUrl;
    this.transportMap = map;
    this.domainFormat = domainFormat;
  }

  @Override
  protected void handleSaslStartMessage() throws TTransportException, SaslException {
    SaslResponse message = this.receiveSaslMessage();
    LOG.debug(String.format("Received start message with status %s", message.status));
    if (message.status != NegotiationStatus.START) {
      throw this.sendAndThrowMessage(NegotiationStatus.ERROR, "Expecting START status, received " + message.status);
    } else {
      String mechanismName = new String(message.payload, StandardCharsets.UTF_8);
      LOG.debug(String.format("Received mechanism name '%s'", mechanismName));
      setSaslServer(factory.createSaslServer("LDAP", null, null, null, new CallbackHandler() {
        @Override
        public void handle(Callback[] callbacks)
            throws UnsupportedCallbackException {
          String username = "";
          String password = "";
          for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
              NameCallback nameCallback = (NameCallback) callback;
              username = nameCallback.getName();
            } else if (callback instanceof PasswordCallback) {
              PasswordCallback passwordCallback = (PasswordCallback) callback;
              password = new String(passwordCallback.getPassword());
            } else if (callback instanceof AuthorizeCallback) {
              AuthorizeCallback authorizeCallback = (AuthorizeCallback) callback;
              LOG.debug("Authentication with username: " + username);
              authorizeCallback.setAuthorized(
                  LdapUtilities.authenticate(username, password, ldapUrl, domainFormat));
            } else {
              throw new UnsupportedCallbackException(callback);
            }
          }
        }
      }));
    }
  }

  @Override
  public void close() {
    super.close();
    transportMap.remove(this.underlyingTransport);
  }
}
