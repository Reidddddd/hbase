/**
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
package org.apache.hadoop.hbase.thrift.authentication;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.thrift.HBaseServiceHandler;
import org.apache.hadoop.security.SaslPlainServer;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class FacadeTransport extends TSaslServerTransport {
  private static final Log LOG = LogFactory.getLog(FacadeTransport.class);

  private final SaslPlainServer.SaslPlainServerFactory factory =
      new SaslPlainServer.SaslPlainServerFactory();
  private final HBaseServiceHandler hBaseServiceHandler;

  public FacadeTransport(TTransport transport, HBaseServiceHandler hBaseServiceHandler) {
    super(transport);
    this.hBaseServiceHandler = hBaseServiceHandler;
  }

  @Override
  protected void handleSaslStartMessage() throws TTransportException, SaslException {
    SaslResponse message = this.receiveSaslMessage();
    LOG.debug("Received start message with status " + message.status);
    if (message.status != NegotiationStatus.START) {
      throw this.sendAndThrowMessage(NegotiationStatus.ERROR,
          "Expecting START status, received " + message.status);
    } else {
      SaslServer saslServer = factory.createSaslServer("PLAIN",
          null, null, null, new PlainCallbackHandler());
      this.setSaslServer(saslServer);
    }
  }

  private class PlainCallbackHandler implements CallbackHandler {
    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      AuthorizeCallback ac = null;

      for (Callback callback : callbacks) {
        if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else {
          throw new UnsupportedCallbackException(callback,
              "Unrecognized SASL PLAIN Callback");
        }
      }
      String userName = null;
      String password = null;
      if (nc != null) {
        userName = nc.getName();
      }
      if (pc != null) {
        password = new String(pc.getPassword());
      }
      if (ac != null) {
        if (userName != null && !userName.isEmpty() &&
            password != null && !password.isEmpty()) {
          ac.setAuthorized(true);
          ac.setAuthorizedID(userName);
          hBaseServiceHandler.setEffectiveUser(userName);
          hBaseServiceHandler.setEffectivePassword(password);
        } else {
          ac.setAuthorized(false);
        }
      }
    }
  }
}
