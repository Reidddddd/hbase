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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

/**
 * Factory class for LDAP transport, use a concurrent map to trace the underlying transport
 * and the corresponding LDAP transport.
 */
@InterfaceAudience.Private
public class TLdapTransportFactory extends TTransportFactory {
  private static final Log LOG = LogFactory.getLog(TLdapTransportFactory.class);

  private final String ldapUrl;
  private final String domainFormat;
  private final ConcurrentMap<TTransport, TLdapTransport> transports = new ConcurrentHashMap<>();

  public TLdapTransportFactory(String ldapUrl, String domainFormat) {
    this.ldapUrl = ldapUrl;
    this.domainFormat = domainFormat;
  }

  public TTransport getTransport(TTransport base) {

    TLdapTransport transport;

    if (!transports.containsKey(base)) {
      transport = new TLdapTransport(base, ldapUrl, transports, domainFormat);
      try {
        transport.open();
      } catch (TTransportException e) {
        LOG.warn("Sasl transport for LDAP authentication is failed to open.", e);
        return null;
      }
      transports.put(base, transport);
    }

    transport = transports.get(base);
    return transport;
  }
}
