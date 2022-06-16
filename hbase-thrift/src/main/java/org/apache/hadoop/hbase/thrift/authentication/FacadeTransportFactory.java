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

import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.thrift.HBaseServiceHandler;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.yetus.audience.InterfaceAudience;
import org.ehcache.core.collections.ConcurrentWeakIdentityHashMap;

@InterfaceAudience.Private
public class FacadeTransportFactory extends TSaslServerTransport.Factory {
  private static final Log LOG = LogFactory.getLog(FacadeTransportFactory.class);
  private static final ConcurrentMap<TTransport, WeakReference<TSaslServerTransport>> transportMap =
      new ConcurrentWeakIdentityHashMap<>();

  private final HBaseServiceHandler hBaseServiceHandler;

  public FacadeTransportFactory(HBaseServiceHandler handler) {
    this.hBaseServiceHandler = handler;
  }

  @Override
  public TTransport getTransport(TTransport base) {
    WeakReference<TSaslServerTransport> ret = transportMap.get(base);
    if (ret == null || ret.get() == null) {
      TSaslServerTransport transport = new FacadeTransport(base, hBaseServiceHandler);
      ret = new WeakReference<>(transport);
      transportMap.put(base, ret);
      try {
        transport.open();
      } catch (TTransportException e) {
        if (LOG.isTraceEnabled()) {
          // This exception should not impact our server side.
          // Only set the address to trace the client here.
          if (base instanceof TSocket) {
            TSocket tSocket = (TSocket) base;
            LOG.trace("Failed to open sasl transport for " + tSocket.getSocket().getInetAddress()
              + "\n", e);
          }
        }
      }
    }
    return ret.get();
  }
}
