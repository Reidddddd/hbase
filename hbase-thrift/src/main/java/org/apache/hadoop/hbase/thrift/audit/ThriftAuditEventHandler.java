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
package org.apache.hadoop.hbase.thrift.audit;

import java.net.InetSocketAddress;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.thrift.HBaseServiceHandler;
import org.apache.hadoop.hbase.thrift.audit.ThriftAuditLogSyncer.ThriftConnectionInfo;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class ThriftAuditEventHandler implements TServerEventHandler {

  private static final Log LOG = LogFactory.getLog(ThriftAuditEventHandler.class);

  private final ThreadLocal<ThriftConnectionInfo> connectionInfo = new ThreadLocal<>();

  private final HBaseServiceHandler hbaseServiceHandler;

  private final ThriftAuditLogSyncer auditLogSyncer;

  public ThriftAuditEventHandler(HBaseServiceHandler hbaseServiceHandler,
    ThriftAuditLogSyncer auditLogSyncer) {
    this.hbaseServiceHandler = hbaseServiceHandler;
    this.auditLogSyncer = auditLogSyncer;
  }

  /**
   * Called before the server begins.
   */
  @Override
  public void preServe() {

  }

  /**
   * Called when a new client has connected and is about to being processing.
   */
  @Override
  public ServerContext createContext(TProtocol tProtocol, TProtocol tProtocol1) {
    TTransport transport = tProtocol.getTransport();
    ThriftConnectionInfo info = new ThriftConnectionInfo();
    info.setStartTime(System.currentTimeMillis());

    if (transport instanceof TSocket) {
      info.setRemoteAddress(
        (InetSocketAddress) ((TSocket) transport).getSocket().getRemoteSocketAddress());
    }
    connectionInfo.set(info);
    return null;
  }

  /**
   * Called when a client has finished request-handling to delete server context.
   */
  @Override
  public void deleteContext(ServerContext serverContext, TProtocol tProtocol,
      TProtocol tProtocol1) {
    ThriftConnectionInfo info = connectionInfo.get();
    if (info != null) {
      info.setEffectiveUser(this.hbaseServiceHandler.getEffectiveUser());
      info.setEndTime(System.currentTimeMillis());

      auditLogSyncer.logConnection(info);
    }
  }

  public void extractAddressFromTransport(TTransport transport) {
    if (transport instanceof TSocket) {
      ThriftConnectionInfo info = connectionInfo.get();
      info.setRemoteAddress(
        (InetSocketAddress) ((TSocket) transport).getSocket().getRemoteSocketAddress());
    }
  }

  /**
   * Called when a client is about to call the processor.
   */
  @Override
  public void processContext(ServerContext serverContext, TTransport tTransport,
    TTransport tTransport1) {

  }
}
