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
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.logging.Log;
import org.apache.hadoop.hbase.util.AbstractAuditLogSyncer;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * An AuditLogSyncer to print thrift audits.
 */
@InterfaceAudience.Private
public class ThriftAuditLogSyncer extends AbstractAuditLogSyncer {

  public static final String CONF_AUDIT = "hbase.thrift.security.audit";
  public static final String CONF_AUDIT_FLUSH_INTERVAL =
    "hbase.thrift.security.audit.flush.interval";

  public static final boolean DEFAULT_AUDIT = true;
  public static final Long DEFAULT_AUDIT_FLUSH_INTERVAL = 1000L;

  // Used in audit log when we cannot get the ip:port of client.
  public static final String ADDRESS_PLACEHOLDER = "default";

  private final LinkedBlockingQueue<ThriftConnectionInfo> events = new LinkedBlockingQueue<>();

  public ThriftAuditLogSyncer(long flushInterval, Log LOG) {
    super(flushInterval, LOG);
  };

  @Override
  protected void auditSync() {
    // Do not use take here, as we have synchronized outside.
    ThriftConnectionInfo connectionInfo = events.poll();
    if (connectionInfo != null) {
      AUDITLOG.info(connectionInfo.toString());
    }
  }

  void logConnection(ThriftConnectionInfo connectionInfo) {
    events.offer(connectionInfo);
  }

  static class ThriftConnectionInfo {
    private String effectiveUser;

    private InetSocketAddress remoteAddress;

    private long startTime;

    private long endTime;

    private final String USER = "user:";

    private final String IP = "ip:";

    private final String SERVE_TIME = "served time:";


    ThriftConnectionInfo() {
      this.effectiveUser = null;
      this.remoteAddress = null;
      this.startTime = 0L;
      this.endTime = 0L;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      long servedTime = endTime - startTime;
      sb.append(USER).append(effectiveUser).append('\t');

      if (remoteAddress == null) {
        sb.append(IP).append(ADDRESS_PLACEHOLDER).append('\t');
      } else {
        sb.append(IP).append(remoteAddress.getHostString()).append(':')
          .append(remoteAddress.getPort()).append('\t');
      }

      sb.append(SERVE_TIME).append(servedTime).append(" ms").append('\t');

      return sb.toString();
    }

    public void setEffectiveUser(String user) {
      this.effectiveUser = user;
    }

    public void setStartTime(long time) {
      this.startTime = time;
    }

    public void setEndTime(long time) {
      this.endTime = time;
    }

    public void setRemoteAddress(InetSocketAddress address) {
      this.remoteAddress = address;
    }
  }

}
