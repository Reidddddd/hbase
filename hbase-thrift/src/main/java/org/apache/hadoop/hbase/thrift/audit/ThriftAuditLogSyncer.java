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
  public static final String ADDRESS_PLACEHOLDER = "unknown";

  private final LinkedBlockingQueue<ThriftAuditEntry> events = new LinkedBlockingQueue<>();

  private final ThreadLocal<String> ipPort = new ThreadLocal<>();

  public ThriftAuditLogSyncer(long flushInterval, Log LOG) {
    super(flushInterval, LOG);
  };

  @Override
  protected void auditSync() {
    ThriftAuditEntry logEntry = events.poll();
    if (logEntry != null) {
      AUDITLOG.info(logEntry.toString());
    }
  }

  public void logRequestProcess(String tableName, String methodName, String user, long serveTime) {
    AUDITLOG.info(new ThriftAuditEntry().setServeTime(serveTime)
      .setEffectiveUser(user).setTableName(tableName).setMethod(methodName)
      .setRemoteAddress(ipPort.get()).toString());
  }

  void setIpPort(String ipPort) {
    this.ipPort.set(ipPort);
  }

  static class ThriftAuditEntry {
    private static final String USER = "user: ";
    private static final String IP = "ip: ";
    private static final String METHOD = "method: ";
    private static final String TABLE = "table: ";
    private static final String SERVE_TIME = "served time: ";

    private String effectiveUser;
    private String methodName;
    private String tableName;
    private String remoteAddress;
    private long serveTime;

    ThriftAuditEntry() {
      this.effectiveUser = "";
      this.remoteAddress = null;
      this.serveTime = 0L;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(USER).append(effectiveUser).append('\t');

      if (remoteAddress == null) {
        sb.append(IP).append(ADDRESS_PLACEHOLDER).append('\t');
      } else {
        sb.append(IP).append(remoteAddress).append('\t');
      }

      sb.append(TABLE).append(tableName).append('\t');
      sb.append(METHOD).append(methodName).append('\t');
      sb.append(SERVE_TIME).append(serveTime).append(" ms").append('\t');

      return sb.toString();
    }

    public ThriftAuditEntry setEffectiveUser(String user) {
      this.effectiveUser = user;
      return this;
    }

    public ThriftAuditEntry setServeTime(long serveTime) {
      this.serveTime = serveTime;
      return this;
    }

    public ThriftAuditEntry setRemoteAddress(String address) {
      this.remoteAddress = address;
      return this;
    }

    public ThriftAuditEntry setMethod(String methodName) {
      this.methodName = methodName;
      return this;
    }

    public ThriftAuditEntry setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }
  }

}
