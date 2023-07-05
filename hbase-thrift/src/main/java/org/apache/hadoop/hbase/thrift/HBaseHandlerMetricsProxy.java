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

package org.apache.hadoop.hbase.thrift;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.thrift.audit.ThriftAuditLogSyncer;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts a Hbase.Iface using InvocationHandler so that it reports process
 * time of each call to ThriftMetrics.
 */
@InterfaceAudience.Private
public final class HBaseHandlerMetricsProxy implements InvocationHandler {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseHandlerMetricsProxy.class);

  private static final String UNKNOWN = "unknown";

  private final Object handler;
  private final ThriftMetrics metrics;
  private final ThriftAuditLogSyncer auditLogSyncer;

  public static Hbase.Iface newInstance(Hbase.Iface handler,
                                        ThriftMetrics metrics,
                                        Configuration conf,ThriftAuditLogSyncer auditLogSyncer) {
    return (Hbase.Iface) Proxy.newProxyInstance(
        handler.getClass().getClassLoader(),
        new Class[]{Hbase.Iface.class},
        new HBaseHandlerMetricsProxy(handler, metrics, conf, auditLogSyncer));
  }

  // for thrift 2
  public static THBaseService.Iface newInstance(THBaseService.Iface handler,
      ThriftMetrics metrics, Configuration conf, ThriftAuditLogSyncer auditLogSyncer) {
    return (THBaseService.Iface) Proxy.newProxyInstance(
        handler.getClass().getClassLoader(),
        new Class[]{THBaseService.Iface.class},
        new HBaseHandlerMetricsProxy(handler, metrics, conf, auditLogSyncer));
  }

  private HBaseHandlerMetricsProxy(Object handler, ThriftMetrics metrics, Configuration conf,
    ThriftAuditLogSyncer auditLogSyncer) {
    this.handler = handler;
    this.metrics = metrics;
    this.auditLogSyncer = auditLogSyncer;
  }

  @Override
  public Object invoke(Object proxy, Method m, Object[] args)
      throws Throwable {
    Object result;
    long start = now();

    String effectiveUser = handler instanceof HBaseServiceHandler ?
      ((HBaseServiceHandler) handler).getEffectiveUser() : UNKNOWN;
    // In all table level ops, the first parameter should be a ByteBuffer of TableName.
    String tableName = args != null ? args[0] instanceof ByteBuffer ?
      Bytes.toString(Bytes.getBytes((ByteBuffer) args[0])) : UNKNOWN : UNKNOWN;

    try {
      result = m.invoke(handler, args);
    } catch (InvocationTargetException e) {
      metrics.exception(e.getCause());
      throw e.getTargetException();
    } catch (Exception e) {
      metrics.exception(e);
      throw new RuntimeException(
          "unexpected invocation exception: " + e.getMessage());
    } finally {
      long processTime = now() - start;
      metrics.incMethodTime(m.getName(), processTime);
      if (auditLogSyncer != null) {
        this.auditLogSyncer.logRequestProcess(tableName, m.getName(), effectiveUser,
          processTime / 1000 / 1000);
      } else {
        LOG.info("AuditLog Syncer is null");
      }
    }
    return result;
  }
  
  private static long now() {
    return System.nanoTime();
  }
}
