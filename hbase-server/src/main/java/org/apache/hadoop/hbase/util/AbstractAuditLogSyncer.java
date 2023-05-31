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
package org.apache.hadoop.hbase.util;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.logging.Log4jUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public abstract class AbstractAuditLogSyncer extends Thread {

  protected static final Logger AUDITLOG = LoggerFactory.getLogger("SecurityLogger." +
    Server.class.getName());
  protected static final int asyncAppenderBufferSize = 100000;

  protected AtomicBoolean samplingLock = new AtomicBoolean(false);
  protected final long flushInterval;
  protected final Logger LOG;
  protected final Object lock = new Object();
  protected volatile Boolean closeAuditSyncer = Boolean.FALSE;

  public AbstractAuditLogSyncer(long flushInterval, Logger LOG) {
    this.flushInterval = flushInterval;
    this.LOG = LOG;
  }

  @Override
  public void run() {
    try {
      synchronized (lock) {
        while (!closeAuditSyncer) {
          auditSync();
          lock.wait(this.flushInterval);
        }
      }
    } catch (InterruptedException e) {
      LOG.debug(getName() + " interrupted while waiting for sync requests");
    } catch (Exception e) {
      LOG.error("Error while syncing auditLog ", e);
    } finally {
      LOG.info(getName() + " exiting");
    }
  }

  public void close() {
    synchronized (lock) {
      closeAuditSyncer = Boolean.TRUE;
      lock.notifyAll();
    }
  }

  public void enableAsyncAuditLog() {
    Log4jUtils.enableAsyncAuditLog("SecurityLogger", false, asyncAppenderBufferSize);
    LOG.info("Async AuditLog is enabled");
  }

  protected abstract void auditSync();
}
