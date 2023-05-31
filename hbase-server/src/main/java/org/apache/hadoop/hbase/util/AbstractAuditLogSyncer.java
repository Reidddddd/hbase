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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.hbase.Server;
import org.apache.log4j.Appender;
import org.apache.log4j.AsyncAppender;
import org.apache.log4j.Logger;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public abstract class AbstractAuditLogSyncer extends HasThread {

  protected static final Log AUDITLOG = LogFactory.getLog("SecurityLogger." +
    Server.class.getName());
  protected static final int asyncAppenderBufferSize = 100000;

  protected AtomicBoolean samplingLock = new AtomicBoolean(false);
  protected final long flushInterval;
  protected final Log LOG;
  protected final Object lock = new Object();
  protected volatile Boolean closeAuditSyncer = Boolean.FALSE;

  public AbstractAuditLogSyncer(long flushInterval, Log LOG) {
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
    if (!(AUDITLOG instanceof Log4JLogger)) {
      LOG.warn("Log4j is required to enable async auditlog");
      return;
    }
    Logger logger = Logger.getLogger("SecurityLogger");
    @SuppressWarnings("unchecked") List<Appender> appenders =
      Collections.list(logger.getAllAppenders());
    // failsafe against trying to async it more than once
    if (!appenders.isEmpty() && !(appenders.get(0) instanceof AsyncAppender)) {
      AsyncAppender asyncAppender = new AsyncAppender();
      // change logger to have an async appender containing all the
      // previously configured appenders
      for (Appender appender : appenders) {
        logger.removeAppender(appender);
        asyncAppender.addAppender(appender);
      }
      // non-blocking so that server will not wait for async logger
      // even when the appender's buffer is full
      // some audit events will be lost in this case
      asyncAppender.setBlocking(false);
      asyncAppender.setBufferSize(asyncAppenderBufferSize);
      logger.addAppender(asyncAppender);
    }
    LOG.info("Async AuditLog is enabled");
  }

  protected abstract void auditSync();
}
