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
package org.apache.hadoop.hbase.ipc;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Handler thread run the {@link CallRunner#run()} in.
 * Should only be used in {@link RpcExecutor} and its subclass.
 */
@InterfaceAudience.Private
public class RpcHandler extends Thread {
  private static final Log LOG = LogFactory.getLog(RpcHandler.class);

  final AtomicInteger activeHandlerCount;
  final AtomicInteger failedHandlerCount;

  /**
   * Q to find CallRunners to run in.
   */
  final Queue<CallRunner> q;
  final Abortable abortable;
  final double handlerFailureThreshhold;
  final int handlerCount;

  private boolean running;

  RpcHandler(final String name, final double handlerFailureThreshhold, final Queue<CallRunner> q,
      final AtomicInteger activeHandlerCount, final AtomicInteger failedHandlerCount,
      final int handlerCount, Abortable abortable) {
    super(name);
    setDaemon(true);
    this.q = q;
    this.handlerFailureThreshhold = handlerFailureThreshhold;
    this.activeHandlerCount = activeHandlerCount;
    this.failedHandlerCount = failedHandlerCount;
    this.handlerCount = handlerCount;
    this.abortable = abortable;
  }

  /**
   * @return A {@link CallRunner}
   * @throws InterruptedException
   */
  protected CallRunner getCallRunner() throws InterruptedException {
    if (this.q instanceof BlockingQueue) {
      BlockingQueue<CallRunner> bq = (BlockingQueue<CallRunner>) this.q;
      return bq.take();
    }
    return this.q.poll();
  }

  @Override
  public void run() {
    boolean interrupted = false;
    this.running = true;
    try {
      while (running) {
        try {
          run(getCallRunner());
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } catch (Exception e) {
      LOG.warn(e);
      throw e;
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void run(CallRunner cr) {
    if (cr == null) return;
    MonitoredRPCHandler status = RpcServer.getStatus();
    cr.setStatus(status);
    try {
      activeHandlerCount.incrementAndGet();
      cr.run();
    } catch (Throwable e) {
      if (e instanceof Error) {
        int failedCount = failedHandlerCount.incrementAndGet();
        if (this.handlerFailureThreshhold >= 0
            && failedCount > handlerCount * this.handlerFailureThreshhold) {
          String message = "Number of failed RpcServer handler runs exceeded threshhold "
              + this.handlerFailureThreshhold + "; reason: " + StringUtils.stringifyException(e);
          if (abortable != null) {
            abortable.abort(message, e);
          } else {
            LOG.error("Error but can't abort because abortable is null: "
                + StringUtils.stringifyException(e));
            throw e;
          }
        } else {
          LOG.warn("Handler errors " + StringUtils.stringifyException(e));
        }
      } else {
        LOG.warn("Handler  exception " + StringUtils.stringifyException(e));
      }
    } finally {
      this.activeHandlerCount.decrementAndGet();
    }
  }

  public void stopRunning() {
    this.running = false;
  }
}
