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

package org.apache.hadoop.hbase;

import java.util.Deque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A thread pool with the {@link ConcurrentLinkedDeque} based FastPath feature.
 * Has a better performance of {@link BlockingQueue}
 */
@InterfaceStability.Evolving
@InterfaceAudience.Public
public class FastPathExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(FastPathExecutor.class);

  class FastPathHandler extends Thread {

    private final Semaphore semaphore = new Semaphore(0);

    private FastPathProcessable loadedTask;

    private boolean running;

    private int errorLog = 0;

    public FastPathHandler(String name) {
      super(name);
      setDaemon(true);
    }

    @Override
    public void run() {
      boolean interrupted = false;
      running = true;
      while (running) {
        try {
          getTask().process();
        } catch (InterruptedException e) {
          interrupted = true;
        } catch (Throwable e) {
          // probably will be chatty , just log once in a while
          if (errorLog % 100 == 0) {
            LOG.warn("", e);
            errorLog = 0;
          }
          errorLog++;
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
            running = false;
          }
        }
      }
    }

    private FastPathProcessable getTask() throws InterruptedException {
      FastPathProcessable task = queue.poll();
      if (task == null) {
        fastPathHandlerStack.push(this);
        this.semaphore.acquire();
        task = this.loadedTask;
        this.loadedTask = null;
      }
      return task;
    }

    private boolean loadTask(final FastPathProcessable task) {
      this.loadedTask = task;
      this.semaphore.release();
      return true;
    }

  }

  private final Deque<FastPathHandler> fastPathHandlerStack = new ConcurrentLinkedDeque<>();

  private FastPathHandler[] handlers;

  private BlockingQueue<FastPathProcessable> queue = new LinkedBlockingQueue<>();

  public FastPathExecutor(int numberHandlers, String name) {
    handlers = new FastPathHandler[numberHandlers];
    for (int i = 0; i < numberHandlers; i++) {
      handlers[i] = new FastPathHandler(name + "-" + i);
    }
  }

  public void start() {
    for (FastPathHandler handler : handlers) {
      handler.start();
    }
  }

  public void stop() {
    for (FastPathHandler handler : handlers) {
      handler.interrupt();
    }

    for (FastPathHandler handler : handlers) {
      try {
        handler.join();
      } catch (InterruptedException e) {
        // just ignore in stopping phase
      }
    }
  }

  public boolean accept(FastPathProcessable task) {
    FastPathHandler handler = fastPathHandlerStack.poll();
    return handler == null ? queue.offer(task) : handler.loadTask(task);
  }

  int numHandlerInStack() {
    return fastPathHandlerStack.size();
  }

  int numTasksInQueue() {
    return queue.size();
  }

}
