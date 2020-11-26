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

package org.apache.hadoop.hbase.io;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

public final class IPCReservoir {
  private static final Log LOG = LogFactory.getLog(IPCReservoir.class);

  private static final String RESERVOIR_ENABLE = "hbase.ipc.server.reservoir.enabled";
  private static final String USE_DIRECT = "hbase.ipc.server.reservoir.direct.buffer";
  private static final String MAX_BUFFER_SIZE = "hbase.ipc.server.reservoir.max.buffer.size";
  private static final String MIN_BUFFER_SIZE = "hbase.ipc.server.reservoir.initial.buffer.size";
  private static final String MAX_IN_RESERVOIR = "hbase.ipc.server.reservoir.initial.max";

  /**
   * Singleton
   */
  private IPCReservoir() {}
  private static class Singleton {
    private static final IPCReservoir RESERVOIR = new IPCReservoir();
  }
  public static IPCReservoir getInstance() {
    return Singleton.RESERVOIR;
  }

  private boolean useReservoir = false;
  private boolean useDirect;
  private int maxByteBufferSize;
  private int minByteBufferSize;
  private int maxByteBufferNum;
  private Map<Integer, BufferSizeManager> managers = new ConcurrentHashMap<>();

  private final ReentrantLock updateLock = new ReentrantLock();

  /**
   * Initialize IPCReservoir
   */
  public void initialize(Configuration conf) {
    if (conf.getBoolean(RESERVOIR_ENABLE, true)) {
      useReservoir = true;
      useDirect = conf.getBoolean(USE_DIRECT, false);
      maxByteBufferSize = leastPowerOfTwo(conf.getInt(MAX_BUFFER_SIZE, 1024 * 1024));
      minByteBufferSize = leastPowerOfTwo(conf.getInt(MIN_BUFFER_SIZE, 16 * 1024));
      maxByteBufferNum = conf.getInt(MAX_IN_RESERVOIR,
          conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT,
              HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT));
    }
  }

  /**
   * @return the least power of two greater than or equal to n.
   */
  private static int leastPowerOfTwo(final int n) {
    if (n <= 0) {
      throw new IllegalArgumentException("n = " + n + " <= 0");
    }

    final int highestOne = Integer.highestOneBit(n);
    if (highestOne == n) {
      return n; // n is a power of two.
    }
    final int powerOfTwo = highestOne << 1;
    if (powerOfTwo < 0) {
      throw new IllegalArgumentException(
          "Overflow: for n = " + n + ", the least power of two > Integer.MAX_VALUE = " + Integer.MAX_VALUE);
    }
    return powerOfTwo;
  }

  public ByteBuffer claimBuffer(int size) {
    if (!useReservoir) {
      return ByteBuffer.allocate(size);
    }

    int alignedSize = Math.max(minByteBufferSize, leastPowerOfTwo(size));
    if (alignedSize > maxByteBufferSize) {
      return useDirect ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
    }

    BufferSizeManager manager;
    if (!managers.containsKey(alignedSize)) {
      manager = new BufferSizeManager(alignedSize, useDirect);
      updateLock.lock();
      try {
        if (!managers.containsKey(alignedSize)) {
          managers.put(alignedSize, manager);
        }
      } finally {
        updateLock.unlock();
      }
    }
    manager = managers.get(alignedSize);
    ByteBuffer buffer = manager.allocate();
    buffer.limit(size);
    return buffer;
  }

  public void reclaimBuffer(ByteBuffer buffer) {
    if (!useReservoir || buffer == null) {
      return;
    }

    BufferSizeManager manager = managers.get(buffer.capacity());
    if (manager == null) {
      return;
    }
    manager.recycle(buffer);
  }

  public ByteBuffOutputStream createByteBuffOutputStream(ByteBuffer buffer) {
    return new ByteBuffOutputStream(buffer, this);
  }

  public ByteBuffInputStream createByteBuffInputStream(ByteBuffer buffer) {
    return new ByteBuffInputStream(buffer, this);
  }

  class BufferSizeManager {
    private final int bufferSize;
    private final Queue<ByteBuffer> pool = new LinkedBlockingQueue<>();
    private final boolean direct;
    private final AtomicLong lastTouch = new AtomicLong(-1);
    private final AtomicLong allocated = new AtomicLong(0);
    private final AtomicLong reused = new AtomicLong(0);

    BufferSizeManager(int bufferSize, boolean direct) {
      this.bufferSize = bufferSize;
      this.direct = direct;
    }

    ByteBuffer allocate() {
      ByteBuffer buf = pool.poll();
      if (buf == null) {
        buf = direct ? ByteBuffer.allocateDirect(bufferSize) : ByteBuffer.allocate(bufferSize);
        allocated.getAndIncrement();
      } else {
        reused.getAndIncrement();
      }
      buf.clear();
      touch();
      return buf;
    }

    /**
     * Null check should be done before calling recycle()
     * @param buf non-null ByteBuffer
     */
    void recycle(ByteBuffer buf) {
      buf.clear();
      pool.offer(buf);
    }

    void touch() {
      lastTouch.set(EnvironmentEdgeManager.currentTime());
    }

    @Override
    public String toString() {
      return "BufferSizeManager{" + "bufferSize=" + bufferSize + ", bufferInPool=" + pool.size() +
          ", direct=" + direct + ", lastTouch=" + lastTouch + ", allocated=" + allocated +
          ", reused=" + reused + '}';
    }
  }

}
