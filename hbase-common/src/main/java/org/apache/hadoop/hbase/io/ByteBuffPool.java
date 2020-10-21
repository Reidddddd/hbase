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
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import com.google.common.annotations.VisibleForTesting;

/**
 * <p>A ByteBuffer pool in RegionServer. It is initialized with min and max size. All allocation size
 * smaller than min size will get a buffer with min size. Larger than max size will get an exactly
 * size as claimed. All size between min and max will be managed by this pool and get reused. Note that,
 * you probably will not get an exactly same capacity as claim, but rather the nearest pow of two of claim size.
 * E.g, if you claim a ByteBuffer with size 6, this pool will return a ByteBuffer with capacity 8.
 * This intention is to align memory buffer and not to fragile the memory.
 *
 * <p>There's no limit on the number of ByteBuffer in pool, but there's one chore in this pool intervally
 * clean no time no use ByteBuffer in 2 hours.
 *
 * <p>This class is thread safe.
 */
@InterfaceAudience.Private
@SuppressWarnings("NonAtomicVolatileUpdate") // Suppress error-prone warning, see HBASE-21162
public class ByteBuffPool {
  private static final Log LOG = LogFactory.getLog(ByteBuffPool.class);

  // Maximum size of a ByteBuffer to retain in pool
  private int maxByteBufferSizeToCache;
  // Minimum size of a ByteBuffer to retain in pool
  private int minByteBufferSizeToCache;
  // Use directy ByteBuffer or not
  private boolean directBuffer;

  private final Map<Integer, BufferSizeManager> bufferManagers = new ConcurrentHashMap<>();

  private boolean useReservoir = false;

  private final Timer timer = new Timer();

  public void initialize(Configuration conf) {
    if (conf.getBoolean("hbase.ipc.server.reservoir.enabled", true)) {
      this.useReservoir = true;
      this.maxByteBufferSizeToCache = leastPowerOfTwo(
        conf.getInt("hbase.ipc.server.reservoir.max.buffer.size", 1024 * 1024));
      this.minByteBufferSizeToCache = leastPowerOfTwo(
        conf.getInt("hbase.ipc.server.reservoir.initial.buffer.size", 16 * 1024));
      this.directBuffer =
          conf.getBoolean("hbase.ipc.server.reservoir.direct.buffer", false);

      // 3 hours as ttl
      timer.scheduleAtFixedRate(new CleanByteBufferTask(10800000), 1800000, 1800000);
    }
    // else we still use ByteBuffPool, but only allocate on heap memory, with no management and check.
  }

  /**
   * Claim ByteBuffer with exact size, and it is claimed from on-heap. It should be used when size is clear and sure.
   * @param size exact size
   * @return a byte buffer
   */
  public ByteBuffer claimBufferExactly(int size) {
    // BufferSizeManager for this should not be cleaned. And it doesn't respect the min max.
    if (!useReservoir) {
      return ByteBuffer.allocate(size);
    }

    BufferSizeManager manager = bufferManagers.get(size);
    if (manager == null) {
      manager = new BufferSizeManager(size, false, true);
      if (!bufferManagers.containsKey(size)) {
        bufferManagers.put(size, manager);
      }
    }
    // Get again, for cocurrency concern.
    manager = bufferManagers.get(size);
    return manager.allocate();
  }

  /**
   * Claim ByteBuffer with estimated size, use direct or not is determined by reservoir.
   * It should be used in Requests/Response.
   * @param size estimated size
   * @return a byte buffer
   */
  public ByteBuffer claimBuffer(int size) {
    return claimBuffer(size, directBuffer);
  }

  /**
   * Claim ByteBuffer with estimated size. It should be used in Requests/Response.
   * @param size estimated size
   * @param direct use direct ByteBuffer if true. If reservoir is disabled, it will allocate on-heap ByteBuffer
   * @return a byte buffer
   */
  public ByteBuffer claimBuffer(int size, boolean direct) {
    if (!useReservoir) {
      return ByteBuffer.allocate(size);
    }

    // powerOfTwo is smaller than minByteBufferSizeToCachem, we use minByteBufferSizeToCache;
    int actualSize = Math.max(leastPowerOfTwo(size), minByteBufferSizeToCache);
    // if size is too big, let's say larger than maxByteBufferSizeToCache.
    // We still allocate it, but without BufferSizeManager.
    if (actualSize > maxByteBufferSizeToCache) {
      return directBuffer ?
        ByteBuffer.allocateDirect(actualSize) :
        ByteBuffer.allocate(actualSize);
    }

    BufferSizeManager manager = bufferManagers.get(actualSize);
    if (manager == null) {
      manager = new BufferSizeManager(actualSize, direct, false);
      if (!bufferManagers.containsKey(size)) {
        bufferManagers.put(actualSize, manager);
      }
    }
    // Get again, for cocurrency concern.
    manager = bufferManagers.get(actualSize);
    return manager.allocate();
  }

  /**
   * Reclaim byte buffer
   * @param buf return byte buffer
   */
  public void reclaimBuffer(ByteBuffer buf) {
    if (!useReservoir) {
      return;
    }

    BufferSizeManager manager = bufferManagers.get(buf.capacity());
    if (manager == null) {
      // e.g., if buf was larger than maxByteBufferSizeToCache
      // there would be no BufferSizeManager.
      return;
    }
    manager.recycle(buf);
  }

  public ByteBuffOutputStream createResizableByteBufferOutputStream(ByteBuffer buf) {
    return new ByteBuffOutputStream(buf, this);
  }

  private class CleanByteBufferTask extends TimerTask {
    final long ttl;

    CleanByteBufferTask(long ttl) {
      this.ttl = ttl;
    }

    @Override
    public void run() {
      boolean cleaned = false;
      long currentT = EnvironmentEdgeManager.currentTime();
      for (BufferSizeManager bsm : bufferManagers.values()) {
        if (bsm.direct || bsm.noNeedToClean) {
          continue; // allocate direct memory is expensive. And it doesn't effect GC, just leave it.
        }

        bsm.lock.lock();
        try {
          if (currentT - bsm.lastTouch < ttl || bsm.pool.size() == 0) {
            continue;
          }
          bsm.pool.poll(); // Remove it since it hasn't been claimed for more than ttl.
          bsm.touch();
          cleaned = true;
          LOG.info("Removed one buffer with size " + bsm.bufferSize +
              ". since it hasn't been used for hours. Remains " + bsm.pool.size());
          break; // We only remove one from one of the BufferSizeManager at one time, to be gently.
        } finally {
          bsm.lock.unlock();
        }
      }
      if (!cleaned) {
        for (BufferSizeManager bsm : bufferManagers.values()) {
          if (bsm.pool.size() == 0) {
            continue;
          }
          LOG.info("There are " + bsm.pool.size() + " buffer size with " + bsm.bufferSize);
        }
      }
    }
  }

  @VisibleForTesting
  void initialize(int maxBufferSize, int initialSize, boolean runCleanTask) {
    this.useReservoir = true;
    this.maxByteBufferSizeToCache = leastPowerOfTwo(maxBufferSize);
    this.minByteBufferSizeToCache = leastPowerOfTwo(initialSize);
    this.directBuffer = false;
    if (runCleanTask) {
      this.timer.schedule(new CleanByteBufferTask(5000), 10000, 6000);
    }
  }

  @VisibleForTesting
  int getMinByteBufferSizeToCache() {
    return minByteBufferSizeToCache;
  }

  @VisibleForTesting
  int getMaxByteBufferSizeToCache() {
    return maxByteBufferSizeToCache;
  }

  @VisibleForTesting
  int getBufferInPoolNum() {
    int num = 0;
    for (BufferSizeManager bsm : bufferManagers.values()) {
      num += bsm.pool.size();
    }
    return num;
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

  private ByteBuffPool() {}

  private static class Singleton {
    private static final ByteBuffPool POOL = new ByteBuffPool();
  }

  public static ByteBuffPool getInstance() {
    return Singleton.POOL;
  }

  private class BufferSizeManager {
    final int bufferSize;
    final ReentrantLock lock =  new ReentrantLock();
    final Queue<ByteBuffer> pool = new LinkedList<>();
    final boolean direct;
    final boolean noNeedToClean;

    volatile long lastTouch = -1;

    BufferSizeManager(int bufferSize, boolean direct, boolean noNeedToClean) {
      this.bufferSize = bufferSize;
      this.direct = direct;
      this.noNeedToClean = noNeedToClean;
    }

    ByteBuffer allocate() {
      lock.lock();
      try {
        ByteBuffer buf = pool.poll();
        return buf != null ? buf :
            direct ? ByteBuffer.allocateDirect(bufferSize) : ByteBuffer.allocate(bufferSize);
      } finally {
        touch();
        lock.unlock();
      }
    }

    void recycle(ByteBuffer buf) {
      buf.clear();
      lock.lock();
      try {
        pool.offer(buf);
      } finally {
        lock.unlock();
      }
    }

    void touch() {
      lastTouch = EnvironmentEdgeManager.currentTime();
    }
  }

}
