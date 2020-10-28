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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
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
 * clean no time no use ByteBuffer in 3 hours.
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
  // Max number for each size
  private int maxBuffer;

  private final Map<Integer, BufferSizeManager> onheapManagers = new ConcurrentHashMap<>();
  private final Map<Integer, BufferSizeManager> offheapManagers = new ConcurrentHashMap<>();

  private final ReentrantLock onheapUpdateLock = new ReentrantLock();
  private final ReentrantLock offheapUpdateLock = new ReentrantLock();

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
      this.maxBuffer = conf.getInt("hbase.ipc.server.reservoir.initial.max",
        conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT,
            HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT));

      // 3 hours as ttl
      timer.scheduleAtFixedRate(new CleanByteBufferTask(10800000), 1800000, 1800000);
    }
    // else we still use ByteBuffPool, but only allocate on heap memory, with no management and check.
  }

  /**
   * Claim ByteBuffer with exact size, and it is claimed from on-heap.
   * It should be used when size is clear and sure ans small if better.
   * @param size exact size
   * @return a byte buffer
   */
  public ByteBuffer claimBufferExactly(int size) {
    // BufferSizeManager for this should not be cleaned. And it doesn't respect the min max.
    if (!useReservoir) {
      return ByteBuffer.allocate(size);
    }

    BufferSizeManager manager;
    if (!onheapManagers.containsKey(size)) {
      manager = new BufferSizeManager(size, false, true);
      onheapUpdateLock.lock();
      try {
        if (!onheapManagers.containsKey(size)) {
          onheapManagers.put(size, manager);
        }
      } finally {
        onheapUpdateLock.unlock();
      }
    }

    manager = onheapManagers.get(size);
    return manager.allocate();
  }

  /**
   * Claim a onheap ByteBuffer with size. The capacity of ByteBuffer will be the least power of 2
   * of the param size, unless it is larger than hbase.ipc.server.reservoir.max.buffer.size.
   * Please use limit() for capacity check.
   * @param size size
   * @return a onheap ByteBuffer
   */
  public ByteBuffer claimOnheapBuffer(int size) {
    return claimBuffer(size, false);
  }

  /**
   * Claim a offheap ByteBuffer with size if hbase.ipc.server.reservoir.direct.buffer set true.
   * Otherwise it will be the same as claimOnheapBuffer.
   * This method always be used in sending RPC response.
   * @param size size
   * @return a offheap ByteBuffer if hbase.ipc.server.reservoir.direct.buffer is true, onheap else
   */
  public ByteBuffer claimOffheapBufferIfPossible(int size) {
    return claimBuffer(size, directBuffer);
  }

  /**
   * Claim ByteBuffer with estimated size. It should be used in Requests/Response.
   * @param size estimated size
   * @param direct use direct ByteBuffer if true. If reservoir is disabled, it will allocate on-heap ByteBuffer
   * @return a byte buffer
   */
  private ByteBuffer claimBuffer(int size, boolean direct) {
    if (!useReservoir) {
      return ByteBuffer.allocate(size);
    }

    // powerOfTwo is smaller than minByteBufferSizeToCachem, we use minByteBufferSizeToCache;
    int actualSize = Math.max(leastPowerOfTwo(size), minByteBufferSizeToCache);
    // if size is too big, let's say larger than maxByteBufferSizeToCache.
    // We still allocate it, but without BufferSizeManager.
    if (actualSize > maxByteBufferSizeToCache) {
      return direct ?
        ByteBuffer.allocateDirect(size) :
        ByteBuffer.allocate(size);
    }

    Map<Integer, BufferSizeManager> bufferManagers = direct ? offheapManagers : onheapManagers;
    ReentrantLock lock = direct ? offheapUpdateLock : onheapUpdateLock;

    BufferSizeManager manager;
    if (!bufferManagers.containsKey(actualSize)) {
      manager = new BufferSizeManager(actualSize, direct, false);
      lock.lock();
      try {
        if (!bufferManagers.containsKey(actualSize)) {
          bufferManagers.put(actualSize, manager);
        }
      } finally {
        lock.unlock();
      }
    }

    manager = bufferManagers.get(actualSize);
    ByteBuffer buf = manager.allocate();
    buf.limit(size);
    return buf;
  }

  /**
   * Reclaim byte buffer
   * @param buf return byte buffer
   */
  public void reclaimBuffer(ByteBuffer buf) {
    if (!useReservoir || buf == null) {
      return;
    }

    Map<Integer, BufferSizeManager> bufferManagers = buf.hasArray() ? onheapManagers : offheapManagers;
    BufferSizeManager manager = bufferManagers.get(buf.capacity());
    if (manager == null) {
      // e.g., if buf was larger than maxByteBufferSizeToCache
      // there would be no BufferSizeManager.
      return;
    }
    manager.recycle(buf);
  }

  /**
   * It is resizeable, if buf provided is not suffcient for write.
   * Calling close will not reclaim the ByteBuffer because it creates bug.
   * Need to explicitly call #reclaimBuffer.
   */
  public ByteBuffOutputStream createByteBuffOutputStream(ByteBuffer buf) {
    return new ByteBuffOutputStream(buf, this);
  }

  /**
   * ByteBuffInputStream, close will automatically reclaim the buf.
   */
  public ByteBuffInputStream createByteBuffInputStream(ByteBuffer buf) {
    return new ByteBuffInputStream(buf, this);
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
      // allocate direct memory is expensive. And it doesn't effect GC, just leave it.
      for (BufferSizeManager bsm : onheapManagers.values()) {
        if (bsm.noNeedToClean) {
          continue;
        }

        if (bsm.pool.size() > maxBuffer) {
          bsm.pool.poll(); // Remove it since it hasn't been claimed for more than ttl.
          bsm.touch();
          cleaned = true;
          LOG.info("Removed one buffer with size " + bsm.bufferSize +
              ". since there're more than " + maxBuffer);
          continue; // not break here, since other bsm may have more than maxBuffer as well
        }

        if (currentT - bsm.lastTouch < ttl || bsm.pool.size() == 0) {
          continue;
        }

        bsm.pool.poll(); // Remove it since it hasn't been claimed for more than ttl.
        bsm.touch();
        cleaned = true;
        LOG.info("Removed one buffer with size " + bsm.bufferSize +
            ". since it hasn't been used for hours. Remains " + bsm.pool.size());
        break; // We only remove one from one of the BufferSizeManager at one time, to be gently.
      }

      if (!cleaned) {
        for (BufferSizeManager bsm : onheapManagers.values()) {
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
    for (BufferSizeManager bsm : onheapManagers.values()) {
      num += bsm.pool.size();
    }
    for (BufferSizeManager bsm : offheapManagers.values()) {
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
    final Queue<ByteBuffer> pool = new LinkedBlockingQueue<>();
    final boolean direct;
    final boolean noNeedToClean;

    volatile long lastTouch = -1;

    BufferSizeManager(int bufferSize, boolean direct, boolean noNeedToClean) {
      this.bufferSize = bufferSize;
      this.direct = direct;
      this.noNeedToClean = noNeedToClean;
    }

    ByteBuffer allocate() {
      ByteBuffer buf = pool.poll();
      if (buf == null) {
        buf = direct ? ByteBuffer.allocateDirect(bufferSize) : ByteBuffer.allocate(bufferSize);
      }
      touch();
      return buf;
    }

    void recycle(ByteBuffer buf) {
      buf.clear();
      // Null check is done before calling recycle()
      pool.offer(buf);
    }

    void touch() {
      lastTouch = EnvironmentEdgeManager.currentTime();
    }
  }

}
