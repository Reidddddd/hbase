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
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Pair;

@InterfaceAudience.Private
public final class HByteBuffer {
  private static Log LOG = LogFactory.getLog(HByteBuffer.class);

  static final String ENABLE = "hbase.regionserver.bytebuffer.enable";
  static final String SIZE = "hbase.regionserver.bytebuffer.buffer.size";
  static final String RETAINED = "hbase.regionserver.bytebuffer.retained.for.connection";

  private final Lock lock = new ReentrantLock();

  private int size;
  private boolean enable;
  private int retainedForConnection;
  private Queue<ByteBuffer> connectionBuffer = new ArrayDeque<>();

  private ThreadLocal<ByteBuffer> handlerByteBuffer = new ThreadLocal() {
    @Override
    protected ByteBuffer initialValue() {
      return ByteBuffer.allocate(size);
    }
  };


  private static class Singleton {
    private static final HByteBuffer HBB = new HByteBuffer();
  }

  public static HByteBuffer getInstance() {
    return Singleton.HBB;
  }

  private HByteBuffer() {
  }

  public void initialize(Configuration conf) {
    enable = conf.getBoolean(ENABLE, false);
    size = conf.getInt(SIZE, 1024 * 1024 * 3);
    retainedForConnection = conf.getInt(RETAINED, 500);
    connectionBuffer = new ArrayDeque<>(retainedForConnection);
  }

  public Pair<byte[], Integer> claimHandlerBuffer(int len) {
    if (!enable || len > size) {
      return new Pair<>(ByteBuffer.allocate(len).array(), 0);
    }

    ByteBuffer buf = handlerByteBuffer.get();
    if (buf.remaining() >= len) {
      ByteBuffer slice;
      try {
        slice = buf.slice();
      } finally {
        buf.position(buf.position() + len);
      }
      return new Pair<>(slice.array(), slice.arrayOffset());
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Encounter a large request which is more than " + size + " bytes");
      }
      return new Pair<>(ByteBuffer.allocate(len).array(), 0);
    }
  }


  public void clearHandlerBuffer() {
    if (enable) {
      handlerByteBuffer.get().clear();
    }
  }

  public ByteBuffer claimConnectionBuffer() {
    if (!enable) {
      return ByteBuffer.allocate(size);
    }

    ByteBuffer buf;
    lock.lock();
    try {
      buf = connectionBuffer.poll();
    } finally {
      lock.unlock();
    }
    return buf == null ? ByteBuffer.allocate(size) : buf;
  }

  public boolean clearConnectionBuffer(ByteBuffer buf) {
    if (!enable || buf.capacity() > size) {
      return false;
    }

    buf.clear();
    lock.lock();
    try {
      if (connectionBuffer.size() >= retainedForConnection) {
        return false;
      }
      connectionBuffer.offer(buf);
    } finally {
      lock.unlock();
    }
    return true;
  }

  /**
   * For unit test only, and it isn't thread-safe, but for test there is only one thread,
   * so it should be fine.
   * Please do not call it in production env.
   */
  @VisibleForTesting
  int size() {
    return connectionBuffer.size();
  }

}
