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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * A handler-local allocation buffer.
 * <p>
 * The purpose of HandlerLAB is for avoiding memory fragments when converting raw data into
 * Mutation in the write request handling. Trying best to ensure as much as k-vs are stored in
 * a contiguous memory.
 * <p>
 * It is thread-local based, so all methods are called in thread-safe.
 * <p>
 * If the MSLAB is disabled, this must not be enable which will lead to data loss otherwise.
 * <p>
 */
@InterfaceAudience.Private
public final class HandlerLAB {
  private static Log LOG = LogFactory.getLog(HandlerLAB.class);

  public static final String SIZE = "hbase.regionserver.hlab.size";
  // Set this default value is due to client side BufferMutator write's buffer size is 2MB
  // We set it a bit larger to cover the oversize edge case.
  public static final int SIZE_DEFAULT = 2621440; // 2.5MB

  static final String ENABLE = "hbase.regionserver.hlab.enable";

  private int size;
  private boolean enable;

  private ThreadLocal<ByteArrayAndOffset> handlerByteArray = new ThreadLocal() {
    @Override
    protected ByteArrayAndOffset initialValue() {
      return new ByteArrayAndOffset(size);
    }
  };

  private static class Singleton {
    private static final HandlerLAB HBB = new HandlerLAB();
  }

  public static HandlerLAB getInstance() {
    return Singleton.HBB;
  }

  private HandlerLAB() {
  }

  public void initialize(Configuration conf) {
    enable = conf.getBoolean(ENABLE, false);
    if (!conf.getBoolean("hbase.hregion.memstore.mslab.enabled", true)) {
      // If mslab is not enabled, we can't use this feature
      // because it will lead to data loss.
      enable = false;
    }
    size = conf.getInt(SIZE, SIZE_DEFAULT);
    String msg = "HandlerLAB is " + (enable ? "enable" : "disable");
    LOG.info(msg + (enable ? ". Size of LAB is " + size + " bytes." : "."));
  }

  public int defaultSize() {
    return size;
  }

  public ByteArrayAndOffset claimBytesFromHandlerLAB(int len) {
    if (!enable || len > size) {
      return new ByteArrayAndOffset(len);
    }

    ByteArrayAndOffset bao = handlerByteArray.get();
    if (bao.remaining >= len) {
      bao.remaining -= len;
      bao.offset = bao.position;
      bao.position += len;
      return bao;
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Encounter a large request which is more than " + size + " bytes");
      }
      return new ByteArrayAndOffset(len);
    }
  }

  public void clearHandlerLAB() {
    if (isEnable()) {
      handlerByteArray.get().clear();
    }
  }

  public boolean isEnable() {
    return enable;
  }

  public static class ByteArrayAndOffset {
    final byte[] array;
    final int capacity;
    int remaining;
    int position;
    int offset;

    ByteArrayAndOffset(int len) {
      array = new byte[len];
      capacity = len;
      remaining = len;
      position = 0;
      offset = 0;
    }

    void clear() {
      remaining = capacity;
      position = 0;
      offset = 0;
    }

    public byte[] array() {
      return array;
    }

    public int offset() {
      return offset;
    }
  }
}
