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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.BytesGenerator;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * This is not thread safe, and it is used in ThreadLocal only.
 */
@InterfaceAudience.Private
public class IntermediateCellPond {
  private static final Log LOG = LogFactory.getLog(IntermediateCellPond.class);

  static final String SEGMENT_SIZE = "hbase.regionserver.segment.size";
  static final String SEGMENT_NUM = "hbase.regionserver.segment.keep.num";
  static final String SEGMENT_ClEAN = "hbase.regionserver.segment.keep.beforeclean";

  private final LinkedList<Segment> retiredSegments = new LinkedList<>();
  private final LinkedList<Segment> availSegments = new LinkedList<>();
  private static final long UNSET = -1;
  private static int segmentSize;
  private static int segmentKeep;
  private static long segmentKeepBeforeClean;

  private int size;
  private int keep;
  private Segment currentSegment;
  private long lastClean = UNSET;

  private IntermediateCellPond() {}
  private static class Singleton {
    private static final IntermediateCellPond CELLPOND = new IntermediateCellPond();
  }
  public static IntermediateCellPond getInstance() {
    return IntermediateCellPond.Singleton.CELLPOND;
  }

  public static final ThreadLocal<IntermediateCellPond> cellPond = new ThreadLocal() {
    @Override
    protected IntermediateCellPond initialValue() {
      return new IntermediateCellPond(segmentSize, segmentKeep);
    }
  };

  public void initial(Configuration conf) {
    segmentSize = conf.getInt(SEGMENT_SIZE, 2 * 1024 * 1024);
    segmentKeep = conf.getInt(SEGMENT_NUM, 1);
    segmentKeepBeforeClean = conf.getLong(SEGMENT_ClEAN, 30 * 60 * 1000);
  }

  public static class CellPondBytesGenerator implements BytesGenerator {
    ByteBuffer buf;

    @Override
    public byte[] getBytes(int len) {
      buf = cellPond.get().allocate(len);
      return buf.array();
    }

    @Override
    public int getOffset() {
      return buf.arrayOffset();
    }
  }

  public IntermediateCellPond(int segmentSize, int segmentKeep) {
    size = segmentSize;
    keep = segmentKeep;
    currentSegment = new Segment(size);
  }

  public ByteBuffer allocate(int len) {
    if (len > size) {
      LOG.warn(len + " is larger than " + size + ". Skip using cell pond.");
      return ByteBuffer.allocate(len);
    }

    ByteBuffer buf = currentSegment.allocate(len);
    if (buf == null) {
      currentSegment.retire();
      currentSegment = availSegments.isEmpty() ? new Segment(size) : availSegments.removeFirst();
      buf = currentSegment.allocate(len);
    }
    return buf;
  }

  public void clear() {
    currentSegment.clear();
    if (lastClean == UNSET && retiredSegments.size() > keep) {
      lastClean = EnvironmentEdgeManager.currentTime();
    }
    for (Segment segment : retiredSegments) {
      segment.clear();
    }
    if (lastClean != UNSET &&
        EnvironmentEdgeManager.currentTime() - lastClean > segmentKeepBeforeClean) {
      int len = retiredSegments.size();
      while (len-- > keep) {
        retiredSegments.removeLast();
      }
      LOG.info("Cleared redundant segments");
      lastClean = UNSET;
    }
    availSegments.addAll(retiredSegments);
    retiredSegments.clear();
  }

  class Segment {
    ByteBuffer buffer;

    Segment(int size) {
      buffer = ByteBuffer.allocate(size);
    }

    void clear() {
      buffer.clear();
    }

    ByteBuffer allocate(int len) {
      if (buffer.remaining() >= len) {
        try {
          return buffer.slice();
        } finally {
          buffer.position(buffer.position() + len);
        }
      }
      return null;
    }

    void retire() {
      retiredSegments.add(this);
    }
  }

}
