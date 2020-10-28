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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SmallTests.class })
public class TestByteBuffPool {
  final int maxByteBufferSizeToCache = 1020;
  final int initialByteBufferSize = 60;

  @Test
  public void testConfiguredSize() {
    ByteBuffPool reservoir = ByteBuffPool.getInstance();
    reservoir.initialize(maxByteBufferSizeToCache, initialByteBufferSize, false);

    assertEquals(64, reservoir.getMinByteBufferSizeToCache());
    assertEquals(1024, reservoir.getMaxByteBufferSizeToCache());
  }

  @Test
  public void testClaimThenReclaim() {
    ByteBuffPool reservoir = ByteBuffPool.getInstance();
    reservoir.initialize(maxByteBufferSizeToCache, initialByteBufferSize, false);

    // Less than min
    ByteBuffer bb = reservoir.claimOnheapBuffer(20);
    assertEquals(64, bb.capacity());
    assertEquals(0, reservoir.getBufferInPoolNum());
    reservoir.reclaimBuffer(bb);
    assertEquals(1, reservoir.getBufferInPoolNum());
    // Larger than min
    bb = reservoir.claimOnheapBuffer(120);
    assertEquals(128, bb.capacity());
    reservoir.reclaimBuffer(bb);
    assertEquals(2, reservoir.getBufferInPoolNum());
    // Near max
    bb = reservoir.claimOnheapBuffer(1000);
    assertEquals(1024, bb.capacity());
    reservoir.reclaimBuffer(bb);
    assertEquals(3, reservoir.getBufferInPoolNum());
    // Larger than max, will return the exactly size
    bb = reservoir.claimOnheapBuffer(2000);
    assertEquals(2000, bb.capacity());
    reservoir.reclaimBuffer(bb);
    // reservoir won't reclaim since it is larger than max
    assertEquals(3, reservoir.getBufferInPoolNum());
    // claim (10) more than allowed, there're already 3 there.
    ByteBuffer[] bbs = new ByteBuffer[10];
    for (int i = 0; i < 10; i++) {
      bbs[i] = reservoir.claimOnheapBuffer(60);
    }
    // buffer size with 128 and 1024 remain
    assertEquals(2, reservoir.getBufferInPoolNum());
  }

  @Test
  public void testTimerCleaner() throws Exception {
    ByteBuffPool reservoir = ByteBuffPool.getInstance();
    reservoir.initialize(maxByteBufferSizeToCache, initialByteBufferSize, true);

    ByteBuffer[] bbs = new ByteBuffer[5];
    for (int i = 0; i < 5; i++) {
      bbs[i] = reservoir.claimOnheapBuffer(60);
    }
    for (int i = 0; i < 5; i++) {
      reservoir.reclaimBuffer(bbs[i]);
    }
    assertEquals(5, reservoir.getBufferInPoolNum());
    Thread.sleep(12000);
    // Should remove one
    assertEquals(4, reservoir.getBufferInPoolNum());
    Thread.sleep(6000);
    // Should remove one
    assertEquals(3, reservoir.getBufferInPoolNum());
    Thread.sleep(6000);
    // Should remove one
    assertEquals(2, reservoir.getBufferInPoolNum());
    Thread.sleep(6000);
    // Should remove one
    assertEquals(1, reservoir.getBufferInPoolNum());
    Thread.sleep(6000);
    // Should remove one
    assertEquals(0, reservoir.getBufferInPoolNum());
  }

}
