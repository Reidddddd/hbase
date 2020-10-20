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
import java.util.Random;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(SmallTests.class)
public class TestByteBuffOutputStream {

  @Test
  public void testByteBuffOutputStream() throws Exception {
    ByteBuffPool reservoir = ByteBuffPool.getInstance();
    reservoir.initialize(1020, 60, false);

    Random random = new Random();
    ByteBuffer bb = reservoir.claimBuffer(64);
    assertEquals(64, bb.capacity());
    ByteBuffOutputStream bbos = reservoir.createResizableByteBufferOutputStream(bb);
    byte[] writeMoreThan64 = new byte[120];
    for (int i = 0; i < 120; i++) {
      writeMoreThan64[i] = (byte) random.nextInt(10);
    }
    bbos.write(writeMoreThan64);
    // ByteBuffOutputStream should claim a new buffer with 128, and the original one is back to pool.
    assertEquals(1, reservoir.getBufferInPoolNum());
    assertEquals(120, bbos.size());
    ByteBuffer exactBuffer = bbos.getByteBuffer();
    assertEquals(128, exactBuffer.capacity());
    reservoir.reclaimBuffer(exactBuffer);
    assertEquals(2, reservoir.getBufferInPoolNum());
  }

}