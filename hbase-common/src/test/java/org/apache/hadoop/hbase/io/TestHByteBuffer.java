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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.ByteBuffer;

@Category({ SmallTests.class })
public class TestHByteBuffer {

  static HByteBuffer bufferPool;

  static int SIZE = 3 * 1024;
  static int RETAINED = 5;

  @BeforeClass
  public static void before() {
    Configuration conf = new Configuration();
    conf.setBoolean(HByteBuffer.ENABLE, true);
    conf.setInt(HByteBuffer.SIZE, SIZE);
    conf.setInt(HByteBuffer.RETAINED, RETAINED);
    bufferPool = HByteBuffer.getInstance();
    bufferPool.initialize(conf);
  }

  @Test
  public void testConnectionBufferCapacity() {
    ByteBuffer buffer;
    buffer = bufferPool.claimConnectionBuffer();
    Assert.assertEquals(SIZE, buffer.capacity());
    bufferPool.clearConnectionBuffer(buffer);
  }

  @Test
  public void testHandlerBuffer() {
    Pair<byte[], Integer> byteInt1 = bufferPool.claimHandlerBuffer(1024);
    Assert.assertEquals(0, byteInt1.getSecond().intValue());
    Pair<byte[], Integer> byteInt2 = bufferPool.claimHandlerBuffer(2000);
    Assert.assertEquals(1024, byteInt2.getSecond().intValue());
    Pair<byte[], Integer> byteInt3 = bufferPool.claimHandlerBuffer(10);
    Assert.assertEquals(1024 + 2000, byteInt3.getSecond().intValue());
    Assert.assertEquals(byteInt1.getFirst(), byteInt2.getFirst());
    Assert.assertEquals(byteInt2.getFirst(), byteInt3.getFirst());
    // It is larger than 3 * 1024, it will allocate a new one
    Pair<byte[], Integer> byteInt4 = bufferPool.claimHandlerBuffer(1024);
    Assert.assertEquals(0, byteInt4.getSecond().intValue());
    Assert.assertNotEquals(byteInt1.getFirst(), byteInt4.getFirst());
    Assert.assertNotEquals(byteInt2.getFirst(), byteInt4.getFirst());
    Assert.assertNotEquals(byteInt3.getFirst(), byteInt4.getFirst());
  }

  @Test
  public void testConnectionBufferRetained() {
    ByteBuffer buf1 = bufferPool.claimConnectionBuffer();
    ByteBuffer buf2 = bufferPool.claimConnectionBuffer();
    ByteBuffer buf3 = bufferPool.claimConnectionBuffer();
    ByteBuffer buf4 = bufferPool.claimConnectionBuffer();
    ByteBuffer buf5 = bufferPool.claimConnectionBuffer();
    ByteBuffer buf6 = bufferPool.claimConnectionBuffer();
    ByteBuffer buf7 = bufferPool.claimConnectionBuffer();
    bufferPool.clearConnectionBuffer(buf1);
    bufferPool.clearConnectionBuffer(buf2);
    bufferPool.clearConnectionBuffer(buf3);
    bufferPool.clearConnectionBuffer(buf4);
    bufferPool.clearConnectionBuffer(buf5);
    bufferPool.clearConnectionBuffer(buf6);
    bufferPool.clearConnectionBuffer(buf7);
    Assert.assertEquals(RETAINED, bufferPool.size());
  }

  @Test
  public void testNotClaimOversizeByteBuffer() {
    ByteBuffer buf = ByteBuffer.allocate(4 * 1024);
    boolean shouldBeFalse = bufferPool.clearConnectionBuffer(buf);
    Assert.assertFalse(shouldBeFalse);
  }

}
