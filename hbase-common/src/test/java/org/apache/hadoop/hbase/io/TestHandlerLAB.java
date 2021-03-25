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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SmallTests.class })
public class TestHandlerLAB {

  static HandlerLAB bufferPool;

  static int SIZE = 3 * 1024;

  @BeforeClass
  public static void before() {
    Configuration conf = new Configuration();
    conf.setBoolean(HandlerLAB.ENABLE, true);
    conf.setInt(HandlerLAB.SIZE, SIZE);
    bufferPool = HandlerLAB.getInstance();
    bufferPool.initialize(conf);
  }

  @Test
  public void testClaimByteArray() {
    HandlerLAB.ByteArrayAndOffset arrayInt1 = bufferPool.claimBytesFromHandlerLAB(1024);
    Assert.assertEquals(0, arrayInt1.offset());
    HandlerLAB.ByteArrayAndOffset arrayInt2 = bufferPool.claimBytesFromHandlerLAB(2000);
    Assert.assertEquals(1024, arrayInt2.offset());
    HandlerLAB.ByteArrayAndOffset arrayInt3 = bufferPool.claimBytesFromHandlerLAB(10);
    Assert.assertEquals(1024 + 2000, arrayInt3.offset());
    Assert.assertEquals(arrayInt1.array(), arrayInt2.array());
    Assert.assertEquals(arrayInt2.array(), arrayInt3.array());
    // It is larger than 3 * 1024, it will allocate a new one
    HandlerLAB.ByteArrayAndOffset arrayInt4 = bufferPool.claimBytesFromHandlerLAB(1024);
    Assert.assertEquals(0, arrayInt4.offset());
    Assert.assertNotEquals(arrayInt1.array(), arrayInt4.array());
    Assert.assertNotEquals(arrayInt2.array(), arrayInt4.array());
    Assert.assertNotEquals(arrayInt3.array(), arrayInt4.array());
  }

}
