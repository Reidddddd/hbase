/*
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

package org.apache.hadoop.hbase;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ SmallTests.class})
public class TestFastPathExecutor {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFastPathExecutor.class);
  private static final Logger LOG = LoggerFactory.getLogger(TestFastPathExecutor.class);

  @Test
  public void testFastPathExecutor() throws Exception {
    int numHandlers = 10;
    AtomicInteger counter = new AtomicInteger(0);
    FastPathExecutor fpe = new FastPathExecutor(numHandlers,"TestFastPathExecutor");
    fpe.start();
    Semaphore semaphore = new Semaphore(0);
    CyclicBarrier barrier = new CyclicBarrier(10, () -> {
      Assert.assertEquals(0, fpe.numHandlerInStack());
      Assert.assertEquals(90, fpe.numTasksInQueue());
    });

    Thread.sleep(100);
    Assert.assertEquals(numHandlers, fpe.numHandlerInStack());
    Assert.assertEquals(0, fpe.numTasksInQueue());

    int i = 0;
    for (; i < 10; i++) {
      fpe.accept(() -> {
        try {
          semaphore.acquire();
        } catch (InterruptedException e) {
          // ignore
        }
        LOG.info("Counter " + counter.incrementAndGet());
        try {
          barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
          // nothing to do
        }
      });
    }
    Assert.assertEquals(0, fpe.numHandlerInStack());
    Assert.assertEquals(0, fpe.numTasksInQueue());

    for (; i < 100; i++) {
      fpe.accept(counter::incrementAndGet);
    }
    Assert.assertEquals(0, fpe.numHandlerInStack());
    Assert.assertEquals(90, fpe.numTasksInQueue());

    semaphore.release(10);

    Thread.sleep(1000);
    Assert.assertEquals(100, counter.get());

    Assert.assertEquals(10, fpe.numHandlerInStack());
    Assert.assertEquals(0, fpe.numTasksInQueue());

    fpe.stop();
  }

}
