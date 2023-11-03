/**
 *
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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SmallTests.class})
public class TestTableLocker {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTableLocker.class);

  @Test
  public void testTryLock() throws InterruptedException {
    TableLocker locker = new TableLocker();
    TableName tableName = TableName.valueOf("testTable");
    Semaphore flag = new Semaphore(0);
    Thread thread = new Thread(() -> {
      Lock lock = locker.getLock(tableName);
      if (lock.tryLock()) {
        flag.release();
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        } finally {
          lock.unlock();
        }
      }
    });

    Lock lock = locker.getLock(tableName);
    thread.start();
    // Wait the other thread lock the table.
    flag.acquire();

    assertFalse(lock.tryLock());
    Thread.sleep(1100);

    assertTrue(lock.tryLock());
    lock.unlock();
  }
}
