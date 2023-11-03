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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.cache.Cache;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;


@InterfaceAudience.Private
public class TableLocker {
  private static final Logger LOG = LoggerFactory.getLogger(TableLocker.class);

  private final Cache<TableName, ReentrantLock> lockCache;
  private final Lock lock = new ReentrantLock();

  public TableLocker() {
    lockCache = CacheBuilder.newBuilder()
      .expireAfterAccess(2, TimeUnit.HOURS)
      .build();
  }

  public ReentrantLock getLock(TableName tableName) {
    ReentrantLock tableLock = null;
    try {
      lock.lock();
      tableLock = lockCache.getIfPresent(tableName);
      if (tableLock == null) {
        tableLock = new ReentrantLock();
        lockCache.put(tableName, tableLock);
      }
      return tableLock;
    } finally {
      lock.unlock();
    }
  }
}
