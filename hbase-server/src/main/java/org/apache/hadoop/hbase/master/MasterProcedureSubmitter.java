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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterBusyException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This is a class to run the master procedures.
 * It contains a {@link TableLocker} to run some table edit ops synchronously.
 * This class will return one IOE to the duplicate request on the same table.
 */
@InterfaceAudience.Private
public class MasterProcedureSubmitter {
  private static final Log LOG = LogFactory.getLog(MasterProcedureSubmitter.class);

  private static final String DDL_PARALLELISM_KEY = "hbase.master.ddl.parallelism";
  private static final int DEFAULT_DDL_PARALLELISM = 3;
  private static final String DDL_SEMAPHORE_TIMEOUT = "hbase.master.ddl.semaphore.timeout";
  private static final int DEFAULT_DDL_SEMAPHORE_TIMEOUT = 60000; // in Milliseconds.

  private final TableLocker tableLocker;
  private final Semaphore signal;
  private final int semaphoreTimeout;

  public MasterProcedureSubmitter(Configuration conf) {
    this.tableLocker = new TableLocker();
    this.signal = new Semaphore(conf.getInt(DDL_PARALLELISM_KEY, DEFAULT_DDL_PARALLELISM));
    this.semaphoreTimeout = conf.getInt(DDL_SEMAPHORE_TIMEOUT, DEFAULT_DDL_SEMAPHORE_TIMEOUT);
  }

  @VisibleForTesting
  public MasterProcedureSubmitter(TableLocker tableLocker, int parallelism, int timeout) {
    this.tableLocker = tableLocker;
    this.signal = new Semaphore(parallelism);
    this.semaphoreTimeout = timeout;
  }

  /**
   * @param task The procedure to run
   * @param tableName The table name to check existence and lock. Could be null.
   * @return The procedure id.
   * @throws IOException Exception threw in the task.
   */
  public long submitProcedure(MasterProcedureUtil.NonceProcedureRunnable task, TableName tableName)
    throws IOException {
    ReentrantLock lock = tableLocker.getLock(tableName);
    if (lock.tryLock()) {
      try {
        if (signal.tryAcquire(this.semaphoreTimeout, TimeUnit.MILLISECONDS)) {
          try {
            // This is running synchronously.
            return MasterProcedureUtil.submitProcedure(task);
          } finally {
            signal.release();
          }
        } else {
          throw new MasterBusyException("Failed acquire master execution signal with timeout : "
            + this.semaphoreTimeout + " ms, master is busy.");
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      } finally {
        lock.unlock();
      }
    } else {
      throw new IOException("Failed acquire lock for table: " + tableName);
    }
  }
}
