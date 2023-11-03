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

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a class to run the master procedures.
 * It contains a {@link TableLocker} to run some table edit ops synchronously.
 * This class will return one IOE to the duplicate request on the same table.
 */
@InterfaceAudience.Private
public class MasterProcedureSubmitter {
  private static final Logger LOG = LoggerFactory.getLogger(MasterProcedureSubmitter.class);

  private final TableLocker tableLocker;

  public MasterProcedureSubmitter() {
    this.tableLocker = new TableLocker();
  }

  public MasterProcedureSubmitter(TableLocker tableLocker) {
    this.tableLocker = tableLocker;
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
        // This is running synchronously.
        return MasterProcedureUtil.submitProcedure(task);
      } finally {
        lock.unlock();
      }
    } else {
      throw new IOException("Failed acquire lock for table: " + tableName);
    }
  }
}
