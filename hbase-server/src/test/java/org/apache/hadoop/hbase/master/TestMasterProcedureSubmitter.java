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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

@Category({ SmallTests.class})
public class TestMasterProcedureSubmitter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMasterProcedureSubmitter.class);


  @Test
  public void testTryLockFailed() {
    TableName tableName = TableName.valueOf("testTryLock");

    TableLocker tableLocker = Mockito.mock(TableLocker.class);
    ReentrantLock mockLock = Mockito.mock(ReentrantLock.class);
    when(tableLocker.getLock(tableName)).thenReturn(mockLock);
    when(mockLock.tryLock()).thenReturn(false);

    MasterProcedureSubmitter submitter = new MasterProcedureSubmitter(tableLocker);
    MasterProcedureUtil.NonceProcedureRunnable task1 =
      Mockito.mock(MasterProcedureUtil.NonceProcedureRunnable.class);

    try {
      submitter.submitProcedure(task1, tableName);
      fail("Should fail when try lock failed.");
    } catch (IOException ioe) {
      assertTrue(ioe.getMessage().contains("Failed acquire lock for table:"));
    }
  }

  @Test
  // TODO Have to use mockito-inline if we want to mockStatic, However,
  //  there are compatibility issues between mockito-incline and other existing individual UTs,
  //  which need to be resolved later.
  @Ignore
  public void testUnlockTable() {
    TableName tableName = TableName.valueOf("testUnlockTable");
    TableLocker tableLocker = new TableLocker();

    MasterProcedureSubmitter submitter = new MasterProcedureSubmitter(tableLocker);
    MasterProcedureUtil.NonceProcedureRunnable task1 =
      Mockito.mock(MasterProcedureUtil.NonceProcedureRunnable.class);

    MockedStatic<MasterProcedureUtil> mockedStatic = Mockito.mockStatic(MasterProcedureUtil.class);
    mockedStatic.when(() -> MasterProcedureUtil.submitProcedure(
      Mockito.any(MasterProcedureUtil.NonceProcedureRunnable.class)))
      .thenThrow(new IOException("Test exception"));

    try {
      submitter.submitProcedure(task1, tableName);
      fail("Should throw exception here.");
    } catch(IOException ioe) {
      // The table should be unlocked.
      assertFalse(tableLocker.getLock(tableName).isLocked());
    }
  }
}
