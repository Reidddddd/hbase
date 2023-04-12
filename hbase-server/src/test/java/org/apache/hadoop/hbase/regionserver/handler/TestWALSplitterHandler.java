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
package org.apache.hadoop.hbase.regionserver.handler;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.doNothing;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.coordination.SplitLogWorkerCoordination;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.regionserver.SplitLogWorker;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test of the {@link WALSplitterHandler}.
 */
@Category(SmallTests.class)
public class TestWALSplitterHandler {

  static class DummyServer implements Server {
    private boolean aborted = false;

    @Override
    public void abort(String why, Throwable e) {
      aborted = true;
    }

    @Override
    public boolean isAborted() {
      return aborted;
    }

    @Override
    public void stop(String why) {

    }

    @Override
    public boolean isStopped() {
      return false;
    }

    @Override
    public Configuration getConfiguration() {
      return HBaseConfiguration.create();
    }

    @Override
    public ZooKeeperWatcher getZooKeeper() {
      return null;
    }

    @Override
    public ClusterConnection getConnection() {
      return null;
    }

    @Override
    public MetaTableLocator getMetaTableLocator() {
      return null;
    }

    @Override
    public ServerName getServerName() {
      return null;
    }

    @Override
    public CoordinatedStateManager getCoordinatedStateManager() {
      return null;
    }

    @Override
    public ChoreService getChoreService() {
      return null;
    }
  }

  SplitLogWorker.TaskExecutor exceptionalExecutor =
    new SplitLogWorker.TaskExecutor() {
      @Override
      public Status exec(String name, ZooKeeperProtos.SplitLogTask.RecoveryMode mode,
          CancelableProgressable p) {
        throw new RuntimeException("Runtime exception for test.");
      }
    };

  @Test
  public void testRSNotAbortWhenRuntimeException() throws IOException {
    DummyServer server = new DummyServer();
    SplitLogWorkerCoordination coordination = Mockito.mock(SplitLogWorkerCoordination.class);
    doNothing().when(coordination).endTask(Mockito.any(), Mockito.any(), Mockito.any());

    WALSplitterHandler handler = new WALSplitterHandler(server, coordination,
      new SplitLogWorkerCoordination.SplitTaskDetails() {
        @Override
        public String getWALFile() {
          return "testFile";
        }
      }, null, new AtomicInteger(), exceptionalExecutor, null);
    handler.run();
    assertFalse(server.isAborted());
  }
}
