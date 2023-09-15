/*
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
package org.apache.hadoop.hbase.replication;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.replication.regionserver.Replication;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceManager;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestReplicationEnableFailover {

  private static final Log LOG =
    LogFactory.getLog(TestReplicationEnableFailover.class);
  private final static HBaseTestingUtility TEST_UTIL =
    new HBaseTestingUtility();
  private final static HBaseTestingUtility TEST_UTIL_PEER =
    new HBaseTestingUtility();
  private static FileSystem FS;
  private static Path oldLogDir;
  private static Path logDir;
  private static Configuration conf = TEST_UTIL.getConfiguration();

  /**
   * @throws java.lang.Exception Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniDFSCluster(1);
    FS = TEST_UTIL.getDFSCluster().getFileSystem();
    Path rootDir = TEST_UTIL.createRootDir();
    oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    if (FS.exists(oldLogDir)) {
      FS.delete(oldLogDir, true);
    }
    logDir = new Path(rootDir, HConstants.HREGION_LOGDIR_NAME);
    if (FS.exists(logDir)) {
      FS.delete(logDir, true);
    }
  }

  @Before
  public void setup() throws IOException {
    if (!FS.exists(logDir)) {
      FS.mkdirs(logDir);
    }
    if (!FS.exists(oldLogDir)) {
      FS.mkdirs(oldLogDir);
    }

    TestReplicationEndpoint.ReplicationEndpointForTest.contructedCount.set(0);
    TestReplicationEndpoint.ReplicationEndpointForTest.startedCount.set(0);
    TestReplicationEndpoint.ReplicationEndpointForTest.replicateCount.set(0);
    TestReplicationEndpoint.ReplicationEndpointForTest.stoppedCount.set(0);
    TestReplicationEndpoint.ReplicationEndpointForTest.lastEntries = null;
  }

  @After
  public void tearDown() throws IOException {
    if (FS.exists(oldLogDir)) {
      FS.delete(oldLogDir, true);
    }
    if (FS.exists(logDir)) {
      FS.delete(logDir, true);
    }
    conf.setBoolean("replication.source.eof.autorecovery", true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL_PEER.shutdownMiniHBaseCluster();
    TEST_UTIL.shutdownMiniHBaseCluster();
    TEST_UTIL.shutdownMiniDFSCluster();
  }

  @Test
  public void testEnableFailover() throws Exception {
    java.nio.file.Path cnfPath = FileSystems.getDefault()
      .getPath("target/test-classes/hbase-site.xml");
    java.nio.file.Path cnf2Path = FileSystems.getDefault()
      .getPath("target/test-classes/hbase-site-replication-enable-failover.xml");
    java.nio.file.Path cnf3Path = FileSystems.getDefault()
      .getPath("target/test-classes/hbase-site-backup.xml");
    java.nio.file.Path cnf4Path = FileSystems.getDefault()
      .getPath("target/test-classes/hbase-site-replication-disable-failover.xml");

    // make a backup of hbase-site.xml
    Files.copy(cnfPath, cnf3Path, StandardCopyOption.REPLACE_EXISTING);
    try {
      // update hbase-site.xml by overwriting it
      Files.copy(cnf4Path, cnfPath, StandardCopyOption.REPLACE_EXISTING);

      conf = TEST_UTIL.getConfiguration();

      // Ensure single-threaded WAL
      conf.set("hbase.wal.provider", "defaultProvider");
      conf.setInt("replication.sleep.before.failover", 2000);

      // Introduces a delay in regionserver shutdown to give the race condition a chance to kick in.
      conf.set(HConstants.REGION_SERVER_IMPL,
        TestReplicationSource.ShutdownDelayRegionServer.class.getName());
      MiniHBaseCluster cluster = TEST_UTIL.startMiniCluster(2);
      TEST_UTIL_PEER.startMiniCluster(1);

      HRegionServer serverA = cluster.getRegionServer(0);
      final ReplicationSourceManager managerA =
        ((Replication) serverA.getReplicationSourceService()).getReplicationManager();
      HRegionServer serverB = cluster.getRegionServer(1);
      final ReplicationSourceManager managerB =
        ((Replication) serverB.getReplicationSourceService()).getReplicationManager();
      final ReplicationAdmin replicationAdmin = new ReplicationAdmin(TEST_UTIL.getConfiguration());

      Assert.assertEquals(false,
        serverB.getConfiguration().getBoolean("replication.source.rs.enable.failover", true));

      final String peerId = "TestPeer";
      replicationAdmin.addPeer(peerId,
        new ReplicationPeerConfig().setClusterKey(TEST_UTIL_PEER.getClusterKey()), null);
      // Wait for replication sources to come up
      Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return !(managerA.getSources().isEmpty() || managerB.getSources().isEmpty());
        }
      });
      // Disabling peer makes sure there is at least one log to claim when the server dies
      // The recovered queue will also stay there until the peer is disabled even if the
      // WALs it contains have no data.
      replicationAdmin.disablePeer(peerId);

      // Stopping serverA
      // Its queues should not be claimed by the only other alive server because we set
      // enableFailover false.
      cluster.stopRegionServer(serverA.getServerName());
      Assert.assertEquals(0, managerB.getOldSources().size());
      Thread.sleep(20000);
      Assert.assertEquals(0, managerB.getOldSources().size());

      // update hbase-site.xml by overwriting it
      Files.copy(cnf2Path, cnfPath, StandardCopyOption.REPLACE_EXISTING);
      TEST_UTIL.getHBaseAdmin().updateConfiguration(serverB.getServerName());
      Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return managerB.getOldSources().size() == 1;
        }
      });

      replicationAdmin.enablePeer(peerId);
      Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return managerB.getOldSources().size() == 0;
        }
      });
    } finally {
      Files.copy(cnf3Path, cnfPath, StandardCopyOption.REPLACE_EXISTING);
    }
  }
}
