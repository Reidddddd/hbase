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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationSourceDummy;
import org.apache.hadoop.hbase.replication.ReplicationStateZKBase;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceManager.PeerConsumeStatus;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category(MediumTests.class)
public class TestReplicationSourceManager2 {
  
  private static final Log LOG = LogFactory.getLog(TestReplicationSourceManager2.class);
  
  private static Configuration conf;
  
  private static HBaseTestingUtility utility;
  
  private static Replication replication;
  
  private static ReplicationSourceManager manager;
  
  private static ZooKeeperWatcher zkw;
  
  private static FileSystem fs;
  
  private static Path oldLogDir;
  
  private static Path logDir;
  
  private static final Path path = new Path("test.12345");
  
  private static final String clusterKey = "localhost:1:/hbase";
  
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    
    conf = HBaseConfiguration.create();
    conf.set("replication.replicationsource.implementation",
        ReplicationSourceDummy.class.getCanonicalName());
    conf.setBoolean(HConstants.REPLICATION_ENABLE_KEY,
        HConstants.REPLICATION_ENABLE_DEFAULT);
    conf.setLong("replication.sleep.before.failover", 2000);
    conf.setInt("replication.source.maxretriesmultiplier", 10);
    utility = new HBaseTestingUtility(conf);
    utility.startMiniZKCluster();
    
    zkw = new ZooKeeperWatcher(conf,
        "testForReplicationSourceOptimize", null);
    ZKUtil.createWithParents(zkw, "/hbase/replication/peers");
    ZKUtil.createWithParents(zkw, "/hbase/replication/state");
    ZKUtil.setData(zkw, "/hbase/replication/state", ReplicationStateZKBase.ENABLED_ZNODE_BYTES);
    
    ZKClusterId.setClusterId(zkw, new ClusterId());
    FSUtils.setRootDir(utility.getConfiguration(), utility.getDataTestDir());
    fs = FileSystem.get(conf);
    oldLogDir = new Path(utility.getDataTestDir(), HConstants.HREGION_OLDLOGDIR_NAME);
    logDir = new Path(utility.getDataTestDir(), HConstants.HREGION_LOGDIR_NAME);
    replication = new Replication(new DummyServer(), fs, logDir, oldLogDir);
    manager = replication.getReplicationManager();
    manager.postLogRoll(path);
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    manager.join();
    utility.shutdownMiniCluster();
  }
  
  @Rule
  public TestName testName = new TestName();
  
  private void cleanLogDir() throws IOException {
    fs.delete(logDir, true);
    fs.delete(oldLogDir, true);
  }
  
  @Before
  public void setUp() throws Exception {
    LOG.info("Start " + testName.getMethodName());
    cleanLogDir();
  }
  
  @After
  public void tearDown() throws Exception {
    LOG.info("End " + testName.getMethodName());
    cleanLogDir();
  }
  
  @Test
  public void testMarkReplicationSourceWaitingDrain() throws Exception {
    final String tableAPeerMarker = "tableAPeerMarker";
    final String tableBPeerMarker = "tableBPeerMarker";
    // both means this peer has two tableCFs in the config
    final String tableABPeerMarker = "tableABPeerMarker";
    final String tableEmptyPeerMarker = "tableEmptyPeerMarker";
    
    TableName tableA =TableName.valueOf("table_a_marker");
    TableName tableB = TableName.valueOf("table_b_marker");
    
    ReplicationPeerConfig tableAConfig = new ReplicationPeerConfig().setClusterKey(clusterKey);
    Map<TableName, List<String>> tableACFs = new HashMap<>();
    tableACFs.put(tableA, new ArrayList<>());
    tableAConfig.setTableCFsMap(tableACFs);
    
    ReplicationPeerConfig tableBConfig = new ReplicationPeerConfig().setClusterKey(clusterKey);
    Map<TableName, List<String>> tableBCFs = new HashMap<>();
    tableBCFs.put(tableB, new ArrayList<>());
    tableBConfig.setTableCFsMap(tableBCFs);
    
    ReplicationPeerConfig tableABConfig = new ReplicationPeerConfig().setClusterKey(clusterKey);
    Map<TableName, List<String>> tableABCFs = new HashMap<>();
    tableABCFs.put(tableA, new ArrayList<>());
    tableABCFs.put(tableB, new ArrayList<>());
    tableABConfig.setTableCFsMap(tableABCFs);
    
    ReplicationPeerConfig tableEmptyConfig = new ReplicationPeerConfig().setClusterKey(clusterKey);
    try {
      manager.increaseOnlineRegionCount(tableA);
      manager.increaseOnlineRegionCount(tableB);
      addPeerAndWait(tableAPeerMarker, tableAConfig, true);
      addPeerAndWait(tableBPeerMarker, tableBConfig, true);
      addPeerAndWait(tableABPeerMarker, tableABConfig, true);
      addPeerAndWait(tableEmptyPeerMarker, tableEmptyConfig, true);
      
      // Only tablePeerA will become WAIT_DRAIN
      manager.decreaseOnlineRegionCount(tableA);
      assertTrue(manager.getSourcesWaitingDrainPaths().containsKey(tableAPeerMarker));
      assertFalse(manager.getSourcesWaitingDrainPaths().containsKey(tableBPeerMarker));
      assertFalse(manager.getSourcesWaitingDrainPaths().containsKey(tableABPeerMarker));
      assertFalse(manager.getSourcesWaitingDrainPaths().containsKey(tableEmptyPeerMarker));
      
      // Only tablePeerEmpty will be ONLINE
      manager.decreaseOnlineRegionCount(tableB);
      assertTrue(manager.getSourcesWaitingDrainPaths().containsKey(tableAPeerMarker));
      assertTrue(manager.getSourcesWaitingDrainPaths().containsKey(tableBPeerMarker));
      assertTrue(manager.getSourcesWaitingDrainPaths().containsKey(tableABPeerMarker));
      assertFalse(manager.getSourcesWaitingDrainPaths().containsKey(tableEmptyPeerMarker));
    } finally {
      removePeerAndWait(tableAPeerMarker);
      removePeerAndWait(tableBPeerMarker);
      removePeerAndWait(tableABPeerMarker);
      removePeerAndWait(tableEmptyPeerMarker);
      assertTrue(manager.getReplicationTables().isEmpty());
      assertTrue(manager.getTableOnlineRegionCount().isEmpty());
    }
  }
  
  @Test
  public void testCleanWaitingDrainSourceWhenMultiWALGroup() throws Exception {
    ReplicationPeers rps = manager.getReplicationPeers();
    final String multiWalPeer = "multiWalPeer";
    ReplicationPeerConfig peerConfig =
        new ReplicationPeerConfig().setClusterKey(clusterKey);
    TableName multiWalPeerTable = TableName.valueOf("multi_wal_peer_table");
    Map<TableName, List<String>> waitingDrainSourceTableCFs = new HashMap<>();
    waitingDrainSourceTableCFs.put(multiWalPeerTable, new ArrayList<>());
    peerConfig.setTableCFsMap(waitingDrainSourceTableCFs);
    Path path1 = new Path("test1.12345");
    Path path2 = new Path("test1.123456");
    Path path3 = new Path("test.123456");
    try {
      manager.postLogRoll(path1);
      addPeerAndWait(multiWalPeer, peerConfig, false);
      assertEquals(2, manager.getSourcesWaitingDrainPaths().get(multiWalPeer).size());

      manager.postLogRoll(path2);
      manager.postLogRoll(path3);
      assertTrue(manager.getSourcesWaitingDrainPaths().get(multiWalPeer)
          .containsAll(Arrays.asList(path, path1)));
      assertEquals(4, manager.getSourceMetrics(multiWalPeer).getSizeOfLogQueue());
      
      manager.logPositionAndCleanOldLogs(path2, multiWalPeer, 1L, false, false);
      assertTrue(manager.getSourcesWaitingDrainPaths().get(multiWalPeer).contains(path));
      manager.logPositionAndCleanOldLogs(path3, multiWalPeer, 1L, false, false);
      assertTrue(manager.getSourcesWaitingDrainPaths().get(multiWalPeer).isEmpty());

      Waiter.waitFor(conf, 20000, (Predicate<Exception>) () ->
          manager.getSourcePeerConsumeStatus(multiWalPeer).equals(PeerConsumeStatus.NOT_CONSUMING)
              && manager.getSource(multiWalPeer) == null
              && !manager.getAllQueues().contains(multiWalPeer)
              && rps.getAllPeerIds().contains(multiWalPeer)
      );
      assertEquals(0, manager.getSourceMetrics(multiWalPeer).getSizeOfLogQueue());
  
      // Test activate
      manager.increaseOnlineRegionCount(multiWalPeerTable);
      assertEquals(manager.getSourcePeerConsumeStatus(multiWalPeer), PeerConsumeStatus.CONSUMING);
    } finally {
      removePeerAndWait(multiWalPeer);
      assertTrue(manager.getReplicationTables().isEmpty());
      manager.decreaseOnlineRegionCount(multiWalPeerTable);
      assertTrue(manager.getTableOnlineRegionCount().isEmpty());
    }
  }
  
  @Test
  public void testActivateReplicationSourceWhenWaitingDrain() throws Exception {
    final String activatePeer = "activatePeer";
    
    TableName activateTable =TableName.valueOf("table_activator");
    Map<TableName, List<String>> activateTableCFs = new HashMap<>();
    activateTableCFs.put(activateTable, new ArrayList<>());
    ReplicationPeerConfig peerConfig =
        new ReplicationPeerConfig().setClusterKey(clusterKey);
    peerConfig.setTableCFsMap(activateTableCFs);
    try {
      /*
        Test for activate new online table when the source is waiting drain
      */
      addPeerAndWait(activatePeer, peerConfig, false);
      manager.increaseOnlineRegionCount(activateTable);
      assertFalse(manager.getSourcesWaitingDrainPaths().containsKey(activatePeer));
    } finally {
      removePeerAndWait(activatePeer);
      assertTrue(manager.getReplicationTables().isEmpty());
      manager.decreaseOnlineRegionCount(activateTable);
      assertTrue(manager.getTableOnlineRegionCount().isEmpty());
    }
  }
  
  @Test
  public void testAddAndRemoveRegionSameTime() throws Exception {
    String peerId = "testConcurrentPeer";
    TableName table =TableName.valueOf("test_concurrent");
    
    ReplicationPeerConfig peerConfig = new ReplicationPeerConfig().setClusterKey(clusterKey);
    Map<TableName, List<String>> tableCFs = new HashMap<>();
    tableCFs.put(table, new ArrayList<>());
    peerConfig.setTableCFsMap(tableCFs);
    try {
      /*
          Test when replication is online, the remove and add operation happen in the same time.
       */
      manager.increaseOnlineRegionCount(table);
      addPeerAndWait(peerId, peerConfig, true);
      new Thread(()->{
        manager.decreaseOnlineRegionCount(table);
      }).start();
      new Thread(()->{
        manager.increaseOnlineRegionCount(table);
      }).start();
      Waiter.waitFor(conf, 20000, (Predicate<Exception>) () ->
          manager.getSourcePeerConsumeStatus(peerId).equals(PeerConsumeStatus.CONSUMING)
              && manager.getSource(peerId) != null
              && manager.getAllQueues().contains(peerId)
              && !manager.getSourcesWaitingDrainPaths().containsKey(peerId)
      );
    } finally {
      removePeerAndWait(peerId);
      assertTrue(manager.getReplicationTables().isEmpty());
      manager.decreaseOnlineRegionCount(table);
      assertTrue(manager.getTableOnlineRegionCount().isEmpty());
    }
  }
  
  /**
   * Add a peer and wait for it to initialize
   * @param peerId peer cluster id to be added
   * @param peerConfig configuration for the replication slave cluster
   * @throws Exception If unable to add peer
   */
  private void addPeerAndWait(final String peerId,
                              final ReplicationPeerConfig peerConfig,
                              final boolean online) throws Exception {
    final ReplicationPeers rp = manager.getReplicationPeers();
    rp.addPeer(peerId, peerConfig);
    if (online) {
      Waiter.waitFor(conf, 20000, (Predicate<Exception>) () ->
          manager.getSource(peerId) != null
              && manager.getAllQueues().contains(peerId)
              && manager.getSourceMetrics(peerId) != null
              && manager.getSourcePeerConsumeStatus(peerId) != null
      );
    } else {
      Waiter.waitFor(conf, 20000, (Predicate<Exception>) () ->
          manager.getSource(peerId) != null
              && manager.getAllQueues().contains(peerId)
              && manager.getSourcesWaitingDrainPaths().containsKey(peerId)
              && manager.getSourceMetrics(peerId) != null
              && manager.getSourcePeerConsumeStatus(peerId) != null
      );
    }
  }
  
  /**
   * Remove a peer and wait for it to get cleaned up
   * @param peerId peer cluster id to be removed
   * @throws Exception If unable to remove peer
   */
  private void removePeerAndWait(final String peerId) throws Exception {
    final ReplicationPeers rp = manager.getReplicationPeers();
    if (rp.getAllPeerIds().contains(peerId)) {
      rp.removePeer(peerId);
    }
    Waiter.waitFor(conf, 20000, (Predicate<Exception>) () ->
        !manager.getAllQueues().contains(peerId)
            && rp.getPeer(peerId) == null
            && manager.getSource(peerId) == null
            && manager.getSourcePeerConsumeStatus(peerId) == null
            && manager.getSourceMetrics(peerId) == null
    );
  }
  
  static class DummyServer implements Server {
    String hostname;
    
    DummyServer() {
      hostname = "hostname.example.org";
    }
    
    @Override
    public Configuration getConfiguration() {
      return conf;
    }
    
    @Override
    public ZooKeeperWatcher getZooKeeper() {
      return zkw;
    }
    
    @Override
    public CoordinatedStateManager getCoordinatedStateManager() {
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
      return ServerName.valueOf(hostname, 1234, 1L);
    }
    
    @Override
    public void abort(String why, Throwable e) {
      // To change body of implemented methods use File | Settings | File Templates.
    }
    
    @Override
    public boolean isAborted() {
      return false;
    }
    
    @Override
    public void stop(String why) {
      // To change body of implemented methods use File | Settings | File Templates.
    }
    
    @Override
    public boolean isStopped() {
      return false; // To change body of implemented methods use File | Settings | File Templates.
    }
    
    @Override
    public ChoreService getChoreService() {
      return null;
    }
  }
}
