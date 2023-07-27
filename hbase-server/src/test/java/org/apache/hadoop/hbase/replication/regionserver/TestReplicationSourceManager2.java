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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
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
import org.apache.hadoop.hbase.MockRegionServerServices;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationSourceDummy;
import org.apache.hadoop.hbase.replication.ReplicationStateZKBase;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceManager.CheckType;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
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

  private static final Log LOG =
      LogFactory.getLog(TestReplicationSourceManager2.class);

  private static Configuration conf;

  private static HBaseTestingUtility utility;

  private static Replication replication;

  private static ReplicationSourceManager manager;

  private static ZooKeeperWatcher zkw;

  private static final String slaveId = "1";

  private static FileSystem fs;

  private static Path oldLogDir;

  private static Path logDir;

  private static DummyRegionServerServices dummyRegionServerServices;

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
    ZKUtil.createWithParents(zkw, "/hbase/replication");
    ZKUtil.createWithParents(zkw, "/hbase/replication/peers/1");

    ZKUtil.setData(zkw, "/hbase/replication/peers/1",
        Bytes.toBytes(clusterKey));
    ZKUtil.createWithParents(zkw, "/hbase/replication/peers/1/peer-state");
    ZKUtil.setData(zkw, "/hbase/replication/peers/1/peer-state",
        ReplicationStateZKBase.ENABLED_ZNODE_BYTES);
    ZKUtil.createWithParents(zkw, "/hbase/replication/state");
    ZKUtil.setData(zkw, "/hbase/replication/state", ReplicationStateZKBase.ENABLED_ZNODE_BYTES);

    ZKClusterId.setClusterId(zkw, new ClusterId());
    FSUtils.setRootDir(utility.getConfiguration(), utility.getDataTestDir());
    fs = FileSystem.get(conf);
    oldLogDir = new Path(utility.getDataTestDir(),
        HConstants.HREGION_OLDLOGDIR_NAME);
    logDir = new Path(utility.getDataTestDir(),
        HConstants.HREGION_LOGDIR_NAME);
    dummyRegionServerServices = new DummyRegionServerServices();
    replication =
        new Replication(dummyRegionServerServices, fs, logDir, oldLogDir);
    manager = replication.getReplicationManager();
    manager.postLogRoll(path);
    manager.addSource(slaveId);
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
  public void testMarkReplicationSourceAsNeedRemove() throws Exception {
    final String tablePeerA = "tablePeer_a";
    final String tablePeerB = "tablePeer_b";
    final String tablePeerEmpty = "tablePeer_empty";
    //both means this peer has two tableCFs in the config
    final String tablePeerBoth = "tablePeer_both";

    TableName tableA =TableName.valueOf("table_a_for_cleaner");
    TableName tableB = TableName.valueOf("table_b_for_cleaner");
    Path newPath = new Path("test1.123456");

    ReplicationPeerConfig tableAConfig = new ReplicationPeerConfig().setClusterKey(clusterKey);
    Map<TableName, List<String>> tableACFs = new HashMap<>();
    tableACFs.put(tableA, new ArrayList<>());
    tableAConfig.setTableCFsMap(tableACFs);

    ReplicationPeerConfig tableBConfig = new ReplicationPeerConfig().setClusterKey(clusterKey);
    Map<TableName, List<String>> tableBCFs = new HashMap<>();
    tableBCFs.put(tableB, new ArrayList<>());
    tableBConfig.setTableCFsMap(tableBCFs);

    ReplicationPeerConfig tableEmptyConfig = new ReplicationPeerConfig().setClusterKey(clusterKey);

    ReplicationPeerConfig tableBothConfig = new ReplicationPeerConfig().setClusterKey(clusterKey);
    Map<TableName, List<String>> tableBothCFs = new HashMap<>();
    tableBothCFs.put(tableA, new ArrayList<>());
    tableBothCFs.put(tableB, new ArrayList<>());
    tableBothConfig.setTableCFsMap(tableBothCFs);
    try {
      dummyRegionServerServices.addOnlineTable(tableA);
      dummyRegionServerServices.addOnlineTable(tableB);
      addPeerAndWait(tablePeerA, tableAConfig);
      addPeerAndWait(tablePeerB, tableBConfig);
      addPeerAndWait(tablePeerEmpty, tableEmptyConfig);
      addPeerAndWait(tablePeerBoth, tableBothConfig);
      
      //Only tablePeerA will have lastPositions
      //And the CheckType will be SKIP
      dummyRegionServerServices.removeOnlineTable(tableA);
      manager.markReplicationSourceAsNeedRemove(tableA);
      assertTrue(manager.getLastPositionsById().containsKey(tablePeerA));
      assertEquals(CheckType.SKIP,
          manager.getLastPositionsById().get(tablePeerA).getFirst());
      assertFalse(manager.getLastPositionsById().containsKey(tablePeerB));
      assertFalse(manager.getLastPositionsById().containsKey(tablePeerBoth));
      assertFalse(manager.getLastPositionsById().containsKey(tablePeerEmpty));
      manager.getLastPositionsById().clear();
      
      //Test the double check in the cleanUnusedSource have the same result
      //But the CheckType will be NOT_SKIP
      manager.cleanUnusedSource();
      assertTrue(manager.getLastPositionsById().containsKey(tablePeerA));
      assertEquals(CheckType.NOT_SKIP,
          manager.getLastPositionsById().get(tablePeerA).getFirst());
      assertFalse(manager.getLastPositionsById().containsKey(tablePeerB));
      assertFalse(manager.getLastPositionsById().containsKey(tablePeerBoth));
      assertFalse(manager.getLastPositionsById().containsKey(tablePeerEmpty));
      
      //Only tablePeerEmpty will ont have lastPositions
      dummyRegionServerServices.removeOnlineTable(tableB);
      manager.markReplicationSourceAsNeedRemove(tableB);
      assertTrue(manager.getLastPositionsById().containsKey(tablePeerA));
      assertTrue(manager.getLastPositionsById().containsKey(tablePeerB));
      assertTrue(manager.getLastPositionsById().containsKey(tablePeerBoth));
      assertFalse(manager.getLastPositionsById().containsKey(tablePeerEmpty));
      manager.getLastPositionsById().clear();
  
      //Test the double check in the cleanUnusedSource.
      //Only tablePeerEmpty will ont have lastPositions.
      manager.cleanUnusedSource();
      assertTrue(manager.getLastPositionsById().containsKey(tablePeerA));
      assertTrue(manager.getLastPositionsById().containsKey(tablePeerB));
      assertTrue(manager.getLastPositionsById().containsKey(tablePeerBoth));
      assertFalse(manager.getLastPositionsById().containsKey(tablePeerEmpty));
      manager.getLastPositionsById().clear();
      
      //test for mark twice and the lastPositions won't change
      ((ReplicationSourceDummy) manager.getSource(tablePeerB)).putPosition("test", 0L);
      manager.markReplicationSourceAsNeedRemove(tableB);
      Map<String, Long> map =
          new HashMap<>(manager.getLastPositionsById().get(tablePeerB).getSecond());
      ((ReplicationSourceDummy) manager.getSource(tablePeerB)).putPosition("test", 1L);
      manager.markReplicationSourceAsNeedRemove(tableB);
      assertEquals(map, manager.getLastPositionsById().get(tablePeerB).getSecond());
      assertNotEquals(map, manager.getSource(tablePeerB).getLastPositions());
      
      //test postLogRoll
      Map<String, SortedSet<String>> peerBWALs =
          new HashMap<>(manager.getWALs().get(tablePeerB));
      Map<String, SortedSet<String>> peerEmptyWALs =
          new HashMap<>(manager.getWALs().get(tablePeerEmpty));
      int peerBQueueSize =
          manager.getSource(tablePeerB).getSourceMetrics().getSizeOfLogQueue();
      int peerEmptyQueueSize =
          manager.getSource(tablePeerEmpty).getSourceMetrics().getSizeOfLogQueue();
      String logName = newPath.getName();
      String logPrefix = DefaultWALProvider.getWALPrefixFromWALName(logName);
      manager.postLogRoll(newPath);
      assertEquals(peerBWALs, manager.getWALs().get(tablePeerB));
      assertEquals(peerBQueueSize,
          manager.getSource(tablePeerB).getSourceMetrics().getSizeOfLogQueue());
      assertNotEquals(peerEmptyWALs, manager.getWALs().get(tablePeerEmpty));
      assertEquals(peerEmptyQueueSize + 1,
          manager.getSource(tablePeerEmpty).getSourceMetrics().getSizeOfLogQueue());
      assertTrue(manager.getLatestPaths().contains(newPath));
      assertFalse(manager.getSource(tablePeerB).getQueues().containsKey(logPrefix));
      assertTrue(manager.getSource(tablePeerEmpty).getQueues().containsKey(logPrefix));
    } finally {
      manager.getLatestPaths().remove(newPath);
      removePeerAndWait(tablePeerA);
      removePeerAndWait(tablePeerB);
      removePeerAndWait(tablePeerBoth);
      removePeerAndWait(tablePeerEmpty);
    }
  }

  @Test
  public void testCleanUnusedSource() throws Exception {
    ReplicationPeers rps = manager.getReplicationPeers();
    final String positionPeer = "positionTestPeer";
    ReplicationPeerConfig peerConfig =
        new ReplicationPeerConfig().setClusterKey(clusterKey);
    TableName positionTable = TableName.valueOf("position_table");
    Map<TableName, List<String>> positionTestTableCFs = new HashMap<>();
    positionTestTableCFs.put(positionTable, new ArrayList<>());
    peerConfig.setTableCFsMap(positionTestTableCFs);
    try {
      dummyRegionServerServices.addOnlineTable(positionTable);
      addPeerAndWait(positionPeer, peerConfig);
  
      dummyRegionServerServices.removeOnlineTable(positionTable);
      ((ReplicationSourceDummy) manager.getSource(positionPeer)).putPosition("test", 0L);
      manager.markReplicationSourceAsNeedRemove(positionTable);
      Map<String, Long> map =
          new HashMap<>(manager.getLastPositionsById().get(positionPeer).getSecond());
      assertTrue(manager.getLastPositionsById().containsKey(positionPeer));
      
      //Test when lastPosition is first added and the record is changed.
      //The source won't be removed but the lastPositionById will be changed
      ((ReplicationSourceDummy) manager.getSource(positionPeer)).putPosition("test", 1L);
      manager.cleanUnusedSource();
      assertTrue(manager.getLastPositionsById().containsKey(positionPeer));
      assertNotEquals(map, manager.getLastPositionsById().get(positionPeer).getSecond());
      assertEquals(CheckType.NOT_SKIP, manager.getLastPositionsById().get(positionPeer).getFirst());
  
      //Test when lastPosition is not first added and the record is changed.
      //The source won't be removed but the lastPositionById will be changed
      ((ReplicationSourceDummy) manager.getSource(positionPeer)).putPosition("test", 2L);
      map = new HashMap<>(manager.getLastPositionsById().get(positionPeer).getSecond());
      manager.cleanUnusedSource();
      assertTrue(manager.getLastPositionsById().containsKey(positionPeer));
      assertNotEquals(map, manager.getLastPositionsById().get(positionPeer).getSecond());
      assertEquals(CheckType.NOT_SKIP, manager.getLastPositionsById().get(positionPeer).getFirst());
      
      //Test when the record of lastPosition is not changed. The source will be removed
      manager.cleanUnusedSource();
      assertFalse(manager.getAllQueues().contains(positionPeer));
      assertFalse(manager.getLastPositionsById().containsKey(positionPeer));
      assertTrue(rps.getPeerIds().contains(positionPeer));
      assertNull(manager.getSource(positionPeer));
      assertFalse(manager.getWALs().containsKey(positionPeer));
    } finally {
      removePeerAndWait(positionPeer);
    }
  }

  /**
   * Add a peer and wait for it to initialize
   * @param peerId peer cluster id to be added
   * @param peerConfig configuration for the replication slave cluster
   * @throws Exception If unable to add peer
   */
  private void addPeerAndWait(final String peerId,
                              final ReplicationPeerConfig peerConfig) throws Exception {
    final ReplicationPeers rp = manager.getReplicationPeers();
    rp.addPeer(peerId, peerConfig);
    Waiter.waitFor(conf, 20000, (Predicate<Exception>) () ->
        manager.getSource(peerId) != null && manager.getAllQueues().contains(peerId)
    );
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
    Waiter.waitFor(conf, 20000, (Predicate<Exception>) () -> {
      List<String> peers = rp.getAllPeerIds();
      return (!manager.getAllQueues().contains(peerId))
          && (rp.getPeer(peerId) == null)
          && (!peers.contains(peerId))
          && (!manager.getLastPositionsById().containsKey(peerId));
    });
  }

  static class DummyRegionServerServices extends MockRegionServerServices {

    private final Set<TableName> onlineTableSet = new HashSet<TableName>();
    String hostname;

    DummyRegionServerServices() {
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

    @Override
    public Set<TableName> getOnlineTables() {
      return onlineTableSet;
    }

    public void removeOnlineTable(TableName tableName) {
      onlineTableSet.remove(tableName);
    }

    public void addOnlineTable(TableName tableName) {
      onlineTableSet.add(tableName);
    }
  }
}
