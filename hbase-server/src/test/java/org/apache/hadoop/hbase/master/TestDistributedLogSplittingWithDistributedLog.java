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

import static org.apache.hadoop.hbase.SplitLogCounters.tot_mgr_wait_for_zk_delete;
import static org.apache.hadoop.hbase.regionserver.HRegion.HREGION_WAL_REPLAYER_CLASS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.distributedlog.shaded.AppendOnlyStreamWriter;
import org.apache.distributedlog.shaded.DLMTestUtil;
import org.apache.distributedlog.shaded.TestDistributedLogBase;
import org.apache.distributedlog.shaded.api.DistributedLogManager;
import org.apache.distributedlog.shaded.api.namespace.Namespace;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.SplitLogCounters;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coordination.ZKSplitLogManagerCoordination;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.regionserver.DistributedLogSplitWorker;
import org.apache.hadoop.hbase.regionserver.DistributedLogWALReplayer;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.SplitLogWorker;
import org.apache.hadoop.hbase.regionserver.WALReplayer;
import org.apache.hadoop.hbase.regionserver.wal.DistributedLogAccessor;
import org.apache.hadoop.hbase.regionserver.wal.DistributedLogReader;
import org.apache.hadoop.hbase.regionserver.wal.DistributedLogWriter;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.DistributedLogWALProvider;
import org.apache.hadoop.hbase.wal.Reader;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.hadoop.hbase.wal.WALSplitterUtil;
import org.apache.hadoop.hbase.wal.WALUtils;
import org.apache.hadoop.hbase.wal.Writer;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ LargeTests.class})
public class TestDistributedLogSplittingWithDistributedLog extends TestDistributedLogBase {
  private static final Log LOG =
    LogFactory.getLog(TestDistributedLogSplittingWithDistributedLog.class);

  // Start a cluster with 2 masters and 6 regionservers
  static final int NUM_MASTERS = 2;
  static final int NUM_RS = 6;

  MiniHBaseCluster cluster;
  HMaster master;
  Configuration conf;
  URI uri;
  Namespace namespace;

  static Configuration originalConf;
  static HBaseTestingUtility TEST_UTIL;
  static MiniDFSCluster dfsCluster;
  static MiniZooKeeperCluster zkCluster;

  @BeforeClass
  public static void setupClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility(HBaseConfiguration.create());
    dfsCluster = TEST_UTIL.startMiniDFSCluster(1);
    zkCluster = TEST_UTIL.startMiniZKCluster();
    originalConf = TEST_UTIL.getConfiguration();
  }

  @AfterClass
  public static void tearDownClass() throws IOException {
    TEST_UTIL.shutdownMiniZKCluster();
    TEST_UTIL.shutdownMiniDFSCluster();
    TEST_UTIL.shutdownMiniHBaseCluster();
  }

  @Before
  public void before() throws Exception {
    // refresh configuration
    conf = HBaseConfiguration.create(originalConf);

    uri = DLMTestUtil.createDLMURI(zkPort, "");
    ensureURICreated(uri);

    // The following two DistributedLog related parameters are initialized by the super class.
    // We just copy them to our hbase configuration.
    conf.set("distributedlog.znode.parent", "/messaging/distributedlog");
    conf.set("distributedlog.zk.quorum", zkServers);
    conf.setClass("hbase.regionserver.hlog.writer.impl", DistributedLogWriter.class,
      Writer.class);
    conf.setClass("hbase.regionserver.hlog.reader.impl", DistributedLogReader.class,
      Reader.class);
    conf.setClass("hbase.wal.provider", DistributedLogWALProvider.class, WALProvider.class);
    conf.setClass("hbase.wal.meta_provider", DistributedLogWALProvider.class, WALProvider.class);
    conf.setClass("hbase.split.log.worker.class", DistributedLogSplitWorker.class,
      SplitLogWorker.class);
    conf.setClass("hbase.split.log.manager.class", DistributedLogSplitManager.class,
      SplitLogManager.class);
    conf.setClass("hbase.split.log.helper.class", DistributedLogSplitHelper.class,
      AbstractSplitLogHelper.class);
    conf.setClass(HREGION_WAL_REPLAYER_CLASS, DistributedLogWALReplayer.class, WALReplayer.class);

    this.namespace = DistributedLogAccessor.getInstance(conf).getNamespace();
  }

  @After
  public void after() throws Exception {
    try {
      if (TEST_UTIL.getHBaseCluster() != null) {
        for (JVMClusterUtil.MasterThread mt : TEST_UTIL.getHBaseCluster().getLiveMasterThreads()) {
          mt.getMaster().abort("closing...", null);
        }
      }
      TEST_UTIL.shutdownMiniHBaseCluster();
    } finally {
      TEST_UTIL.getTestFileSystem().delete(FSUtils.getRootDir(TEST_UTIL.getConfiguration()), true);
      ZKUtil.deleteNodeRecursively(TEST_UTIL.getZooKeeperWatcher(), "/hbase");
    }
  }

  private void startCluster(int num_rs) throws Exception {
    SplitLogCounters.resetCounters();
    LOG.info("Starting cluster");
    conf.getLong("hbase.splitlog.max.resubmit", 0);
    // Make the failure test faster
    conf.setInt("zookeeper.recovery.retry", 0);
    conf.setInt(HConstants.REGIONSERVER_INFO_PORT, -1);
    conf.setFloat(HConstants.LOAD_BALANCER_SLOP_KEY, (float) 100.0); // no load balancing
    conf.setInt("hbase.regionserver.wal.max.splitters", 3);
    conf.setInt(HConstants.REGION_SERVER_HIGH_PRIORITY_HANDLER_COUNT, 10);
    TEST_UTIL.shutdownMiniHBaseCluster();
    TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.setDFSCluster(dfsCluster);
    TEST_UTIL.setZkCluster(zkCluster);
    TEST_UTIL.startMiniHBaseCluster(NUM_MASTERS, num_rs);
    cluster = TEST_UTIL.getHBaseCluster();
    LOG.info("Waiting for active/ready master");
    cluster.waitForActiveAndReadyMaster();
    master = cluster.getMaster();
    while (cluster.getLiveRegionServerThreads().size() < num_rs) {
      Threads.sleep(10);
    }
  }

  @Test(timeout=300000)
  public void testThreeRSAbort() throws Exception {
    LOG.info("testThreeRSAbort");
    final int NUM_REGIONS_TO_CREATE = 40;
    final int NUM_ROWS_PER_REGION = 100;

    startCluster(NUM_RS); // NUM_RS=6.

    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "distributed log splitting test",
      null);

    Table ht = installTable(zkw, "table", "family", NUM_REGIONS_TO_CREATE);
    try {
      populateDataInTable(NUM_ROWS_PER_REGION, "family");

      List<JVMClusterUtil.RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
      assertEquals(NUM_RS, rsts.size());
      rsts.get(0).getRegionServer().abort("testing");
      rsts.get(1).getRegionServer().abort("testing");
      rsts.get(2).getRegionServer().abort("testing");

      long start = EnvironmentEdgeManager.currentTime();
      while (cluster.getLiveRegionServerThreads().size() > (NUM_RS - 3)) {
        if (EnvironmentEdgeManager.currentTime() - start > 60000) {
          fail();
        }
        Thread.sleep(200);
      }

      start = EnvironmentEdgeManager.currentTime();
      while (HBaseTestingUtility.getAllOnlineRegions(cluster).size()
        < (NUM_REGIONS_TO_CREATE + 1)) {
        if (EnvironmentEdgeManager.currentTime() - start > 60000) {
          fail("Timedout");
        }
        Thread.sleep(200);
      }

      // wait for all regions are fully recovered
      TEST_UTIL.waitFor(180000, 200, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          List<String> recoveringRegions = zkw.getRecoverableZooKeeper().getChildren(
            zkw.recoveringRegionsZNode, false);
          return (recoveringRegions != null && recoveringRegions.size() == 0);
        }
      });

      // Waiting for the regions online.
      assertEquals(NUM_REGIONS_TO_CREATE * NUM_ROWS_PER_REGION, TEST_UTIL.countRows(ht));
    } finally {
      if (ht != null) {
        ht.close();
      }
      zkw.close();
    }
  }

  @Test(timeout=30000)
  public void testDelayedDeleteOnFailure() throws Exception {
    LOG.info("testDelayedDeleteOnFailure");
    startCluster(1);
    final SplitLogManager slm = master.getMasterFileSystem().splitLogManager;
    final FileSystem fs = master.getMasterFileSystem().getFileSystem();
    Path logDir = new Path("x");
    ExecutorService executor = null;
    try {
      final Path corruptedLogFile = new Path(logDir, "x");
      DistributedLogManager dlm =
        namespace.openLog(WALUtils.pathToDistributedLogName(corruptedLogFile));
      AppendOnlyStreamWriter writer = dlm.getAppendOnlyStreamWriter();
      writer.write(new byte[]{0});
      writer.write(Bytes.toBytes("corrupted bytes"));
      writer.close();

      ZKSplitLogManagerCoordination coordination =
        (ZKSplitLogManagerCoordination) master.getCoordinatedStateManager()
          .getSplitLogManagerCoordination();
      coordination.setIgnoreDeleteForTesting(true);
      executor = Executors.newSingleThreadExecutor();
      Runnable runnable = () -> {
        try {
          // since the logDir is a fake, corrupted one, so the split log worker
          // will finish it quickly with error, and this call will fail and throw
          // an IOException.
          slm.splitLogDistributed(logDir);
        } catch (IOException ioe) {
          try {
            assertTrue(namespace.logExists(WALUtils.pathToDistributedLogName(corruptedLogFile)));
            // this call will block waiting for the task to be removed from the
            // tasks map which is not going to happen since ignoreZKDeleteForTesting
            // is set to true, until it is interrupted.
            slm.splitLogDistributed(logDir);
          } catch (IOException e) {
            assertTrue(Thread.currentThread().isInterrupted());
            return;
          }
          fail("did not get the expected IOException from the 2nd call");
        }
        fail("did not get the expected IOException from the 1st call");
      };
      Future<?> result = executor.submit(runnable);
      try {
        result.get(2000, TimeUnit.MILLISECONDS);
      } catch (TimeoutException te) {
        // it is ok, expected.
      }
      waitForCounter(tot_mgr_wait_for_zk_delete, 0, 1, 10000);
      executor.shutdownNow();
      executor = null;

      // make sure the runnable is finished with no exception thrown.
      result.get();
    } finally {
      if (executor != null) {
        // interrupt the thread in case the test fails in the middle.
        // it has no effect if the thread is already terminated.
        executor.shutdownNow();
      }
      namespace.deleteLog(WALUtils.pathToDistributedLogName(logDir));
    }
  }

  @Test(timeout = 300000)
  public void testReadWriteSeqIdFiles() throws Exception {
    LOG.info("testReadWriteSeqIdFiles");
    startCluster(2);
    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "table-creation", null);
    Table ht = installTable(zkw, "table", "family", 10);
    try {
      TableName tableName = TableName.valueOf("table");
      Path tableDir = new Path(tableName.getNamespaceAsString(), tableName.getQualifierAsString());
      List<Path> regionDirs = WALUtils.listLogPaths(namespace, tableDir, null);
      long newSeqId = WALSplitterUtil.writeRegionSequenceIdLog(namespace, regionDirs.get(0), 1L,
        1000L);
      WALSplitterUtil.writeRegionSequenceIdLog(namespace, regionDirs.get(0) , 1L, 1000L);
      assertEquals(newSeqId + 2000,
        WALSplitterUtil.writeRegionSequenceIdLog(namespace, regionDirs.get(0), 3L, 1000L));

      Path editsdir = WALSplitterUtil.getRegionDirRecoveredEditsDir(regionDirs.get(0));
      List<String> logs = WALUtils.listLogs(namespace, editsdir, WALSplitterUtil::isSequenceIdFile);
      // only one seqid file should exist
      assertEquals(1, logs.size());

      // verify all seqId files aren't treated as recovered.edits files
      NavigableSet<Path> recoveredEdits = WALSplitterUtil.getSplitEditLogsSorted(namespace,
        regionDirs.get(0));
      assertEquals(0, recoveredEdits.size());
    } finally {
      if (ht != null) {
        ht.close();
      }
      zkw.close();
    }
  }

  Table installTable(ZooKeeperWatcher zkw, String tname, String fname, int nrs) throws Exception {
    return installTable(zkw, tname, fname, nrs, 0);
  }

  Table installTable(ZooKeeperWatcher zkw, String tname, String fname, int nrs, int existingRegions)
    throws Exception {
    // Create a table with regions
    TableName table = TableName.valueOf(tname);
    byte [] family = Bytes.toBytes(fname);
    LOG.info("Creating table with " + nrs + " regions");
    Table ht = TEST_UTIL.createMultiRegionTable(table, family, nrs);
    int numRegions = -1;
    try (RegionLocator r = TEST_UTIL.getConnection().getRegionLocator(table)) {
      numRegions = r.getStartKeys().length;
    }
    assertEquals(nrs, numRegions);
    LOG.info("Waiting for no more RIT\n");
    blockUntilNoRIT(zkw, master);
    // disable-enable cycle to get rid of table's dead regions left behind
    // by createMultiRegions
    LOG.debug("Disabling table\n");
    TEST_UTIL.getHBaseAdmin().disableTable(table);
    LOG.debug("Waiting for no more RIT\n");
    blockUntilNoRIT(zkw, master);
    NavigableSet<String> regions = HBaseTestingUtility.getAllOnlineRegions(cluster);
    LOG.debug("Verifying only catalog and namespace regions are assigned\n");
    if (regions.size() != 2) {
      for (String oregion : regions) {
        LOG.debug("Region still online: " + oregion);
      }
    }
    assertEquals(2 + existingRegions, regions.size());
    LOG.debug("Enabling table\n");
    TEST_UTIL.getHBaseAdmin().enableTable(table);
    LOG.debug("Waiting for no more RIT\n");
    blockUntilNoRIT(zkw, master);
    LOG.debug("Verifying there are " + numRegions + " assigned on cluster\n");
    regions = HBaseTestingUtility.getAllOnlineRegions(cluster);
    assertEquals(numRegions + 2 + existingRegions, regions.size());
    return ht;
  }

  void populateDataInTable(int nrows, String fname) throws Exception {
    byte [] family = Bytes.toBytes(fname);

    List<JVMClusterUtil.RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    assertEquals(NUM_RS, rsts.size());

    for (JVMClusterUtil.RegionServerThread rst : rsts) {
      HRegionServer hrs = rst.getRegionServer();
      List<HRegionInfo> hris = ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices());
      for (HRegionInfo hri : hris) {
        if (hri.getTable().isSystemTable()) {
          continue;
        }
        LOG.debug("adding data to rs = " + rst.getName() +
          " region = "+ hri.getRegionNameAsString());
        Region region = hrs.getOnlineRegion(hri.getRegionName());
        assertNotNull(region);
        putData(region, hri.getStartKey(), nrows, Bytes.toBytes("q"), family);
      }
    }

    for (JVMClusterUtil.MasterThread mt : cluster.getLiveMasterThreads()) {
      HRegionServer hrs = mt.getMaster();
      List<HRegionInfo> hris;
      try {
        hris = ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices());
      } catch (ServerNotRunningYetException e) {
        // It's ok: this master may be a backup. Ignored.
        continue;
      }
      for (HRegionInfo hri : hris) {
        if (hri.getTable().isSystemTable()) {
          continue;
        }
        LOG.debug("adding data to rs = " + mt.getName() +
          " region = "+ hri.getRegionNameAsString());
        Region region = hrs.getOnlineRegion(hri.getRegionName());
        assertNotNull(region);
        putData(region, hri.getStartKey(), nrows, Bytes.toBytes("q"), family);
      }
    }
  }

  private void putData(Region region, byte[] startRow, int numRows, byte [] qf, byte [] ...families)
    throws IOException {
    for(int i = 0; i < numRows; i++) {
      Put put = new Put(Bytes.add(startRow, Bytes.toBytes(i)));
      for(byte [] family : families) {
        put.add(family, qf, null);
      }
      region.put(put);
    }
  }

  private void waitForCounter(AtomicLong ctr, long oldValue, long newValue, long timems) {
    long curt = System.currentTimeMillis();
    long endt = curt + timems;
    while (curt < endt) {
      if (ctr.get() == oldValue) {
        Thread.yield();
        curt = System.currentTimeMillis();
      } else {
        assertEquals(newValue, ctr.get());
        return;
      }
    }
    fail();
  }

  private void blockUntilNoRIT(ZooKeeperWatcher zkw, HMaster master) throws Exception {
    TEST_UTIL.waitUntilNoRegionsInTransition(60000);
  }
}
