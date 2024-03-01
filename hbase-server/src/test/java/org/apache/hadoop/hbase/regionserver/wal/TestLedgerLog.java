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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import dlshade.org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import dlshade.org.apache.zookeeper.CreateMode;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.SampleRegionWALObserver;
import org.apache.hadoop.hbase.mvcc.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.wal.bookkeeper.BKConstants;
import org.apache.hadoop.hbase.regionserver.wal.bookkeeper.LedgerLog;
import org.apache.hadoop.hbase.regionserver.wal.bookkeeper.LedgerLogReader;
import org.apache.hadoop.hbase.regionserver.wal.bookkeeper.LedgerLogSystem;
import org.apache.hadoop.hbase.regionserver.wal.bookkeeper.LedgerLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.filesystem.ProtobufLogReader;
import org.apache.hadoop.hbase.regionserver.wal.filesystem.ProtobufLogWriter;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.LedgerUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.LedgerLogProvider;
import org.apache.hadoop.hbase.wal.Reader;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALPerformanceEvaluation;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.hadoop.hbase.wal.WALUtils;
import org.apache.hadoop.hbase.wal.Writer;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category(MediumTests.class)
public class TestLedgerLog extends BookKeeperClusterTestCase {
  private static final Log LOG = LogFactory.getLog(TestLedgerLog.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final String WAL_ROOT = BKConstants.DEFAULT_LEDGER_ROOT_PATH;

  protected static Path rootDir;

  @Rule
  public TestName name = new TestName();

  public TestLedgerLog() throws IOException {
    super(3);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY,
      SampleRegionWALObserver.class.getName());
    TEST_UTIL.startMiniCluster();
    rootDir = TEST_UTIL.getDefaultRootDirPath();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    Properties prop = new Properties();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    try (InputStream stream = loader.getResourceAsStream("bkConfig.properties")) {
      prop.load(stream);
    }

    prop.setProperty("zkServers", zkUtil.getZooKeeperConnectString());
    LOG.info("zk address: " + zkUtil.getZooKeeperConnectString());
    URL url = loader.getResource("bkConfig.properties");
    assert url != null;
    try (OutputStream output = Files.newOutputStream(Paths.get(url.getPath()))) {
      prop.store(output, null);
    }

    String zkConnectString = zkUtil.getZooKeeperConnectString();
    TEST_UTIL.getConfiguration().set("hbase.ledger.meta.zk.quorums", zkConnectString);
    TEST_UTIL.getConfiguration().set(BKConstants.LEDGER_ROOT_PATH, WAL_ROOT);

    zkc.create(WAL_ROOT, new byte[0], zkc.getACL("/", null), CreateMode.PERSISTENT);

    LedgerLogSystem.setInstance(TEST_UTIL.getConfiguration(),
      LedgerUtil.getBKClientConf(TEST_UTIL.getConfiguration()));
  }

  @Test
  public void testWALCoprocessorLoaded() throws Exception {
    // test to see whether the coprocessor is loaded or not.
    Configuration conf = TEST_UTIL.getConfiguration();

    try (LedgerLog log = new LedgerLog(conf, null, "", "", name.getMethodName(), WAL_ROOT)) {
      WALCoprocessorHost host = log.getCoprocessorHost();
      Coprocessor c = host.findCoprocessor(SampleRegionWALObserver.class.getName());
      assertNotNull(c);
    }
  }

  @Test
  public void testWALListenerOnLogArchive() throws Exception {
    final boolean[] triggeredPreArchive = { false };
    final boolean[] triggeredPostArchive = { false };

    WALActionsListener listener = new WALActionsListener.Base() {
      @Override
      public void preLogArchive(org.apache.hadoop.fs.Path oldPath,
        org.apache.hadoop.fs.Path newPath) throws IOException {
        triggeredPreArchive[0] = true;
      }

      @Override
      public void postLogArchive(org.apache.hadoop.fs.Path oldPath,
          Path newPath) throws IOException {
        triggeredPostArchive[0] = true;
      }
    };

    List<WALActionsListener> listeners = new ArrayList<>(1);
    listeners.add(listener);

    try (LedgerLog log = new LedgerLog(TEST_UTIL.getConfiguration(), listeners, "", "",
      name.getMethodName(), WAL_ROOT)) {
      List<String> archiveLogs = new ArrayList<>();
      archiveLogs.add(log.getLogFullName());

      log.archiveLogUnities(archiveLogs);
      assertTrue(triggeredPreArchive[0]);
      assertTrue(triggeredPostArchive[0]);
    }
  }

  /**
   * tests the log comparator. Ensure that we are not mixing meta logs with non-meta logs (throws
   * exception if we do). Comparison is based on the timestamp present in the wal name.
   */
  @Test
  public void testWALComparator() throws Exception {
    LedgerLog wal1 = null;
    LedgerLog walMeta = null;
    Configuration conf = TEST_UTIL.getConfiguration();
    try {
      wal1 = new LedgerLog(conf, null, "", "", name.getMethodName(), WAL_ROOT);
      LOG.debug("Log obtained is: " + wal1);
      Comparator<String> comp = wal1.LOG_NAME_COMPARATOR;
      String p1 = wal1.computeLogName(11);
      String p2 = wal1.computeLogName(12);
      // comparing with itself returns 0
      assertEquals(0, comp.compare(p1, p1));
      // comparing with different filenum.
      assertTrue(comp.compare(p1, p2) < 0);
      walMeta = new LedgerLog(conf, null, "", WALUtils.META_WAL_PROVIDER_ID,
        name.getMethodName(), WAL_ROOT);
      Comparator<String> compMeta = walMeta.LOG_NAME_COMPARATOR;

      String p1WithMeta = walMeta.computeLogName(11);
      String p2WithMeta = walMeta.computeLogName(12);
      assertEquals(0, compMeta.compare(p1WithMeta, p1WithMeta));
      assertTrue(compMeta.compare(p1WithMeta, p2WithMeta) < 0);
      // mixing meta and non-meta logs gives error
      boolean ex = false;
      try {
        comp.compare(p1WithMeta, p2);
      } catch (IllegalArgumentException e) {
        ex = true;
      }
      assertTrue("Comparator doesn't complain while checking meta log files", ex);
      boolean exMeta = false;
      try {
        compMeta.compare(p1WithMeta, p2);
      } catch (IllegalArgumentException e) {
        exMeta = true;
      }
      assertTrue("Meta comparator doesn't complain while checking log files", exMeta);
    } finally {
      if (wal1 != null) {
        wal1.close();
      }
      if (walMeta != null) {
        walMeta.close();
      }
    }
  }

  /**
   * On rolling a wal after reaching the threshold, {@link WAL#rollWriter()} returns the
   * list of regions which should be flushed in order to archive the oldest wal file.
   * <p>
   * This method tests this behavior by inserting edits and rolling the wal enough times to reach
   * the max number of logs threshold. It checks whether we get the "right regions" for flush on
   * rolling the wal.
   */
  @Test
  public void testFindMemStoresEligibleForFlush() throws Exception {
    LOG.debug(name.getMethodName());
    Configuration conf1 = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    conf1.setInt("hbase.regionserver.maxlogs", 1);
    LedgerLog wal = new LedgerLog(conf1, null,"", "", name.getMethodName(), WAL_ROOT);
    HTableDescriptor t1 =
      new HTableDescriptor(TableName.valueOf("t1")).addFamily(new HColumnDescriptor("row"));
    HTableDescriptor t2 =
      new HTableDescriptor(TableName.valueOf("t2")).addFamily(new HColumnDescriptor("row"));
    HRegionInfo hri1 =
      new HRegionInfo(t1.getTableName(), HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    HRegionInfo hri2 =
      new HRegionInfo(t2.getTableName(), HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    // add edits and roll the wal
    MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();
    try {
      addEdits(wal, hri1, t1, 2, mvcc);
      wal.rollWriter();
      // add some more edits and roll the wal. This would reach the log number threshold
      addEdits(wal, hri1, t1, 2, mvcc);
      wal.rollWriter();
      // with above rollWriter call, the max logs limit is reached.
      int x = wal.getNumRolledLogFiles();
      assertTrue(wal.getNumRolledLogFiles() == 2);

      // get the regions to flush; since there is only one region in the oldest wal, it should
      // return only one region.
      byte[][] regionsToFlush = wal.findRegionsToForceFlush();
      assertEquals(1, regionsToFlush.length);
      assertTrue(Bytes.equals(hri1.getEncodedNameAsBytes(), regionsToFlush[0]));
      // insert edits in second region
      addEdits(wal, hri2, t2, 2, mvcc);
      // get the regions to flush, it should still read region1.
      regionsToFlush = wal.findRegionsToForceFlush();
      assertEquals(regionsToFlush.length, 1);
      assertTrue(Bytes.equals(hri1.getEncodedNameAsBytes(), regionsToFlush[0]));
      // flush region 1, and roll the wal file. Only last wal which has entries for region1 should
      // remain.
      flushRegion(wal, hri1.getEncodedNameAsBytes(), t1.getFamiliesKeys());
      wal.rollWriter();
      // only one wal should remain now (that is for the second region).
      assertEquals(1, wal.getNumRolledLogFiles());
      // flush the second region
      flushRegion(wal, hri2.getEncodedNameAsBytes(), t2.getFamiliesKeys());
      wal.rollWriter(true);
      // no wal should remain now.
      assertEquals(0, wal.getNumRolledLogFiles());
      // add edits both to region 1 and region 2, and roll.
      addEdits(wal, hri1, t1, 2, mvcc);
      addEdits(wal, hri2, t2, 2, mvcc);
      wal.rollWriter();
      // add edits and roll the writer, to reach the max logs limit.
      assertEquals(1, wal.getNumRolledLogFiles());
      addEdits(wal, hri1, t1, 2, mvcc);
      wal.rollWriter();
      // it should return two regions to flush, as the oldest wal file has entries
      // for both regions.
      regionsToFlush = wal.findRegionsToForceFlush();
      assertEquals(2, regionsToFlush.length);
      // flush both regions
      flushRegion(wal, hri1.getEncodedNameAsBytes(), t1.getFamiliesKeys());
      flushRegion(wal, hri2.getEncodedNameAsBytes(), t2.getFamiliesKeys());
      wal.rollWriter(true);
      assertEquals(0, wal.getNumRolledLogFiles());
      // Add an edit to region1, and roll the wal.
      addEdits(wal, hri1, t1, 2, mvcc);
      // tests partial flush: roll on a partial flush, and ensure that wal is not archived.
      wal.startCacheFlush(hri1.getEncodedNameAsBytes(), t1.getFamiliesKeys());
      wal.rollWriter();
      wal.completeCacheFlush(hri1.getEncodedNameAsBytes());
      assertEquals(1, wal.getNumRolledLogFiles());
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (wal != null) {
        wal.close();
      }
    }
  }

  @Test(expected=IOException.class)
  public void testFailedToCreateWALIfParentRenamed() throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    LedgerLog log = new LedgerLog(conf, null, null, null, name.getMethodName(),
      WAL_ROOT);
    long logNum = System.currentTimeMillis();
    LedgerLogSystem ledgerLogSystem = LedgerLogSystem.getInstance(conf);
    String logName = log.computeLogName(logNum);
    log.createWriterInstance(logName);

    String splitParent = ZKUtil.getParent(logName) + "-splitting";
    String newLogName = ZKUtil.joinZNode(splitParent, ZKUtil.getNodeName(logName));

    ledgerLogSystem.renamePath(logName, newLogName);

    log.createWriterInstance(logName);
    fail("It should fail to create the new WAL");
  }

  /**
   * Test flush for sure has a sequence id that is beyond the last edit appended.  We do this
   * by slowing appends in the background ring buffer thread while in foreground we call
   * flush.  The addition of the sync over HRegion in flush should fix an issue where flush was
   * returning before all of its appends had made it out to the WAL (HBASE-11109).
   * see HBASE-11109
   */
  @Test
  public void testFlushSequenceIdIsGreaterThanAllEditsInLog() throws IOException {
    String testName = name.getMethodName();
    final TableName tableName = TableName.valueOf(testName);
    final HRegionInfo hri = new HRegionInfo(tableName);
    final byte[] rowName = tableName.getName();
    final HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor("f"));
    HRegion r = HRegion.createHRegion(hri, rootDir,
      TEST_UTIL.getConfiguration(), htd);
    HRegion.closeHRegion(r);
    final int countPerFamily = 10;
    final MutableBoolean goslow = new MutableBoolean(false);
    Configuration conf = TEST_UTIL.getConfiguration();
    // subclass and doctor a method.
    LedgerLog wal = new LedgerLog(conf, null, null, null, testName,
      WAL_ROOT);

    HRegion region = HRegion.openHRegion(TEST_UTIL.getConfiguration(),
      TEST_UTIL.getTestFileSystem(), rootDir, hri, htd, wal);
    EnvironmentEdge ee = EnvironmentEdgeManager.getDelegate();
    try {
      List<Put> puts = null;
      for (HColumnDescriptor hcd: htd.getFamilies()) {
        puts =
          TestWALReplay.addRegionEdits(rowName, hcd.getName(), countPerFamily, ee, region, "x");
      }

      // Now assert edits made it in.
      final Get g = new Get(rowName);
      Result result = region.get(g);
      assertEquals(countPerFamily * htd.getFamilies().size(), result.size());

      // Construct a WALEdit and add it a few times to the WAL.
      WALEdit edits = new WALEdit();
      for (Put p: puts) {
        CellScanner cs = p.cellScanner();
        while (cs.advance()) {
          edits.add(cs.current());
        }
      }
      // Add any old cluster id.
      List<UUID> clusterIds = new ArrayList<UUID>();
      clusterIds.add(UUID.randomUUID());
      // Now make appends run slow.
      goslow.setValue(true);
      for (int i = 0; i < countPerFamily; i++) {
        final HRegionInfo info = region.getRegionInfo();
        final WALKey logkey = new WALKey(info.getEncodedNameAsBytes(), tableName,
          System.currentTimeMillis(), clusterIds, -1, -1, region.getMVCC());
        wal.append(htd, info, logkey, edits, true);
        region.getMVCC().completeAndWait(logkey.getWriteEntry());
      }
      region.flush(true);
      // FlushResult.flushSequenceId is not visible here so go get the current sequence id.
      long currentSequenceId = region.getSequenceId();
      // Now release the appends
      goslow.setValue(false);
      synchronized (goslow) {
        goslow.notifyAll();
      }
      assertTrue(currentSequenceId >= region.getSequenceId());
    } catch (Throwable t) {
      fail("Unexpected err: " + t);
    } finally {
      region.close(true);
      wal.close();
    }
  }

  /**
   * Write to a log file with three concurrent threads and verifying all data is written.
   */
  @Test
  public void testConcurrentWrites() throws Exception {
    Configuration conf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    conf.setClass("hbase.wal.provider", LedgerLogProvider.class, WALProvider.class);
    conf.setClass(WALUtils.HLOG_WRITER, LedgerLogWriter.class, Writer.class);
    conf.setClass(WALUtils.HLOG_READER, LedgerLogReader.class, Reader.class);
    conf.setClass(WALUtils.RECOVERED_EDITS_WRITER, ProtobufLogWriter.class, Writer.class);
    conf.setClass(WALUtils.RECOVERED_EDITS_READER, ProtobufLogReader.class, Reader.class);

    LedgerLogSystem ledgerLogSystem = LedgerLogSystem.getInstance(conf);
    String testName = name.getMethodName();
    String logPath = ZKUtil.joinZNode(WAL_ROOT, testName);

    if (ledgerLogSystem.logExists(logPath)) {
      for (String log : ledgerLogSystem.getLogUnderPath(logPath)) {
        ledgerLogSystem.deleteLog(ZKUtil.joinZNode(logPath, log));
      }
    }

    // Run the WPE tool with three threads writing 3000 edits each concurrently.
    // When done, verify that all edits were written.
    int errCode = WALPerformanceEvaluation.innerMain(
      conf, new String [] {"-threads", "3", "-verify", "-noclosefs", "-iterations", "3000",
        "-syncInterval", "100", "--walType", "ledger"});
    assertEquals(0, errCode);
  }

  /**
   * Test case for https://issues.apache.org/jira/browse/HBASE-16721
   */
  @Test (timeout = 30000)
  public void testUnflushedSeqIdTracking() throws IOException, InterruptedException {
    Configuration conf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    final String testName = name.getMethodName();
    final byte[] b = Bytes.toBytes("b");

    final AtomicBoolean startHoldingForAppend = new AtomicBoolean(false);
    final CountDownLatch holdAppend = new CountDownLatch(1);
    final CountDownLatch flushFinished = new CountDownLatch(1);
    final CountDownLatch putFinished = new CountDownLatch(1);

    try (LedgerLog log = new LedgerLog(conf, null, null, null, testName, WAL_ROOT)) {

      log.registerWALActionsListener(new WALActionsListener.Base() {
        @Override
        public void visitLogEntryBeforeWrite(HTableDescriptor htd, WALKey logKey, WALEdit logEdit)
          throws IOException {
          if (startHoldingForAppend.get()) {
            try {
              holdAppend.await();
            } catch (InterruptedException e) {
              LOG.error(e);
            }
          }
        }
      });

      // open a new region which uses this WAL
      HTableDescriptor htd =
        new HTableDescriptor(TableName.valueOf("t1")).addFamily(new HColumnDescriptor(b));
      HRegionInfo hri =
        new HRegionInfo(htd.getTableName(), HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);

      final HRegion region = TEST_UTIL.createLocalHRegion(hri, htd, log);
      ExecutorService exec = Executors.newFixedThreadPool(2);

      // do a regular write first because of memstore size calculation.
      region.put(new Put(b).addColumn(b, b,b));

      startHoldingForAppend.set(true);
      exec.submit(new Runnable() {
        @Override
        public void run() {
          try {
            region.put(new Put(b).addColumn(b, b,b));
            putFinished.countDown();
          } catch (IOException e) {
            LOG.error(e);
          }
        }
      });

      // give the put a chance to start
      Threads.sleep(3000);

      exec.submit(new Runnable() {
        @Override
        public void run() {
          try {
            Region.FlushResult flushResult = region.flush(true);
            LOG.info("Flush result:" +  flushResult.getResult());
            LOG.info("Flush succeeded:" +  flushResult.isFlushSucceeded());
            flushFinished.countDown();
          } catch (IOException e) {
            LOG.error(e);
          }
        }
      });

      // give the flush a chance to start. Flush should have got the region lock, and
      // should have been waiting on the mvcc complete after this.
      Threads.sleep(3000);

      // let the append to WAL go through now that the flush already started
      holdAppend.countDown();
      putFinished.await();
      flushFinished.await();

      // check whether flush went through
      assertEquals("Region did not flush?", 1, region.getStoreFileList(new byte[][]{b}).size());

      // now check the region's unflushed seqIds.
      long seqId = log.getEarliestMemstoreSeqNum(hri.getEncodedNameAsBytes());
      assertEquals("Found seqId for the region which is already flushed",
        HConstants.NO_SEQNUM, seqId);

      region.close();
    }
  }

  @Test(timeout=30000)
  public void testArchivePathCorrectness() throws IOException {
    List<String> archiveLogs = new ArrayList<>();
    Configuration conf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    LedgerLog log = new LedgerLog(conf, null, null, null, name.getMethodName(),
      WAL_ROOT);
    String currentLogPath = log.getLogFullName();
    archiveLogs.add(currentLogPath);
    LedgerLogSystem ledgerLogSystem = LedgerLogSystem.getInstance(conf);
    String archiveRoot = LedgerUtil.getLedgerArchivePath(WAL_ROOT);
    String archivedPath = ZKUtil.joinZNode(archiveRoot, ZKUtil.getNodeName(currentLogPath));
    log.archiveLogUnities(archiveLogs);

    Assert.assertFalse(ledgerLogSystem.logExists(currentLogPath));
    Assert.assertTrue(ledgerLogSystem.logExists(archivedPath));
  }

  protected void addEdits(WAL log,
    HRegionInfo hri,
    HTableDescriptor htd,
    int times,
    MultiVersionConcurrencyControl mvcc)
    throws IOException {
    final byte[] row = Bytes.toBytes("row");
    for (int i = 0; i < times; i++) {
      long timestamp = System.currentTimeMillis();
      WALEdit cols = new WALEdit();
      cols.add(new KeyValue(row, row, row, timestamp, row));
      WALKey key = new WALKey(hri.getEncodedNameAsBytes(), htd.getTableName(),
        WALKey.NO_SEQUENCE_ID, timestamp, WALKey.EMPTY_UUIDS, HConstants.NO_NONCE,
        HConstants.NO_NONCE, mvcc);
      log.append(htd, hri, key, cols, true);
    }
    log.sync();
  }

  /**
   * helper method to simulate region flush for a WAL.
   */
  protected void flushRegion(WAL wal, byte[] regionEncodedName, Set<byte[]> flushedFamilyNames) {
    wal.startCacheFlush(regionEncodedName, flushedFamilyNames);
    wal.completeCacheFlush(regionEncodedName);
  }
}
