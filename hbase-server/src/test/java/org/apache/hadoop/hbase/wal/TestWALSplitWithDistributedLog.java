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
package org.apache.hadoop.hbase.wal;

import static org.apache.hadoop.hbase.wal.RegionGroupingProvider.DELEGATE_PROVIDER;
import static org.apache.hadoop.hbase.wal.RegionGroupingProvider.REGION_GROUPING_STRATEGY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import dlshade.org.apache.distributedlog.AppendOnlyStreamReader;
import dlshade.org.apache.distributedlog.AppendOnlyStreamWriter;
import dlshade.org.apache.distributedlog.DLMTestUtil;
import dlshade.org.apache.distributedlog.TestDistributedLogBase;
import dlshade.org.apache.distributedlog.api.DistributedLogManager;
import dlshade.org.apache.distributedlog.api.namespace.Namespace;
import dlshade.org.apache.distributedlog.exceptions.LogNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.regionserver.wal.distributedlog.DistributedLog;
import org.apache.hadoop.hbase.regionserver.wal.distributedlog.DistributedLogAccessor;
import org.apache.hadoop.hbase.regionserver.wal.distributedlog.DistributedLogReader;
import org.apache.hadoop.hbase.regionserver.wal.distributedlog.DistributedLogWriter;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Testing {@link DistributedLog} splitting code.
 */
@Category({ RegionServerTests.class, LargeTests.class})
public class TestWALSplitWithDistributedLog extends TestDistributedLogBase {
  private static final Log LOG = LogFactory.getLog(TestWALSplitWithDistributedLog.class);

  private static Configuration conf;

  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final int NUM_WRITERS = 10;
  private static final int ENTRIES = 10; // entries per writer per region

  private Path HBASEDIR;
  private Path HBASELOGDIR;
  private Path WALDIR;
  private Path OLDLOGDIR;
  private Path CORRUPTDIR;
  private Path TABLEDIR;
  private String TMPDIRNAME;

  private static final String LOGNAME_BEING_SPLIT = "testlog";
  private static final TableName TABLE_NAME = TableName.valueOf("t1");
  private static final byte[] FAMILY = "f1".getBytes();
  private static final byte[] QUALIFIER = "q1".getBytes();
  private static final byte[] VALUE = "v1".getBytes();
  private static final String WAL_FILE_PREFIX = "wal.dat.";
  private static List<String> REGIONS = new ArrayList<String>();
  private static final String HBASE_SKIP_ERRORS = "hbase.hlog.split.skip.errors";
  protected static Path hbaseWALDir;
  private static String ROBBER;
  private static String ZOMBIE;
  private static String [] GROUP = new String [] {"supergroup"};
  private ZooKeeperProtos.SplitLogTask.RecoveryMode mode;

  static private URI uri;
  static private Namespace namespace;

  static enum Corruptions {
    INSERT_GARBAGE_ON_FIRST_LINE,
    INSERT_GARBAGE_IN_THE_MIDDLE,
    APPEND_GARBAGE,
    TRUNCATE,
    TRUNCATE_TRAILER
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    uri = DLMTestUtil.createDLMURI(zkPort, "");
    // The following two DistributedLog related parameters are initialized by the super class.
    // We just copy them to our hbase configuration.
    conf.set("distributedlog.znode.parent", "/messaging/distributedlog");
    conf.set("distributedlog.zk.quorum", zkServers);
    conf.setClass("hbase.regionserver.hlog.writer.impl",
      InstrumentedDistributedLogWriter.class, Writer.class);
    conf.setClass("hbase.regionserver.hlog.reader.impl", DistributedLogReader.class, Reader.class);
    conf.setClass("hbase.wal.provider", RegionGroupingProvider.class, WALProvider.class);
    conf.setClass("hbase.wal.meta_provider", DistributedLogWALProvider.class, WALProvider.class);
    conf.set(REGION_GROUPING_STRATEGY, RegionGroupingProvider.Strategies.bounded.name());
    conf.setClass(DELEGATE_PROVIDER, DistributedLogWALProvider.class, WALProvider.class);
    conf.set("hbase.regionserver.hlog.splitlog.corrupt.dir", HConstants.CORRUPT_DIR_NAME);
    // Create fake maping user to group and set it to the conf.
    Map<String, String []> u2g_map = new HashMap<String, String []>(2);
    ROBBER = User.getCurrent().getName() + "-robber";
    ZOMBIE = User.getCurrent().getName() + "-zombie";
    u2g_map.put(ROBBER, GROUP);
    u2g_map.put(ZOMBIE, GROUP);
    DFSTestUtil.updateConfWithFakeGroupMapping(conf, u2g_map);
    conf.setInt("dfs.heartbeat.interval", 1);
    TEST_UTIL.startMiniDFSCluster(2);
    namespace = DistributedLogAccessor.getInstance(conf).getNamespace();
    hbaseWALDir = TEST_UTIL.createWALRootDir();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniDFSCluster();
  }

  @Rule
  public TestName name = new TestName();
  private WALFactory wals = null;

  @Before
  public void setUp() throws Exception {
    ensureURICreated(uri);
    LOG.info("Cleaning up cluster for new test.");
    HBASEDIR = TEST_UTIL.createRootDir();
    HBASELOGDIR = new Path("/");
    OLDLOGDIR = new Path(HBASELOGDIR, HConstants.HREGION_OLDLOGDIR_NAME);
    CORRUPTDIR = new Path(HBASELOGDIR, HConstants.CORRUPT_DIR_NAME);
    TABLEDIR = new Path(TABLE_NAME.getNamespaceAsString(), TABLE_NAME.getQualifierAsString());
    TMPDIRNAME = conf.get(HConstants.TEMPORARY_FS_DIRECTORY_KEY,
      HConstants.DEFAULT_TEMPORARY_HDFS_DIRECTORY);
    REGIONS.clear();
    Collections.addAll(REGIONS, "bbb", "ccc");
    InstrumentedDistributedLogWriter.activateFailure = false;
    this.mode = (conf.getBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, false) ?
      ZooKeeperProtos.SplitLogTask.RecoveryMode.LOG_REPLAY :
      ZooKeeperProtos.SplitLogTask.RecoveryMode.LOG_SPLITTING);
    wals = new WALFactory(conf, null, name.getMethodName());
    WALDIR = new Path(name.getMethodName());
  }

  @After
  public void tearDown() throws Exception {
    Iterator<String> s = namespace.getLogs();
    while (s.hasNext()) {
      WALUtils.deleteLogsUnderPath(namespace, s.next(),
        DistributedLogAccessor.getDistributedLogStreamName(conf), true);
    }
  }

  /**
   * Simulates splitting a WAL out from under a regionserver that is still trying to write it.
   * Ensures we do not lose edits.
   */
  @Test(timeout=300000)
  public void testLogCannotBeWrittenOnceParsed() throws IOException, InterruptedException {
    final AtomicLong counter = new AtomicLong(0);
    AtomicBoolean stop = new AtomicBoolean(false);
    // Region we'll write edits too and then later examine to make sure they all made it in.
    final String region = REGIONS.get(0);
    final int numWriters = 3;
    Thread zombie = new ZombieLastLogWriterRegionServer(counter, stop, region, numWriters);

    try {
      long startCount = counter.get();
      zombie.start();
      // Wait till writer starts going.
      while (startCount == counter.get()) {
        Threads.sleep(1);
      }
      // Give it a second to write a few appends.
      Threads.sleep(1000);
      final Configuration conf2 = HBaseConfiguration.create(conf);
      final User robber = User.createUserForTesting(conf2, ROBBER, GROUP);
      int count = robber.runAs(new PrivilegedExceptionAction<Integer>() {
        @Override
        public Integer run() throws Exception {
          StringBuilder ls = new StringBuilder("Contents of WALDIR (").append(WALDIR)
            .append("):\n");
          for (Iterator<String> it = namespace.getLogs(); it.hasNext();) {
            String log = it.next();
            ls.append("\t").append(log).append("\n");
          }
          LOG.debug(ls);
          LOG.info("Splitting WALs out from under zombie. Expecting " + numWriters + " files.");
          // In the following split process, the log stream will be marked EOS.
          DistributedLogWALSplitter.split(WALDIR, OLDLOGDIR, conf2, wals, namespace);
          LOG.info("Finished splitting out from under zombie.");
          Path[] logPaths = getLogForRegion(TABLE_NAME, region);
          assertEquals("wrong number of split files for region", numWriters, logPaths.length);
          int count = 0;
          for (Path logPath: logPaths) {
            count += countWAL(logPath);
          }
          return count;
        }
      });
      LOG.info("zombie=" + counter.get() + ", robber=" + count);
      assertTrue("The log file could have at most 1 extra log entry, but can't have less. " +
          "Zombie could write " + counter.get() + " and logfile had only " + count,
        counter.get() == count || counter.get() + 1 == count);
    } finally {
      stop.set(true);
      zombie.interrupt();
      Threads.threadDumpingIsAlive(zombie);
    }
  }

  /**
   * This thread will keep writing to a 'wal' file even after the split process has started.
   * It simulates a region server that was considered dead but woke up and wrote some more to the
   * last log entry. Does its writing as an alternate user in another filesystem instance to
   * simulate better it being a regionserver.
   */
  class ZombieLastLogWriterRegionServer extends Thread {
    final AtomicLong editsCount;
    final AtomicBoolean stop;
    final int numOfWriters;
    /**
     * Region to write edits for.
     */
    final String region;
    final User user;

    public ZombieLastLogWriterRegionServer(AtomicLong counter, AtomicBoolean stop,
        final String region, final int writers) throws IOException, InterruptedException {
      super("ZombieLastLogWriterRegionServer");
      setDaemon(true);
      this.stop = stop;
      this.editsCount = counter;
      this.region = region;
      this.user = User.createUserForTesting(conf, ZOMBIE, GROUP);
      numOfWriters = writers;
    }

    @Override
    public void run() {
      try {
        doWriting();
      } catch (IOException e) {
        LOG.warn(getName() + " Writer exiting " + e);
      } catch (InterruptedException e) {
        LOG.warn(getName() + " Writer exiting " + e);
      }
    }

    private void doWriting() throws IOException, InterruptedException {
      this.user.runAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          // Index of the WAL we want to keep open.  generateWALs will leave open the WAL whose
          // index we supply here.
          int walToKeepOpen = numOfWriters - 1;
          // The below method writes numOfWriters files each with ENTRIES entries for a total of
          // numOfWriters * ENTRIES added per column family in the region.
          Writer writer = null;
          try {
            writer = generateWALs(numOfWriters, ENTRIES, walToKeepOpen);
          } catch (IOException e1) {
            throw new RuntimeException("Failed", e1);
          }
          // Update counter so has all edits written so far.
          editsCount.addAndGet(numOfWriters * ENTRIES);
          loop(writer);
          // If we've been interruped, then things should have shifted out from under us.
          // closing should error
          try {
            writer.close();
            fail("Writing closing after parsing should give an error.");
          } catch (IOException exception) {
            LOG.debug("ignoring error when closing final writer.", exception);
          }
          return null;
        }
      });
    }

    private void loop(final Writer writer) {
      byte [] regionBytes = Bytes.toBytes(this.region);
      while (!stop.get()) {
        try {
          long seq = appendEntry(writer, TABLE_NAME, regionBytes,
            ("r" + editsCount.get()).getBytes(), regionBytes, QUALIFIER, VALUE, 0);
          if (writer instanceof DistributedLogWriter) {
            ((DistributedLogWriter) writer).force();
          }
          long count = editsCount.incrementAndGet();
          LOG.info(getName() + " sync count=" + count + ", seq=" + seq);
          try {
            Thread.sleep(1);
          } catch (InterruptedException e) {
            //
          }
        } catch (IOException ex) {
          LOG.error(getName() + " ex " + ex.toString());
          if (ex instanceof RemoteException) {
            LOG.error("Juliet: got RemoteException " + ex.getMessage() +
              " while writing " + (editsCount.get() + 1));
          } else {
            LOG.error(getName() + " failed to write....at " + editsCount.get());
            fail("Failed to write " + editsCount.get());
          }
          break;
        } catch (Throwable t) {
          LOG.error(getName() + " HOW? " + t);
          LOG.debug("exception details", t);
          break;
        }
      }
      LOG.info(getName() + " Writer exiting");
    }
  }

  /**
   * @param leaveOpen index to leave un-closed. -1 to close all.
   * @return the writer that's still open, or null if all were closed.
   */
  private Writer generateWALs(int writers, int entries, int leaveOpen, int regionEvents) throws IOException {
    return generateWALs(writers, entries, leaveOpen, regionEvents, false);
  }

  private Writer generateWALs(int writers, int entries, int leaveOpen, int regionEvents,
    boolean withCorruptedCell) throws IOException {
    Writer [] ws = new Writer[writers];
    int seq = 0;
    int numRegionEventsAdded = 0;
    for (int i = 0; i < writers; i++) {
      ws[i] = WALUtils.createWALWriter(null, new Path(WALDIR, WAL_FILE_PREFIX + i), conf);
      for (int j = 0; j < entries; j++) {
        int prefix = 0;
        for (String region : REGIONS) {
          String row_key = region + prefix++ + i + j;
          if (withCorruptedCell) {
            appendEntry(ws[i], TABLE_NAME, region.getBytes(), row_key.getBytes(), FAMILY, QUALIFIER,
              VALUE, seq++, true);
            // Only corrupt one cell.
            withCorruptedCell = false;
          } else {
            appendEntry(ws[i], TABLE_NAME, region.getBytes(), row_key.getBytes(), FAMILY, QUALIFIER,
              VALUE, seq++);
          }

          if (numRegionEventsAdded < regionEvents) {
            numRegionEventsAdded ++;
            appendRegionEvent(ws[i], region);
          }
        }
      }
      if (i != leaveOpen) {
        ws[i].close();
        LOG.info("Closing writer " + i);
      }
    }
    if (leaveOpen < 0 || leaveOpen >= writers) {
      return null;
    }
    return ws[leaveOpen];
  }

  public static long appendEntry(Writer writer, TableName table, byte[] region, byte[] row,
      byte[] family, byte[] qualifier, byte[] value, long seq) throws IOException {
    return appendEntry(writer, table, region, row, family, qualifier, value, seq, false);
  }

  public static long appendEntry(Writer writer, TableName table, byte[] region,
    byte[] row, byte[] family, byte[] qualifier,
    byte[] value, long seq, boolean corrupted)
    throws IOException {
    LOG.info(Thread.currentThread().getName() + " append");
    writer.append(createTestEntry(table, region, row, family, qualifier, value, seq, corrupted));
    LOG.info(Thread.currentThread().getName() + " sync");
    writer.sync();
    return seq;
  }


  private static Entry createTestEntry(TableName table, byte[] region, byte[] row, byte[] family,
      byte[] qualifier, byte[] value, long seq, boolean corrupted) {
    long time = System.nanoTime();
    seq++;
    final KeyValue cell = new KeyValue(row, family, qualifier, time, KeyValue.Type.Put, value);
    if (corrupted) {
      // This return the backing array of the whole kv.
      byte[] rawBytes = cell.getQualifierArray();
      // Corrupted the family length with a bigger value.
      rawBytes[cell.getFamilyOffset() - 1] = (byte) -1;
    }

    WALEdit edit = new WALEdit();
    if (corrupted) {
      edit.getCells().add(cell);
    } else {
      // The family is cloned once in this case.
      edit.add(cell);
    }

    return new Entry(new WALKey(region, table, seq, time,
      HConstants.DEFAULT_CLUSTER_ID), edit);
  }

  private static void appendRegionEvent(Writer w, String region) throws IOException {
    WALProtos.RegionEventDescriptor regionOpenDesc = ProtobufUtil.toRegionEventDescriptor(
      WALProtos.RegionEventDescriptor.EventType.REGION_OPEN,
      TABLE_NAME.toBytes(),
      region.getBytes(),
      String.valueOf(region.hashCode()).getBytes(),
      1,
      ServerName.parseServerName("ServerName:9099"), ImmutableMap.<byte[], List<Path>>of());
    final long time = EnvironmentEdgeManager.currentTime();
    KeyValue kv = new KeyValue(region.getBytes(), WALEdit.METAFAMILY, WALEdit.REGION_EVENT,
      time, regionOpenDesc.toByteArray());
    final WALKey walKey = new WALKey(region.getBytes(), TABLE_NAME, 1, time,
      HConstants.DEFAULT_CLUSTER_ID);
    w.append(
      new Entry(walKey, new WALEdit().add(kv)));
    w.sync();
  }

  private Writer generateWALs(int leaveOpen) throws IOException {
    return generateWALs(NUM_WRITERS, ENTRIES, leaveOpen, 0);
  }

  private Writer generateWALs(int writers, int entries, int leaveOpen) throws IOException {
    return generateWALs(writers, entries, leaveOpen, 7);
  }

  private Path[] getLogForRegion(TableName table, String region)
    throws IOException {
    Path tdir = new Path(table.getNamespaceAsString(), table.getQualifierAsString());
    @SuppressWarnings("deprecation")
    Path editsdir = WALSplitterUtil.getRegionDirRecoveredEditsDir(new Path(tdir, region));
    List<Path> logs = new ArrayList<>();
    Iterator<String> iterator = namespace.getLogs(WALUtils.pathToDistributedLogName(editsdir));
    while (iterator.hasNext()) {
      String log = iterator.next();
      if (!WALSplitterUtil.isSequenceIdFile(log)) {
        logs.add(new Path(editsdir, log));
      }
    }
    Path[] paths = new Path[logs.size()];
    paths = logs.toArray(paths);

    return paths;
  }

  private int countWAL(Path log) throws IOException {
    int count = 0;
    Reader in = WALUtils.createReader(null, log, conf);
    while (in.next() != null) {
      count++;
    }
    in.close();
    return count;
  }

  /**
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-3020">HBASE-3020</a>
   */
  @Test (timeout=300000)
  public void testRecoveredEditsPathForMeta() {
    byte [] encoded = HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes();
    Path tdir = new Path(TableName.META_TABLE_NAME.getNamespaceAsString(),
      TableName.META_TABLE_NAME.getQualifierAsString());
    Path regiondir = new Path(tdir, HRegionInfo.FIRST_META_REGIONINFO.getEncodedName());

    long now = System.currentTimeMillis();
    Entry entry =
      new Entry(new WALKey(encoded,
        TableName.META_TABLE_NAME, 1, now, HConstants.DEFAULT_CLUSTER_ID),
        new WALEdit());
    Path p = WALSplitterUtil.getRegionSplitEditsPath4DistributedLog(entry, LOGNAME_BEING_SPLIT);
    String parentOfParent = p.getParent().getParent().getName();
    assertEquals(parentOfParent, HRegionInfo.FIRST_META_REGIONINFO.getEncodedName());
  }

  /**
   * Test old recovered edits file doesn't break WALSplitter.
   * This is useful in upgrading old instances.
   */
  @Test (timeout=300000)
  public void testOldRecoveredEditsFileSidelined() throws IOException {
    byte [] encoded = HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes();
    Path tdir = new Path(TableName.META_TABLE_NAME.getNamespaceAsString(),
      TableName.META_TABLE_NAME.getQualifierAsString());
    Path regiondir = new Path(tdir, HRegionInfo.FIRST_META_REGIONINFO.getEncodedName());

    long now = System.currentTimeMillis();
    Entry entry =
      new Entry(new WALKey(encoded,
        TableName.META_TABLE_NAME, 1, now, HConstants.DEFAULT_CLUSTER_ID),
        new WALEdit());
    Path parent = WALSplitterUtil.getRegionDirRecoveredEditsDir(regiondir);
    assertEquals(parent.getName(), HConstants.RECOVERED_EDITS_DIR);

    Path p = WALSplitterUtil.getRegionSplitEditsPath4DistributedLog(entry, LOGNAME_BEING_SPLIT);
    String parentOfParent = p.getParent().getParent().getName();
    assertEquals(parentOfParent, HRegionInfo.FIRST_META_REGIONINFO.getEncodedName());
    WALUtils.createRecoveredEditsWriter(null, p, conf).close();
  }

  @Test (timeout=300000)
  public void testSplitPreservesEdits() throws IOException{
    final String REGION = "region__1";
    REGIONS.clear();
    REGIONS.add(REGION);

    generateWALs(1, 10, -1, 0);

    DistributedLogWALSplitter.split(WALDIR, OLDLOGDIR, conf, wals, namespace);
    List<String> list = getLogsUnderPath(OLDLOGDIR.getName());
    Path originalLog = new Path(OLDLOGDIR, list.get(0));

    Path[] splitLog = getLogForRegion(TABLE_NAME, REGION);
    assertEquals(1, splitLog.length);

    assertTrue("edits differ after split", logsAreEqual(originalLog, splitLog[0]));
  }

  private boolean logsAreEqual(Path p1, Path p2) throws IOException {
    Reader in1, in2;
    in1 = WALUtils.createReader(null, p1, conf);
    in2 = WALUtils.createReader(null, p2, conf);
    Entry entry1;
    Entry entry2;
    while ((entry1 = in1.next()) != null) {
      entry2 = in2.next();
      if ((entry1.getKey().compareTo(entry2.getKey()) != 0) ||
        (!entry1.getEdit().toString().equals(entry2.getEdit().toString()))) {
        return false;
      }
    }
    in1.close();
    in2.close();
    return true;
  }

  private List<String> getLogsUnderPath(String parent) throws IOException {
    List<String> list = new ArrayList<>();
    try {
      Iterator<String> iterator = namespace.getLogs(parent);
      while (iterator.hasNext()) {
        list.add(iterator.next());
      }
    } catch (LogNotFoundException e) {
      // Do nothing.
    }
    return list;
  }

  private List<String> getLogsUnderPath(Path parent) throws IOException {
    return getLogsUnderPath(WALUtils.pathToDistributedLogName(parent));
  }

  @Test (timeout=300000)
  public void testSplitRemovesRegionEventsEdits() throws IOException{
    final String REGION = "region__1";
    REGIONS.clear();
    REGIONS.add(REGION);

    generateWALs(1, 10, -1, 100);

    DistributedLogWALSplitter.split(WALDIR, OLDLOGDIR, conf, wals, namespace);
    List<String> list = getLogsUnderPath(OLDLOGDIR.getName());
    Path originalLog = new Path(OLDLOGDIR, list.get(0));

    Path[] splitLog = getLogForRegion(TABLE_NAME, REGION);
    assertEquals(1, splitLog.length);

    assertFalse("edits differ after split", logsAreEqual(originalLog, splitLog[0]));
    // split log should only have the test edits
    assertEquals(10, countWAL(splitLog[0]));
  }

  @Test (timeout=300000)
  public void testSplitLeavesCompactionEventsEdits() throws IOException{
    HRegionInfo hri = new HRegionInfo(TABLE_NAME);
    REGIONS.clear();
    REGIONS.add(hri.getEncodedName());
    Path regionDir = new Path(TABLE_NAME.getNamespaceAsString(), TABLE_NAME.getQualifierAsString());
    regionDir = new Path(regionDir, hri.getEncodedName());
    LOG.info("Creating region directory: " + regionDir);

    Writer writer = generateWALs(1, 10, 0, 10);
    String[] compactInputs = new String[]{"log1", "log2", "log3"};
    String compactOutput = "log4";
    appendCompactionEvent(writer, hri, compactInputs, compactOutput);
    writer.close();

    DistributedLogWALSplitter.split(WALDIR, OLDLOGDIR, conf, wals, namespace);

    List<String> list = getLogsUnderPath(OLDLOGDIR.getName());
    Path originalLog = new Path(OLDLOGDIR, list.get(0));
    // original log should have 10 test edits, 10 region markers, 1 compaction marker
    assertEquals(21, countWAL(originalLog));

    Path[] splitLog = getLogForRegion(TABLE_NAME, hri.getEncodedName());
    assertEquals(1, splitLog.length);

    assertFalse("edits differ after split", logsAreEqual(originalLog, splitLog[0]));
    // split log should have 10 test edits plus 1 compaction marker
    assertEquals(11, countWAL(splitLog[0]));
  }

  private static void appendCompactionEvent(Writer w, HRegionInfo hri, String[] inputs,
    String output) throws IOException {
    WALProtos.CompactionDescriptor.Builder desc = WALProtos.CompactionDescriptor.newBuilder();
    desc.setTableName(ByteString.copyFrom(hri.getTable().toBytes()))
      .setEncodedRegionName(ByteString.copyFrom(hri.getEncodedNameAsBytes()))
      .setRegionName(ByteString.copyFrom(hri.getRegionName()))
      .setFamilyName(ByteString.copyFrom(FAMILY))
      .setStoreHomeDir(hri.getEncodedName() + "/" + Bytes.toString(FAMILY))
      .addAllCompactionInput(Arrays.asList(inputs))
      .addCompactionOutput(output);

    WALEdit edit = WALEdit.createCompaction(hri, desc.build());
    WALKey key = new WALKey(hri.getEncodedNameAsBytes(), TABLE_NAME, 1,
      EnvironmentEdgeManager.currentTime(), HConstants.DEFAULT_CLUSTER_ID);
    w.append(new Entry(key, edit));
    w.sync();
  }

  @Test (timeout=300000)
  public void testEmptyLogFiles() throws IOException {
    testEmptyLogFiles(true);
  }

  @Test (timeout=300000)
  public void testWriteWithCorruptedCell() throws IOException {
    conf.setBoolean("hbase.split.sanity.check", true);
    generateWALs(NUM_WRITERS, ENTRIES, -1, 0, true);
    // One writer will fail.
    splitAndCount(NUM_WRITERS - 1, (NUM_WRITERS - 1) * ENTRIES);
    conf.setBoolean("hbase.split.sanity.check", false);
  }

  @Test (timeout=300000)
  public void testEmptyOpenLogFiles() throws IOException {
    testEmptyLogFiles(false);
  }

  private void testEmptyLogFiles(final boolean close) throws IOException {
    injectEmptyFile(".empty", close);
    generateWALs(Integer.MAX_VALUE);
    injectEmptyFile("empty", close);
    splitAndCount(NUM_WRITERS, NUM_WRITERS * ENTRIES); // skip 2 empty
  }

  private void injectEmptyFile(String suffix, boolean closeFile)
    throws IOException {
    Writer writer = WALUtils.createWALWriter(null, new Path(WALDIR, WAL_FILE_PREFIX + suffix),
      conf);
    if (closeFile){
      writer.close();
    }
  }

  /**
   * @param expectedEntries -1 to not assert
   * @return the count across all regions
   */
  private int splitAndCount(final int expectedFiles, final int expectedEntries)
    throws IOException {
    DistributedLogWALSplitter.split(WALDIR, OLDLOGDIR, conf, wals, namespace);
    int result = 0;
    for (String region : REGIONS) {
      Path[] logPaths = getLogForRegion(TABLE_NAME, region);
      assertEquals(expectedFiles, logPaths.length);
      int count = 0;
      for (Path logPath: logPaths) {
        count += countWAL(logPath);
      }
      if (-1 != expectedEntries) {
        assertEquals(expectedEntries, count);
      }
      result += count;
    }
    return result;
  }

  @Test (timeout=300000)
  public void testOpenZeroLengthReportedFileButWithDataGetsSplit() throws IOException {
    // generate logs but leave wal.dat.5 open.
    generateWALs(5);
    splitAndCount(NUM_WRITERS, NUM_WRITERS * ENTRIES);
  }

  @Test (timeout=300000)
  public void testTralingGarbageCorruptionFileSkipErrorsPasses() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, true);
    generateWALs(Integer.MAX_VALUE);
    corruptWAL(new Path(WALDIR, WAL_FILE_PREFIX + "5"), Corruptions.APPEND_GARBAGE, true);
    splitAndCount(NUM_WRITERS, NUM_WRITERS * ENTRIES);
  }

  @Test (timeout=300000)
  public void testFirstLineCorruptionLogFileSkipErrorsPasses() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, true);
    generateWALs(Integer.MAX_VALUE);
    corruptWAL(new Path(WALDIR, WAL_FILE_PREFIX + "5"),
      Corruptions.INSERT_GARBAGE_ON_FIRST_LINE, true);
    splitAndCount(NUM_WRITERS - 1, (NUM_WRITERS - 1) * ENTRIES); //1 corrupt
  }

  @Test (timeout=300000)
  public void testMiddleGarbageCorruptionSkipErrorsReadsHalfOfFile() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, true);
    generateWALs(Integer.MAX_VALUE);
    corruptWAL(new Path(WALDIR, WAL_FILE_PREFIX + "5"),
      Corruptions.INSERT_GARBAGE_IN_THE_MIDDLE, false);
    // the entries in the original logs are alternating regions
    // considering the sequence file header, the middle corruption should
    // affect at least half of the entries
    int goodEntries = (NUM_WRITERS - 1) * ENTRIES;
    int firstHalfEntries = (int) Math.ceil(ENTRIES / 2) - 1;
    int allRegionsCount = splitAndCount(NUM_WRITERS, -1);
    assertTrue("The file up to the corrupted area hasn't been parsed",
      REGIONS.size() * (goodEntries + firstHalfEntries) <= allRegionsCount);
  }

  @Test (timeout=300000)
  public void testCorruptedFileGetsArchivedIfSkipErrors() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, true);
    for (FaultyDistributedLogReader.FailureType failureType :
        FaultyDistributedLogReader.FailureType.values()) {
      // We do not have a SequenceFileReader with DistributedLog.
      // So that when "NONE" is set, all the files are not corrupted.
      if (failureType == FaultyDistributedLogReader.FailureType.NONE) {
        continue;
      }
      final Set<String> walDirContents = splitCorruptWALs(failureType);
      final Set<String> archivedLogs = new HashSet<String>();
      final StringBuilder archived = new StringBuilder("Archived logs in CORRUPTDIR:");
      List<String> logs = getLogsUnderPath(CORRUPTDIR);
      for (String log : logs) {
        archived.append("\n\t").append(log);
        archivedLogs.add(log);
      }
      LOG.debug(archived.toString());
      assertEquals(failureType.name() + ": expected to find all of our wals corrupt.",
        walDirContents, archivedLogs);
    }
  }

  /**
   * @return set of wal names present prior to split attempt.
   * @throws IOException if the split process fails
   */
  private Set<String> splitCorruptWALs(final FaultyDistributedLogReader.FailureType failureType)
    throws IOException {
    Class<?> backupClass = conf.getClass("hbase.regionserver.hlog.reader.impl",
      Reader.class);
    InstrumentedDistributedLogWriter.activateFailure = false;

    try {
      conf.setClass("hbase.regionserver.hlog.reader.impl",
        FaultyDistributedLogReader.class, Reader.class);
      conf.set("faultysequencefilelogreader.failuretype", failureType.name());
      // Clean up from previous tests or previous loop
      try {
        wals.shutdown();
      } catch (IOException exception) {
        // since we're splitting out from under the factory, we should expect some closing failures.
        LOG.debug("Ignoring problem closing WALFactory.", exception);
      }
      wals.close();
      try {
        for (String log : getLogsUnderPath(CORRUPTDIR)) {
          namespace.deleteLog(WALUtils.pathToDistributedLogName(new Path(CORRUPTDIR, log)));
        }
      } catch (LogNotFoundException exception) {
        LOG.debug("no previous CORRUPTDIR to clean.");
      }
      // change to the faulty reader
      wals = new WALFactory(conf, null, name.getMethodName());
      generateWALs(-1);
      // Our reader will render all of these files corrupt.
      final Set<String> walDirContents = new HashSet<String>(getLogsUnderPath(WALDIR));
      DistributedLogWALSplitter.split(WALDIR, OLDLOGDIR, conf, wals, namespace);
      return walDirContents;
    } finally {
      conf.setClass("hbase.regionserver.hlog.reader.impl", backupClass,
        Reader.class);
    }
  }

  private void corruptWAL(Path path, Corruptions corruption, boolean close)
    throws IOException {
    String logPath = WALUtils.pathToDistributedLogName(path);
    AppendOnlyStreamWriter out;
    DistributedLogManager dlm = namespace.openLog(logPath);
    int fileSize = (int) dlm.getLastTxId();

    AppendOnlyStreamReader in = dlm.getAppendOnlyStreamReader();
    byte[] corruptedBytes = new byte[fileSize];
    in.read(corruptedBytes);
    in.close();

    namespace.deleteLog(logPath);
    namespace.createLog(logPath);
    out = namespace.openLog(logPath).getAppendOnlyStreamWriter();

    switch (corruption) {
      case APPEND_GARBAGE:
        out.write(corruptedBytes);
        out.write("-----".getBytes());
        break;

      case INSERT_GARBAGE_ON_FIRST_LINE:
        out.write(new byte[]{0, 0});
        out.write(corruptedBytes);
        break;

      case INSERT_GARBAGE_IN_THE_MIDDLE:
        int middle = (int) Math.floor(corruptedBytes.length / 2);
        byte[] halfCorruptedBytes = Arrays.copyOfRange(corruptedBytes, 0, middle);
        byte[] lastHalfCorruptedBytes =
          Arrays.copyOfRange(corruptedBytes, middle, corruptedBytes.length - middle);
        out.write(halfCorruptedBytes);
        out.write(new byte[]{0});
        out.write(lastHalfCorruptedBytes);
        break;

      case TRUNCATE:
        out.write(Arrays.copyOfRange(corruptedBytes, 0, fileSize
          - (32 + WALUtils.PB_WAL_COMPLETE_MAGIC.length + Bytes.SIZEOF_INT)));
        break;

      case TRUNCATE_TRAILER:
        // trailer is truncated.
        out.write(Arrays.copyOfRange(corruptedBytes, 0, fileSize - Bytes.SIZEOF_INT));
        break;

      default:
        break;
    }

    out.markEndOfStream();
    closeOrFlush(close, out);
  }

  private void closeOrFlush(boolean close, AppendOnlyStreamWriter out)
    throws IOException {
    out.force(false);
    if (close) {
      out.close();
    }
  }

  @Test (timeout=300000, expected = IOException.class)
  public void testTrailingGarbageCorruptionLogFileSkipErrorsFalseThrows()
    throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);
    splitCorruptWALs(FaultyDistributedLogReader.FailureType.BEGINNING);
  }

  @Test (timeout=300000)
  public void testCorruptedLogFilesSkipErrorsFalseDoesNotTouchLogs()
    throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);
    try {
      splitCorruptWALs(FaultyDistributedLogReader.FailureType.BEGINNING);
    } catch (IOException e) {
      LOG.debug("split with 'skip errors' set to 'false' correctly threw");
    }
    assertEquals("if skip.errors is false all files should remain in place",
      NUM_WRITERS, getLogsUnderPath(WALDIR).size());
  }

  @Test (timeout=300000)
  public void testEOSisIgnored() throws IOException {
    int entryCount = 10;
    ignoreCorruption(Corruptions.TRUNCATE, entryCount, entryCount-1);
  }

  @Test (timeout=300000)
  public void testCorruptWALTrailer() throws IOException {
    int entryCount = 10;
    ignoreCorruption(Corruptions.TRUNCATE_TRAILER, entryCount, entryCount);
  }

  @Test (timeout=300000)
  public void testLogsGetArchivedAfterSplit() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);
    generateWALs(-1);
    DistributedLogWALSplitter.split(WALDIR, OLDLOGDIR, conf, wals, namespace);
    List<String> archivedLogs = getLogsUnderPath(OLDLOGDIR);
    assertEquals("wrong number of files in the archive log", NUM_WRITERS, archivedLogs.size());
  }

  @Test (timeout=300000)
  public void testSplit() throws IOException {
    generateWALs(-1);
    splitAndCount(NUM_WRITERS, NUM_WRITERS * ENTRIES);
  }

  @Test (timeout=300000)
  public void testLogDirectoryShouldBeDeletedAfterSuccessfulSplit()
    throws IOException {
    generateWALs(-1);
    DistributedLogWALSplitter.split(WALDIR, OLDLOGDIR, conf, wals, namespace);
    List<String> logs = null;
    try {
      logs = getLogsUnderPath(WALDIR);
      if (logs.size() > 0) {
        fail("Files left in log dir: " + Joiner.on(",").join(logs));
      }
    } catch (LogNotFoundException e) {
      // Do nothing.
    }
  }

  private void ignoreCorruption(final Corruptions corruption, final int entryCount,
    final int expectedCount) throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);

    final String REGION = "region__1";
    REGIONS.clear();
    REGIONS.add(REGION);

    Path c1 = new Path(WALDIR, WAL_FILE_PREFIX + "0");
    generateWALs(1, entryCount, -1, 0);
    corruptWAL(c1, corruption, true);

    DistributedLogWALSplitter.split(WALDIR, OLDLOGDIR, conf, wals, namespace);

    Path[] splitLog = getLogForRegion(TABLE_NAME, REGION);
    assertEquals(1, splitLog.length);

    int actualCount = 0;
    Reader in = WALUtils.createReader(null, splitLog[0], conf);
    @SuppressWarnings("unused")
    Entry entry;
    while ((entry = in.next()) != null) {
      ++actualCount;
    }
    assertEquals(expectedCount, actualCount);
    in.close();

    // should not have stored the EOF files as corrupt
    List<String> archivedLogs = getLogsUnderPath(CORRUPTDIR);
    assertEquals(archivedLogs.size(), 0);
  }

  @Test(timeout=300000, expected = IOException.class)
  public void testSplitWillFailIfWritingToRegionFails() throws Exception {
    //leave 5th log open so we could append the "trap"
    Writer writer = generateWALs(4);

    String region = "break";
    Path tablePath = new Path(TABLE_NAME.getNamespaceAsString(), TABLE_NAME.getQualifierAsString());
    Path regionPath = new Path(tablePath, region);

    InstrumentedDistributedLogWriter.activateFailure = false;
    appendEntry(writer, TABLE_NAME, Bytes.toBytes(region),
      ("r" + 999).getBytes(), FAMILY, QUALIFIER, VALUE, 0);
    writer.close();

    try {
      InstrumentedDistributedLogWriter.activateFailure = true;
      DistributedLogWALSplitter.split(WALDIR, OLDLOGDIR, conf, wals, namespace);
    } catch (IOException e) {
      assertTrue(e.getMessage().
        contains("This exception is instrumented and should only be thrown for testing"));
      throw e;
    } finally {
      InstrumentedLogWriter.activateFailure = false;
    }
  }

  @Test (timeout=300000)
  public void testSplitDeletedRegion() throws IOException {
    REGIONS.clear();
    String region = "region_that_splits";
    REGIONS.add(region);
    Path tablePath = new Path(TABLE_NAME.getNamespaceAsString(), TABLE_NAME.getQualifierAsString());
    Path regionPath = new Path(tablePath, region);

    generateWALs(1);

    DistributedLogWALSplitter.split(WALDIR, OLDLOGDIR, conf, wals, namespace);
    assertFalse(namespace.logExists(WALUtils.pathToDistributedLogName(regionPath)));
  }

  @Test (timeout=300000)
  public void testIOEOnOutputThread() throws Exception {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);

    generateWALs(-1);
    List<String> logs = getLogsUnderPath(WALDIR);
    assertTrue("There should be some log file", logs.size() > 0);
    // wals with no entries (like the one we don't use in the factory)
    // won't cause a failure since nothing will ever be written.
    // pick the largest one since it's most likely to have entries.
    int largestLog = 0;
    long largestSize = 0;
    for (int i = 0; i < logs.size(); i++) {
      Path logPath = new Path(WALDIR, logs.get(i));
      long logSize = namespace.openLog(WALUtils.pathToDistributedLogName(logPath)).getLastTxId();
      if (logSize > largestSize) {
        largestLog = i;
        largestSize = logSize;
      }
    }
    assertTrue("There should be some log greater than size 0.", 0 < largestSize);
    // Set up a splitter that will throw an IOE on the output side
    DistributedLogWALSplitter logSplitter = new DistributedLogWALSplitter(conf, wals,
        null, null, this.mode) {
      @Override
      protected Writer createWriter(Path logfile) throws IOException {
        Writer mockWriter = Mockito.mock(Writer.class);
        Mockito.doThrow(new IOException("Injected")).when(mockWriter).append(Mockito.<Entry>any());
        return mockWriter;
      }
    };
    // Set up a background thread dumper.  Needs a thread to depend on and then we need to run
    // the thread dumping in a background thread so it does not hold up the test.
    final AtomicBoolean stop = new AtomicBoolean(false);
    final Thread someOldThread = new Thread("Some-old-thread") {
      @Override
      public void run() {
        while(!stop.get()) {
          Threads.sleep(10);
        }
      }
    };
    someOldThread.setDaemon(true);
    someOldThread.start();
    final Thread t = new Thread("Background-thread-dumper") {
      public void run() {
        try {
          Threads.threadDumpingIsAlive(someOldThread);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };
    t.setDaemon(true);
    t.start();
    try {
      Path logPath = new Path(WALDIR, logs.get(largestLog));
      logSplitter.splitLog(WALUtils.pathToDistributedLogName(logPath), null);
      fail("Didn't throw!");
    } catch (IOException ioe) {
      assertTrue(ioe.toString().contains("Injected"));
    } finally {
      // Setting this to true will turn off the background thread dumper.
      stop.set(true);
    }
  }

  /**
   * @param spiedNamespace should be instrumented for failure.
   */
  private void retryOverDistributedLogProblem(final Namespace spiedNamespace) throws Exception {
    generateWALs(-1);

    try {
      DistributedLogWALSplitter.split(WALDIR, OLDLOGDIR, conf, wals, spiedNamespace);
      assertEquals(NUM_WRITERS, getLogsUnderPath(OLDLOGDIR).size());
      assertFalse(getLogsUnderPath(WALDIR).size() > 0);
    } catch (IOException e) {
      fail("There shouldn't be any exception but: " + e.toString());
    }
  }

  // Test for HBASE-3412
  @Test (timeout=300000)
  public void testMovedWALDuringRecovery() throws Exception {
    // This partial mock will throw LEE for every file simulating
    // files that were moved
    Namespace spiedNamespace = Mockito.spy(namespace);
    // The "Log does not exist" part is very important,
    Mockito.doThrow(new LeaseExpiredException("Injected: Log does not exist")).
      when(spiedNamespace).openLog(Mockito.any());
    retryOverDistributedLogProblem(spiedNamespace);
  }

  @Test (timeout=300000)
  public void testRetryOpenDuringRecovery() throws Exception {
    Namespace spiedNamespace = Mockito.spy(namespace);
    // The "Cannot obtain block length", "Could not obtain the last block",
    // and "Blocklist for [^ ]* has changed.*" part is very important,
    // that's how it comes out of HDFS. If HDFS changes the exception
    // message, this test needs to be adjusted accordingly.
    //
    // When DFSClient tries to open a file, HDFS needs to locate
    // the last block of the file and get its length. However, if the
    // last block is under recovery, HDFS may have problem to obtain
    // the block length, in which case, retry may help.
    Mockito.doAnswer(new Answer<DistributedLogManager>() {
      private final String[] errors = new String[] {
        "Cannot obtain log length", "Could not obtain the last record",
        "Meta info of " + OLDLOGDIR + " has changed"};
      private int count = 0;

      public DistributedLogManager answer(InvocationOnMock invocation) throws Throwable {
        if (count < 3) {
          throw new IOException(errors[count++]);
        }
        return (DistributedLogManager)invocation.callRealMethod();
      }
    }).when(spiedNamespace).openLog(Mockito.<String>any());
    retryOverDistributedLogProblem(spiedNamespace);
  }

  @Test (timeout=300000)
  public void testTerminationAskedByReporter() throws IOException {
    generateWALs(1, 10, -1);
    List<String> logs = getLogsUnderPath(WALDIR);
    String logFile = WALUtils.pathToDistributedLogName(new Path(WALDIR, logs.get(0)));

    final AtomicInteger count = new AtomicInteger();

    CancelableProgressable localReporter = new CancelableProgressable() {
      @Override
      public boolean progress() {
        count.getAndIncrement();
        return false;
      }
    };

    Namespace spiedNamespace = Mockito.spy(namespace);
    Mockito.doAnswer(new Answer<DistributedLogManager>() {
      public DistributedLogManager answer(InvocationOnMock invocation) throws Throwable {
        Thread.sleep(1500); // Sleep a while and wait report status invoked
        return (DistributedLogManager)invocation.callRealMethod();
      }
    }).when(spiedNamespace).openLog(Mockito.<String>any());

    try {
      conf.setInt("hbase.splitlog.report.period", 1000);
      boolean ret = DistributedLogWALSplitter.splitLog(logFile, conf,
        localReporter, null, null, this.mode, wals);
      assertFalse("Log splitting should failed", ret);
      assertTrue(count.get() > 0);
    } catch (IOException e) {
      fail("There shouldn't be any exception but: " + e.toString());
    } finally {
      // reset it back to its default value
      conf.setInt("hbase.splitlog.report.period", 59000);
    }
  }

  /**
   * Test log split process with fake data and lots of edits to trigger threading
   * issues.
   */
  @Test (timeout=300000)
  public void testThreading() throws Exception {
    doTestThreading(20000, 128*1024*1024, 0);
  }

  /**
   * Test blocking behavior of the log split process if writers are writing slower
   * than the reader is reading.
   */
  @Test (timeout=300000)
  public void testThreadingSlowWriterSmallBuffer() throws Exception {
    doTestThreading(200, 1024, 50);
  }

  /**
   * Sets up a log splitter with a mock reader and writer. The mock reader generates
   * a specified number of edits spread across 5 regions. The mock writer optionally
   * sleeps for each edit it is fed.
   * *
   * After the split is complete, verifies that the statistics show the correct number
   * of edits output into each region.
   *
   * @param numFakeEdits number of fake edits to push through pipeline
   * @param bufferSize size of in-memory buffer
   * @param writerSlowness writer threads will sleep this many ms per edit
   */
  private void doTestThreading(final int numFakeEdits, final int bufferSize,
      final int writerSlowness) throws Exception {

    Configuration localConf = new Configuration(conf);
    localConf.setInt("hbase.regionserver.hlog.splitlog.buffersize", bufferSize);

    // Create a fake log (we'll override the reader to produce a stream of edits)
    Path logPath = new Path(WALDIR, WAL_FILE_PREFIX + ".fake");
    WALUtils.createWriter(conf, null, logPath, false);

    // Make region dirs for our destination regions so the output doesn't get skipped
    final List<String> regions = ImmutableList.of("r0", "r1", "r2", "r3", "r4");

    // Create a splitter that reads and writes the data without touching disk
    DistributedLogWALSplitter logSplitter = new DistributedLogWALSplitter(localConf, wals,
      null, null, this.mode) {

      /* Produce a mock writer that doesn't write anywhere */
      @Override
      protected Writer createWriter(Path logfile) throws IOException {
        Writer mockWriter = Mockito.mock(Writer.class);
        Mockito.doAnswer(new Answer<Void>() {
          int expectedIndex = 0;

          @Override
          public Void answer(InvocationOnMock invocation) {
            if (writerSlowness > 0) {
              try {
                Thread.sleep(writerSlowness);
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
              }
            }
            Entry entry = (Entry) invocation.getArguments()[0];
            WALEdit edit = entry.getEdit();
            List<Cell> cells = edit.getCells();
            assertEquals(1, cells.size());
            Cell cell = cells.get(0);

            // Check that the edits come in the right order.
            assertEquals(expectedIndex, Bytes.toInt(cell.getRowArray(), cell.getRowOffset(),
              cell.getRowLength()));
            expectedIndex++;
            return null;
          }
        }).when(mockWriter).append(Mockito.<Entry>any());
        return mockWriter;
      }

      /* Produce a mock reader that generates fake entries */
      @Override
      protected Reader getReader(Path curLogFile, CancelableProgressable reporter)
        throws IOException {
        Reader mockReader = Mockito.mock(Reader.class);
        Mockito.doAnswer(new Answer<Entry>() {
          int index = 0;

          @Override
          public Entry answer(InvocationOnMock invocation) throws Throwable {
            if (index >= numFakeEdits) {
              return null;
            }

            // Generate r0 through r4 in round robin fashion
            int regionIdx = index % regions.size();
            byte[] region = new byte[] {(byte)'r', (byte) (0x30 + regionIdx)};

            Entry ret = createTestEntry(TABLE_NAME, region,
              Bytes.toBytes((int)(index / regions.size())),
              FAMILY, QUALIFIER, VALUE, index, false);
            index++;
            return ret;
          }
        }).when(mockReader).next();
        return mockReader;
      }
    };

    logSplitter.splitLog(WALUtils.pathToDistributedLogName(logPath), null);

    // Verify number of written edits per region
    Map<byte[], Long> outputCounts = logSplitter.outputSink.getOutputCounts();
    for (Map.Entry<byte[], Long> entry : outputCounts.entrySet()) {
      LOG.info("Got " + entry.getValue() + " output edits for region " +
        Bytes.toString(entry.getKey()));
      assertEquals((long)entry.getValue(), numFakeEdits / regions.size());
    }
    assertEquals("Should have as many outputs as regions", regions.size(), outputCounts.size());
  }

  // Does leaving the writer open in testSplitDeletedRegion matter enough for two tests?
  @Test (timeout=300000)
  public void testSplitLogFileDeletedRegionDir() throws IOException {
    LOG.info("testSplitLogFileDeletedRegionDir");
    final String REGION = "region__1";
    REGIONS.clear();
    REGIONS.add(REGION);

    generateWALs(1, 10, -1);

    Path regionDir = new Path(TABLEDIR, REGION);
    LOG.info("Region directory is" + regionDir);
    String regionDirStr = WALUtils.pathToDistributedLogName(regionDir);
    namespace.deleteLog(regionDirStr);
    DistributedLogWALSplitter.split(WALDIR, OLDLOGDIR, conf, wals, namespace);
    assertFalse(namespace.logExists(regionDirStr));
  }

  @Test (timeout=300000)
  public void testSplitLogEmpty() throws IOException {
    LOG.info("testSplitFileEmpty");

    injectEmptyFile(".empty", true);

    DistributedLogWALSplitter.split(WALDIR, OLDLOGDIR, conf, wals, namespace);
    assertFalse(namespace.logExists(WALUtils.pathToDistributedLogName(TABLEDIR)));
    List<String> logs = getLogsUnderPath(OLDLOGDIR);
    assertEquals(0, countWAL(new Path(OLDLOGDIR, logs.get(0))));
  }

  @Test (timeout=300000)
  public void testSplitLogFileMultipleRegions() throws IOException {
    LOG.info("testSplitLogFileMultipleRegions");
    generateWALs(1, 10, -1);
    splitAndCount(1, 10);
  }

  @Test (timeout=300000)
  public void testSplitLogFileFirstLineCorruptionLog()
    throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, true);
    generateWALs(1, 10, -1);
    String log = getLogsUnderPath(WALDIR).get(0);

    corruptWAL(new Path(WALDIR, log), Corruptions.INSERT_GARBAGE_ON_FIRST_LINE, true);

    DistributedLogWALSplitter.split(WALDIR, OLDLOGDIR, conf, wals, namespace);

    final Path corruptPath = new Path(HConstants.CORRUPT_DIR_NAME);
    assertEquals(1, getLogsUnderPath(corruptPath).size());
  }

  /**
   * {@see https://issues.apache.org/jira/browse/HBASE-4862}
   */
  @Test (timeout=300000)
  public void testConcurrentSplitLogAndReplayRecoverEdit() throws IOException {
    LOG.info("testConcurrentSplitLogAndReplayRecoverEdit");
    // Generate wals for our destination region
    String regionName = "r0";
    final Path regiondir = new Path(TABLEDIR, regionName);
    REGIONS.clear();
    REGIONS.add(regionName);
    generateWALs(-1);

    wals.getWAL(Bytes.toBytes(regionName), null);
    List<String> logs = getLogsUnderPath(WALDIR);
    assertTrue("There should be some log file", logs.size() > 0);

    DistributedLogWALSplitter logSplitter = new DistributedLogWALSplitter(conf, wals, null, null,
      this.mode) {
      @Override
      protected Writer createWriter(Path logPath)
        throws IOException {
        Writer writer = WALUtils.createRecoveredEditsWriter(null, logPath, conf);
        // After creating writer, simulate region's
        // replayRecoveredEditsIfAny() which gets SplitEditFiles of this
        // region and delete them, excluding files with '.temp' suffix.
        NavigableSet<Path> logs = WALSplitterUtil.getSplitEditFilesSorted4DistributedLog(namespace,
          regiondir);
        if (!logs.isEmpty()) {
          for (Path log : logs) {
            String logStr = WALUtils.pathToDistributedLogName(log);
            try {
              namespace.deleteLog(logStr);
              LOG.debug("Deleted recovered.edits file=" + log);
            } catch (IOException ioe) {
              LOG.error("Failed delete of " + log + " with exception: \n", ioe);
            }
          }
        }
        return writer;
      }
    };
    try{
      String logStr = WALUtils.pathToDistributedLogName(new Path(WALDIR, logs.get(0)));
      logSplitter.splitLog(logStr, null);
    } catch (IOException e) {
      LOG.info(e);
      fail("Throws IOException when spliting "
        + "log, it is most likely because writing file does not "
        + "exist which is caused by concurrent replayRecoveredEditsIfAny()");
    }
    if (namespace.logExists(HConstants.CORRUPT_DIR_NAME)) {
      if (getLogsUnderPath(CORRUPTDIR).size() > 0) {
        fail("There are some corrupt logs, "
          + "it is most likely caused by concurrent replayRecoveredEditsIfAny()");
      }
    }
  }
}
