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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import dlshade.org.apache.distributedlog.DLMTestUtil;
import dlshade.org.apache.distributedlog.TestDistributedLogBase;
import dlshade.org.apache.distributedlog.api.namespace.Namespace;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.mvcc.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.regionserver.wal.distributedlog.DistributedLog;
import org.apache.hadoop.hbase.regionserver.wal.distributedlog.DistributedLogAccessor;
import org.apache.hadoop.hbase.regionserver.wal.distributedlog.DistributedLogReader;
import org.apache.hadoop.hbase.regionserver.wal.distributedlog.DistributedLogWriter;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category(MediumTests.class)
public class TestDistributedLogWALProvider extends TestDistributedLogBase {
  private static final Log LOG = LogFactory.getLog(TestDistributedLogWALProvider.class);

  private static Configuration conf;

  protected MultiVersionConcurrencyControl mvcc;

  private URI uri;
  private Namespace namespace;

  @Rule
  public final TestName currentTest = new TestName();

  @Before
  public void setUp() throws Exception {
    this.uri = DLMTestUtil.createDLMURI(zkPort, "");
    ensureURICreated(this.uri);
    conf = HBaseConfiguration.create();

    // The following two DistributedLog related parameters are initialized by the super class.
    // We just copy them to our hbase configuration.
    conf.set("distributedlog.znode.parent", "/messaging/distributedlog");
    conf.set("distributedlog.zk.quorum", zkServers);
    conf.setClass("hbase.regionserver.hlog.writer.impl", DistributedLogWriter.class, Writer.class);
    conf.setClass("hbase.regionserver.hlog.reader.impl", DistributedLogReader.class, Reader.class);
    conf.setClass("hbase.wal.provider", DistributedLogWALProvider.class, WALProvider.class);
    conf.setClass("hbase.wal.meta_provider", DistributedLogWALProvider.class, WALProvider.class);

    this.namespace = DistributedLogAccessor.getInstance(conf).getNamespace();
    this.mvcc = new MultiVersionConcurrencyControl();
  }

  protected void addEdits(WAL log, HRegionInfo hri, HTableDescriptor htd,
    int times) throws IOException {
    final byte[] row = Bytes.toBytes("row");
    for (int i = 0; i < times; i++) {
      long timestamp = System.currentTimeMillis();
      WALEdit cols = new WALEdit();
      cols.add(new KeyValue(row, row, row, timestamp, row));
      log.append(htd, hri, getWalKey(hri.getEncodedNameAsBytes(), htd.getTableName(), timestamp),
        cols, true);
    }
    log.sync();
  }

  /**
   * used by TestDefaultWALProviderWithHLogKey
   */
  WALKey getWalKey(final byte[] info, final TableName tableName, final long timestamp) {
    return new WALKey(info, tableName, timestamp, mvcc);
  }

  /**
   * helper method to simulate region flush for a WAL.
   */
  protected void flushRegion(WAL wal, byte[] regionEncodedName, Set<byte[]> flushedFamilyNames) {
    wal.startCacheFlush(regionEncodedName, flushedFamilyNames);
    wal.completeCacheFlush(regionEncodedName);
  }

  private static final byte[] UNSPECIFIED_REGION = new byte[]{};

  @Test
  public void testLogCleaning() throws Exception {
    LOG.info("testLogCleaning");
    final HTableDescriptor htd =
      new HTableDescriptor(TableName.valueOf("testLogCleaning")).addFamily(new HColumnDescriptor(
        "row"));
    final HTableDescriptor htd2 =
      new HTableDescriptor(TableName.valueOf("testLogCleaning2"))
        .addFamily(new HColumnDescriptor("row"));
    final Configuration localConf = new Configuration(conf);
    localConf.set(WALFactory.WAL_PROVIDER, DistributedLogWALProvider.class.getName());
    final WALFactory wals = new WALFactory(localConf, null, currentTest.getMethodName());
    try {
      HRegionInfo hri = new HRegionInfo(htd.getTableName(),
        HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      HRegionInfo hri2 = new HRegionInfo(htd2.getTableName(),
        HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      // we want to mix edits from regions, so pick our own identifier.
      final WAL log = wals.getWAL(UNSPECIFIED_REGION, null);

      // Add a single edit and make sure that rolling won't remove the file
      // Before HBASE-3198 it used to delete it
      addEdits(log, hri, htd, 1);
      log.rollWriter();
      assertEquals(1, ((DistributedLog)log).getNumRolledLogFiles());

      // See if there's anything wrong with more than 1 edit
      addEdits(log, hri, htd, 2);
      log.rollWriter();
      assertEquals(2, ((DistributedLog)log).getNumRolledLogFiles());

      // Now mix edits from 2 regions, still no flushing
      addEdits(log, hri, htd, 1);
      addEdits(log, hri2, htd2, 1);
      addEdits(log, hri, htd, 1);
      addEdits(log, hri2, htd2, 1);
      log.rollWriter();
      assertEquals(3, ((DistributedLog)log).getNumRolledLogFiles());

      // Flush the first region, we expect to see the first two files getting
      // archived. We need to append something or writer won't be rolled.
      addEdits(log, hri2, htd2, 1);
      log.startCacheFlush(hri.getEncodedNameAsBytes(), htd.getFamiliesKeys());
      log.completeCacheFlush(hri.getEncodedNameAsBytes());
      log.rollWriter();
      assertEquals(2, ((DistributedLog)log).getNumRolledLogFiles());

      // Flush the second region, which removes all the remaining output files
      // since the oldest was completely flushed and the two others only contain
      // flush information
      addEdits(log, hri2, htd2, 1);
      log.startCacheFlush(hri2.getEncodedNameAsBytes(), htd2.getFamiliesKeys());
      log.completeCacheFlush(hri2.getEncodedNameAsBytes());
      log.rollWriter();
      assertEquals(0, ((DistributedLog)log).getNumRolledLogFiles());
    } finally {
      if (wals != null) {
        wals.close();
      }
    }
  }

  /**
   * Tests wal archiving by adding data, doing flushing/rolling and checking we archive old logs
   * and also don't archive "live logs" (that is, a log with un-flushed entries).
   * <p>
   * This is what it does:
   * It creates two regions, and does a series of inserts along with log rolling.
   * Whenever a WAL is rolled, HLogBase checks previous wals for archiving. A wal is eligible for
   * archiving if for all the regions which have entries in that wal file, have flushed - past
   * their maximum sequence id in that wal file.
   * <p>
   */
  @Test
  public void testWALArchiving() throws IOException {
    LOG.debug("testWALArchiving");
    HTableDescriptor table1 =
      new HTableDescriptor(TableName.valueOf("t1")).addFamily(new HColumnDescriptor("row"));
    HTableDescriptor table2 =
      new HTableDescriptor(TableName.valueOf("t2")).addFamily(new HColumnDescriptor("row"));
    final Configuration localConf = new Configuration(conf);
    localConf.set(WALFactory.WAL_PROVIDER, DistributedLogWALProvider.class.getName());
    final WALFactory wals = new WALFactory(localConf, null, currentTest.getMethodName());
    try {
      final WAL wal = wals.getWAL(UNSPECIFIED_REGION, null);
      assertEquals(0, ((DistributedLog)wal).getNumRolledLogFiles());
      HRegionInfo hri1 =
        new HRegionInfo(table1.getTableName(), HConstants.EMPTY_START_ROW,
          HConstants.EMPTY_END_ROW);
      HRegionInfo hri2 =
        new HRegionInfo(table2.getTableName(), HConstants.EMPTY_START_ROW,
          HConstants.EMPTY_END_ROW);
      // ensure that we don't split the regions.
      hri1.setSplit(false);
      hri2.setSplit(false);
      // variables to mock region sequenceIds.
      // start with the testing logic: insert a waledit, and roll writer
      addEdits(wal, hri1, table1, 1);
      wal.rollWriter();
      // assert that the wal is rolled
      assertEquals(1, ((DistributedLog)wal).getNumRolledLogFiles());
      // add edits in the second wal file, and roll writer.
      addEdits(wal, hri1, table1, 1);
      wal.rollWriter();
      // assert that the wal is rolled
      assertEquals(2, ((DistributedLog)wal).getNumRolledLogFiles());
      // add a waledit to table1, and flush the region.
      addEdits(wal, hri1, table1, 3);
      flushRegion(wal, hri1.getEncodedNameAsBytes(), table1.getFamiliesKeys());
      // roll log; all old logs should be archived.
      wal.rollWriter();
      assertEquals(0, ((DistributedLog)wal).getNumRolledLogFiles());
      // add an edit to table2, and roll writer
      addEdits(wal, hri2, table2, 1);
      wal.rollWriter();
      assertEquals(1, ((DistributedLog)wal).getNumRolledLogFiles());
      // add edits for table1, and roll writer
      addEdits(wal, hri1, table1, 2);
      wal.rollWriter();
      assertEquals(2, ((DistributedLog)wal).getNumRolledLogFiles());
      // add edits for table2, and flush hri1.
      addEdits(wal, hri2, table2, 2);
      flushRegion(wal, hri1.getEncodedNameAsBytes(), table2.getFamiliesKeys());
      // the log : region-sequenceId map is
      // log1: region2 (unflushed)
      // log2: region1 (flushed)
      // log3: region2 (unflushed)
      // roll the writer; log2 should be archived.
      wal.rollWriter();
      assertEquals(2, ((DistributedLog)wal).getNumRolledLogFiles());
      // flush region2, and all logs should be archived.
      addEdits(wal, hri2, table2, 2);
      flushRegion(wal, hri2.getEncodedNameAsBytes(), table2.getFamiliesKeys());
      wal.rollWriter();
      assertEquals(0, ((DistributedLog)wal).getNumRolledLogFiles());
    } finally {
      if (wals != null) {
        wals.close();
      }
    }
  }

  /**
   * Ensure that we can use Set.add to deduplicate WALs
   */
  @Test
  public void setMembershipDedups() throws IOException {
    final Configuration localConf = new Configuration(conf);
    localConf.set(WALFactory.WAL_PROVIDER, DistributedLogWALProvider.class.getName());
    final WALFactory wals = new WALFactory(localConf, null, currentTest.getMethodName());
    try {
      final Set<WAL> seen = new HashSet<WAL>(1);
      final Random random = new Random();
      assertTrue("first attempt to add WAL from default provider should work.",
        seen.add(wals.getWAL(Bytes.toBytes(random.nextInt()), null)));
      for (int i = 0; i < 1000; i++) {
        assertFalse("default wal provider is only supposed to return a single wal, which should "
            + "compare as .equals itself.",
          seen.add(wals.getWAL(Bytes.toBytes(random.nextInt()), null)));
      }
    } finally {
      wals.close();
    }
  }
}
