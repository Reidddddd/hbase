/*
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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import dlshade.org.apache.distributedlog.DLMTestUtil;
import dlshade.org.apache.distributedlog.TestDistributedLogBase;
import dlshade.org.apache.distributedlog.api.namespace.Namespace;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.SampleRegionWALObserver;
import org.apache.hadoop.hbase.regionserver.wal.DistributedLogAccessor;
import org.apache.hadoop.hbase.regionserver.wal.DistributedLogReader;
import org.apache.hadoop.hbase.regionserver.wal.DistributedLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.DistributedLogWALProvider;
import org.apache.hadoop.hbase.wal.Entry;
import org.apache.hadoop.hbase.wal.Reader;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.hadoop.hbase.wal.WALSplitterUtil;
import org.apache.hadoop.hbase.wal.WALUtils;
import org.apache.hadoop.hbase.wal.Writer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Tests around replay of recovered.edits content.
 */
@Category({ MediumTests.class})
public class TestRecoveredEditsWithDistributedLog extends TestDistributedLogBase {
  private static final Log LOG = LogFactory.getLog(TestRecoveredEditsWithDistributedLog.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static Configuration conf;

  private URI uri;
  private Namespace namespace;

  protected static FileSystem fs;
  protected static Path rootDir;

  @Rule
  public final TestName testName = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Make block sizes small.
    TEST_UTIL.getConfiguration().setInt("dfs.blocksize", 1024 * 1024);
    // quicker heartbeat interval for faster DN death notification
    TEST_UTIL.getConfiguration().setInt("dfs.namenode.heartbeat.recheck-interval", 5000);
    TEST_UTIL.getConfiguration().setInt("dfs.heartbeat.interval", 1);
    TEST_UTIL.getConfiguration().setInt("dfs.client.socket-timeout", 5000);

    // faster failover with cluster.shutdown();fs.close() idiom
    TEST_UTIL.getConfiguration()
      .setInt("hbase.ipc.client.connect.max.retries", 1);
    TEST_UTIL.getConfiguration().setInt(
      "dfs.client.block.recovery.retries", 1);
    TEST_UTIL.getConfiguration().setInt(
      "hbase.ipc.client.connection.maxidletime", 500);
    TEST_UTIL.getConfiguration().set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY,
      SampleRegionWALObserver.class.getName());
    TEST_UTIL.startMiniDFSCluster(3);

    conf = TEST_UTIL.getConfiguration();
    fs = TEST_UTIL.getDFSCluster().getFileSystem();
    setupCluster(numBookies);
  }

  @Before
  public void setUp() throws Exception {
    this.uri = DLMTestUtil.createDLMURI(zkPort, "");
    ensureURICreated(this.uri);

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
    conf.setClass("hbase.hregion.wal.replayer.class", DistributedLogWALReplayer.class,
      WALReplayer.class);

    this.namespace = DistributedLogAccessor.getInstance(conf).getNamespace();

    FileStatus[] entries = fs.listStatus(new Path("/"));
    for (FileStatus dir : entries) {
      fs.delete(dir.getPath(), true);
    }
    rootDir = TEST_UTIL.createRootDir();
  }

  /**
   * HBASE-12782 ITBLL fails for me if generator does anything but 5M per maptask.
   * Create a region. Close it. Then copy into place a file to replay, one that is bigger than
   * configured flush size so we bring on lots of flushes.  Then reopen and confirm all edits
   * made it in.
<<<<<<< HEAD
   * @throws IOException
=======
>>>>>>> 32bffe33873 (SPDI-69433 Implement WAL Replay with DistributedLog)
   */
  @Test(timeout=60000)
  public void testReplayWorksThoughLotsOfFlushing() throws IOException {
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    // Set it so we flush every 1M or so.  Thats a lot.
    conf.setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 1024*1024);
    // The file of recovered edits has a column family of 'meta'. Also has an encoded regionname
    // of 4823016d8fca70b25503ee07f4c6d79f which needs to match on replay.
    final String encodedRegionName = "4823016d8fca70b25503ee07f4c6d79f";
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(testName.getMethodName()));
    final String columnFamily = "meta";
    byte [][] columnFamilyAsByteArray = new byte [][] { Bytes.toBytes(columnFamily)};
    htd.addFamily(new HColumnDescriptor(columnFamily));
    HRegionInfo hri = new HRegionInfo(htd.getTableName()) {
      @Override
      public synchronized String getEncodedName() {
        return encodedRegionName;
      }

      // Cache the name because lots of lookups.
      private byte [] encodedRegionNameAsBytes = null;
      @Override
      public synchronized byte[] getEncodedNameAsBytes() {
        if (encodedRegionNameAsBytes == null) {
          this.encodedRegionNameAsBytes = Bytes.toBytes(getEncodedName());
        }
        return this.encodedRegionNameAsBytes;
      }
    };
    Path hbaseRootDir = TEST_UTIL.getDataTestDir();
    FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
    Path tableDir = FSUtils.getTableDir(hbaseRootDir, htd.getTableName());
    HRegionFileSystem hrfs =
      new HRegionFileSystem(TEST_UTIL.getConfiguration(), fs, tableDir, hri);
    if (fs.exists(hrfs.getRegionDir())) {
      LOG.info("Region directory already exists. Deleting.");
      fs.delete(hrfs.getRegionDir(), true);
    }
    HRegion region = HRegion.createHRegion(hri, hbaseRootDir, conf, htd, null);
    assertEquals(encodedRegionName, region.getRegionInfo().getEncodedName());
    List<String> storeFiles = region.getStoreFileList(columnFamilyAsByteArray);
    // There should be no store files.
    assertTrue(storeFiles.isEmpty());
    region.close();
    Path regionDir = region.getWalReplayer().getWALRegionDir();
    Path recoveredEditsDir = WALSplitterUtil.getRegionDirRecoveredEditsDir(regionDir);
    // This is a little fragile getting this path to a file of 10M of edits.
    Path recoveredEditsFile = new Path(
      System.getProperty("test.build.classes", "target/test-classes"),
      "0000000000000016310");
    Path randomDir = TEST_UTIL.getRandomDir();
    // Copy the content of this file to DistributedLog.
    Path destination = new Path(randomDir, recoveredEditsFile.getName());
    fs.copyFromLocalFile(recoveredEditsFile, destination);
    assertTrue(fs.exists(destination));

    Path logPath = new Path(recoveredEditsDir, recoveredEditsFile.getName());
    Path randomPath = new Path(TEST_UTIL.getRandomDir(), recoveredEditsFile.getName());

    // Use ProtobufLogReader & DistributedLogWriter transfer data from HDFS to DistributedLog
    Reader reader = WALUtils.createReader(fs, destination, HBaseConfiguration.create());
    Writer writer = WALUtils.createWriter(TEST_UTIL.getConfiguration(), null, logPath, false);
    Writer writer1 = WALUtils.createWriter(TEST_UTIL.getConfiguration(), null, randomPath, false);
    while (true) {
      Entry entry = reader.next();
      if (entry != null) {
        writer.append(entry);
        writer1.append(entry);
      } else {
        break;
      }
    }
    ((DistributedLogWriter) writer).force();
    ((DistributedLogWriter) writer1).force();
    writer.close();
    writer1.close();

    assertTrue(namespace.logExists(WALUtils.pathToDistributedLogName(logPath)));
    // Now the file 0000000000000016310 is under recovered.edits, reopen the region to replay.
    region = HRegion.openHRegion(region, null);
    assertEquals(encodedRegionName, region.getRegionInfo().getEncodedName());
    storeFiles = region.getStoreFileList(columnFamilyAsByteArray);
    // Our 0000000000000016310 is 10MB. Most of the edits are for one region. Lets assume that if
    // we flush at 1MB, that there are at least 3 flushed files that are there because of the
    // replay of edits.
    assertTrue("Files count=" + storeFiles.size(), storeFiles.size() > 10);
    // Now verify all edits made it into the region.
    int count = verifyAllEditsMadeItIn(conf, randomPath, region);
    LOG.info("Checked " + count + " edits made it in");
  }

  /**
   * @return Return how many edits seen.
   */
  private int verifyAllEditsMadeItIn(final Configuration conf, final Path edits,
      final HRegion region) throws IOException {
    int count = 0;
    // Based on HRegion#replayRecoveredEdits
    try (Reader reader = WALUtils.createReader(null, edits, conf)) {
      Entry entry;
      while ((entry = reader.next()) != null) {
        WALKey key = entry.getKey();
        WALEdit val = entry.getEdit();
        count++;
        // Check this edit is for this region.
        if (!Bytes.equals(key.getEncodedRegionName(),
          region.getRegionInfo().getEncodedNameAsBytes())) {
          continue;
        }
        Cell previous = null;
        for (Cell cell : val.getCells()) {
          if (CellUtil.matchingFamily(cell, WALEdit.METAFAMILY)) {
            continue;
          }
          if (previous != null && CellComparator.compareRows(previous, cell) == 0) {
            continue;
          }
          previous = cell;
          Get g = new Get(CellUtil.cloneRow(cell));
          Result r = region.get(g);
          boolean found = false;
          for (CellScanner scanner = r.cellScanner(); scanner.advance(); ) {
            Cell current = scanner.current();
            if (CellComparator.compare(cell, current, true) == 0) {
              found = true;
              break;
            }
          }
          assertTrue("Failed to find " + cell, found);
        }
      }
    }
    return count;
  }
}
