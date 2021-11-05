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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.exceptions.MergeRegionException;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DispatchMergingRegionsRequest;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.ServiceException;

/**
 * Class to test HBaseAdmin.
 * Spins up the minicluster once at test start and then takes it down afterward.
 * Add any testing of HBaseAdmin functionality here.
 */
@Category(LargeTests.class)
public class TestAdmin1 extends TestAdminBase {
  private static final Log LOG = LogFactory.getLog(TestAdmin1.class);

  @Test (timeout=300000)
  public void testSplitFlushCompactUnknownTable() throws InterruptedException {
    final TableName unknowntable = TableName.valueOf("fubar");
    Exception exception = null;
    try {
      admin.compact(unknowntable);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      admin.flush(unknowntable);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);

    exception = null;
    try {
      admin.split(unknowntable);
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);
  }

  @Test (timeout=300000)
  public void testCompactionTimestamps() throws Exception {
    HColumnDescriptor fam1 = new HColumnDescriptor("fam1");
    TableName tableName = TableName.valueOf("testCompactionTimestampsTable");
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(fam1);
    admin.createTable(htd);
    HTable table = (HTable)TEST_UTIL.getConnection().getTable(htd.getTableName());
    long ts = admin.getLastMajorCompactionTimestamp(tableName);
    assertEquals(0, ts);
    Put p = new Put(Bytes.toBytes("row1"));
    p.add(Bytes.toBytes("fam1"), Bytes.toBytes("fam1"), Bytes.toBytes("fam1"));
    table.put(p);
    ts = admin.getLastMajorCompactionTimestamp(tableName);
    // no files written -> no data
    assertEquals(0, ts);

    admin.flush(tableName);
    ts = admin.getLastMajorCompactionTimestamp(tableName);
    // still 0, we flushed a file, but no major compaction happened
    assertEquals(0, ts);

    byte[] regionName =
        table.getRegionLocator().getAllRegionLocations().get(0).getRegionInfo().getRegionName();
    long ts1 = admin.getLastMajorCompactionTimestampForRegion(regionName);
    assertEquals(ts, ts1);
    p = new Put(Bytes.toBytes("row2"));
    p.add(Bytes.toBytes("fam1"), Bytes.toBytes("fam1"), Bytes.toBytes("fam1"));
    table.put(p);
    admin.flush(tableName);
    ts = admin.getLastMajorCompactionTimestamp(tableName);
    // make sure the region API returns the same value, as the old file is still around
    assertEquals(ts1, ts);

    TEST_UTIL.compact(tableName, true);
    table.put(p);
    // forces a wait for the compaction
    admin.flush(tableName);
    ts = admin.getLastMajorCompactionTimestamp(tableName);
    // after a compaction our earliest timestamp will have progressed forward
    assertTrue(ts > ts1);

    // region api still the same
    ts1 = admin.getLastMajorCompactionTimestampForRegion(regionName);
    assertEquals(ts, ts1);
    table.put(p);
    admin.flush(tableName);
    ts = admin.getLastMajorCompactionTimestamp(tableName);
    assertEquals(ts, ts1);
    table.close();
  }

  @Test (timeout=300000)
  public void testHColumnValidName() {
       boolean exceptionThrown;
       try {
         new HColumnDescriptor("\\test\\abc");
       } catch(IllegalArgumentException iae) {
           exceptionThrown = true;
           assertTrue(exceptionThrown);
       }
   }

  @Test (timeout=300000)
  public void testShouldFailOnlineSchemaUpdateIfOnlineSchemaIsNotEnabled()
      throws Exception {
    final TableName tableName = TableName.valueOf("changeTableSchemaOnlineFailure");
    TEST_UTIL.getMiniHBaseCluster().getMaster().getConfiguration().setBoolean(
        "hbase.online.schema.update.enable", false);
    HTableDescriptor[] tables = admin.listTables();
    int numTables = tables.length;
    TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY).close();
    tables = admin.listTables();
    assertEquals(numTables + 1, tables.length);

    // FIRST, do htabledescriptor changes.
    HTableDescriptor htd = admin.getTableDescriptor(tableName);
    // Make a copy and assert copy is good.
    HTableDescriptor copy = new HTableDescriptor(htd);
    assertTrue(htd.equals(copy));
    // Now amend the copy. Introduce differences.
    long newFlushSize = htd.getMemStoreFlushSize() / 2;
    if (newFlushSize <=0) {
      newFlushSize = HTableDescriptor.DEFAULT_MEMSTORE_FLUSH_SIZE / 2;
    }
    copy.setMemStoreFlushSize(newFlushSize);
    final String key = "anyoldkey";
    assertTrue(htd.getValue(key) == null);
    copy.setValue(key, key);
    boolean expectedException = false;
    try {
      admin.modifyTable(tableName, copy);
    } catch (TableNotDisabledException re) {
      expectedException = true;
    }
    assertTrue("Online schema update should not happen.", expectedException);

    // Reset the value for the other tests
    TEST_UTIL.getMiniHBaseCluster().getMaster().getConfiguration().setBoolean(
        "hbase.online.schema.update.enable", true);
  }

  @Test (timeout=120000)
  public void testTableExist() throws IOException {
    final TableName table = TableName.valueOf("testTableExist");
    boolean exist;
    exist = admin.tableExists(table);
    assertEquals(false, exist);
    TEST_UTIL.createTable(table, HConstants.CATALOG_FAMILY);
    exist = admin.tableExists(table);
    assertEquals(true, exist);
  }

  /**
   * Tests forcing split from client and having scanners successfully ride over split.
   * @throws Exception
   * @throws IOException
   */
  @Test (timeout=400000)
  public void testForceSplit() throws Exception {
    byte[][] familyNames = new byte[][] { Bytes.toBytes("cf") };
    int[] rowCounts = new int[] { 6000 };
    int numVersions = HColumnDescriptor.DEFAULT_VERSIONS;
    int blockSize = 256;
    splitTest(null, familyNames, rowCounts, numVersions, blockSize);

    byte[] splitKey = Bytes.toBytes(3500);
    splitTest(splitKey, familyNames, rowCounts, numVersions, blockSize);
  }

  /**
   * Multi-family scenario. Tests forcing split from client and
   * having scanners successfully ride over split.
   * @throws Exception
   * @throws IOException
   */
  @Test (timeout=800000)
  public void testForceSplitMultiFamily() throws Exception {
    int numVersions = HColumnDescriptor.DEFAULT_VERSIONS;

    // use small HFile block size so that we can have lots of blocks in HFile
    // Otherwise, if there is only one block,
    // HFileBlockIndex.midKey()'s value == startKey
    int blockSize = 256;
    byte[][] familyNames = new byte[][] { Bytes.toBytes("cf1"),
      Bytes.toBytes("cf2") };

    // one of the column families isn't splittable
    int[] rowCounts = new int[] { 6000, 1 };
    splitTest(null, familyNames, rowCounts, numVersions, blockSize);

    rowCounts = new int[] { 1, 6000 };
    splitTest(null, familyNames, rowCounts, numVersions, blockSize);

    // one column family has much smaller data than the other
    // the split key should be based on the largest column family
    rowCounts = new int[] { 6000, 300 };
    splitTest(null, familyNames, rowCounts, numVersions, blockSize);

    rowCounts = new int[] { 300, 6000 };
    splitTest(null, familyNames, rowCounts, numVersions, blockSize);

  }

  void splitTest(byte[] splitPoint, byte[][] familyNames, int[] rowCounts,
    int numVersions, int blockSize) throws Exception {
    TableName tableName = TableName.valueOf("testForceSplit");
    StringBuilder sb = new StringBuilder();
    // Add tail to String so can see better in logs where a test is running.
    for (int i = 0; i < rowCounts.length; i++) {
      sb.append("_").append(Integer.toString(rowCounts[i]));
    }
    assertFalse(admin.tableExists(tableName));
    final HTable table = TEST_UTIL.createTable(tableName, familyNames,
      numVersions, blockSize);

    int rowCount = 0;
    byte[] q = new byte[0];

    // insert rows into column families. The number of rows that have values
    // in a specific column family is decided by rowCounts[familyIndex]
    for (int index = 0; index < familyNames.length; index++) {
      ArrayList<Put> puts = new ArrayList<Put>(rowCounts[index]);
      for (int i = 0; i < rowCounts[index]; i++) {
        byte[] k = Bytes.toBytes(i);
        Put put = new Put(k);
        put.add(familyNames[index], q, k);
        puts.add(put);
      }
      table.put(puts);

      if ( rowCount < rowCounts[index] ) {
        rowCount = rowCounts[index];
      }
    }

    // get the initial layout (should just be one region)
    Map<HRegionInfo, ServerName> m = table.getRegionLocations();
    LOG.info("Initial regions (" + m.size() + "): " + m);
    assertTrue(m.size() == 1);

    // Verify row count
    Scan scan = new Scan();
    ResultScanner scanner = table.getScanner(scan);
    int rows = 0;
    for(@SuppressWarnings("unused") Result result : scanner) {
      rows++;
    }
    scanner.close();
    assertEquals(rowCount, rows);

    // Have an outstanding scan going on to make sure we can scan over splits.
    scan = new Scan();
    scanner = table.getScanner(scan);
    // Scan first row so we are into first region before split happens.
    scanner.next();

    // Split the table
    admin.split(tableName, splitPoint);

    final AtomicInteger count = new AtomicInteger(0);
    Thread t = new Thread("CheckForSplit") {
      @Override
      public void run() {
        for (int i = 0; i < 45; i++) {
          try {
            sleep(1000);
          } catch (InterruptedException e) {
            continue;
          }
          // check again    table = new HTable(conf, tableName);
          Map<HRegionInfo, ServerName> regions = null;
          try {
            regions = table.getRegionLocations();
          } catch (IOException e) {
            e.printStackTrace();
          }
          if (regions == null) continue;
          count.set(regions.size());
          if (count.get() >= 2) {
            LOG.info("Found: " + regions);
            break;
          }
          LOG.debug("Cycle waiting on split");
        }
        LOG.debug("CheckForSplit thread exited, current region count: " + count.get());
      }
    };
    t.setPriority(Thread.NORM_PRIORITY - 2);
    t.start();
    t.join();

    // Verify row count
    rows = 1; // We counted one row above.
    for (@SuppressWarnings("unused") Result result : scanner) {
      rows++;
      if (rows > rowCount) {
        scanner.close();
        assertTrue("Scanned more than expected (" + rowCount + ")", false);
      }
    }
    scanner.close();
    assertEquals(rowCount, rows);

    Map<HRegionInfo, ServerName> regions = null;
    try {
      regions = table.getRegionLocations();
    } catch (IOException e) {
      e.printStackTrace();
    }
    assertNotNull(regions);
    assertEquals(2, regions.size());
    Set<HRegionInfo> hRegionInfos = regions.keySet();
    HRegionInfo[] r = hRegionInfos.toArray(new HRegionInfo[hRegionInfos.size()]);
    if (splitPoint != null) {
      // make sure the split point matches our explicit configuration
      assertEquals(Bytes.toString(splitPoint),
          Bytes.toString(r[0].getEndKey()));
      assertEquals(Bytes.toString(splitPoint),
          Bytes.toString(r[1].getStartKey()));
      LOG.debug("Properly split on " + Bytes.toString(splitPoint));
    } else {
      if (familyNames.length > 1) {
        int splitKey = Bytes.toInt(r[0].getEndKey());
        // check if splitKey is based on the largest column family
        // in terms of it store size
        int deltaForLargestFamily = Math.abs(rowCount/2 - splitKey);
        LOG.debug("SplitKey=" + splitKey + "&deltaForLargestFamily=" + deltaForLargestFamily +
          ", r=" + r[0]);
        for (int index = 0; index < familyNames.length; index++) {
          int delta = Math.abs(rowCounts[index]/2 - splitKey);
          if (delta < deltaForLargestFamily) {
            assertTrue("Delta " + delta + " for family " + index
              + " should be at least deltaForLargestFamily " + deltaForLargestFamily,
              false);
          }
        }
      }
    }
    TEST_UTIL.deleteTable(tableName);
    table.close();
  }

  @Test
  public void testSplitAndMergeWithReplicaTable() throws Exception {
    // The test tries to directly split replica regions and directly merge replica regions. These
    // are not allowed. The test validates that. Then the test does a valid split/merge of allowed
    // regions.
    // Set up a table with 3 regions and replication set to 3
    TableName tableName = TableName.valueOf("testSplitAndMergeWithReplicaTable");
    byte[] cf = "f".getBytes();
    createReplicaTable(tableName, cf);
    List<HRegion> oldRegions;
    do {
      oldRegions = TEST_UTIL.getHBaseCluster().getRegions(tableName);
      Thread.sleep(10);
    } while (oldRegions.size() != 9); //3 regions * 3 replicas
    // write some data to the table
    HTable ht = new HTable(TEST_UTIL.getConfiguration(), tableName);
    List<Put> puts = new ArrayList<Put>();
    byte[] qualifier = "c".getBytes();
    Put put = new Put(new byte[]{(byte)'1'});
    put.add(cf, qualifier, "100".getBytes());
    puts.add(put);
    put = new Put(new byte[]{(byte)'6'});
    put.add(cf, qualifier, "100".getBytes());
    puts.add(put);
    put = new Put(new byte[]{(byte)'8'});
    put.add(cf, qualifier, "100".getBytes());
    puts.add(put);
    ht.put(puts);
    ht.flushCommits();
    ht.close();
    List<Pair<HRegionInfo, ServerName>> regions =
        MetaTableAccessor.getTableRegionsAndLocations(TEST_UTIL.getZooKeeperWatcher(),
                          TEST_UTIL.getConnection(), tableName);
    boolean gotException = false;
    // the element at index 1 would be a replica (since the metareader gives us ordered
    // regions). Try splitting that region via the split API . Should fail
    try {
      TEST_UTIL.getHBaseAdmin().split(regions.get(1).getFirst().getRegionName());
    } catch (IllegalArgumentException ex) {
      gotException = true;
    }
    assertTrue(gotException);
    gotException = false;
    // the element at index 1 would be a replica (since the metareader gives us ordered
    // regions). Try splitting that region via a different split API (the difference is
    // this API goes direct to the regionserver skipping any checks in the admin). Should fail
    try {
      TEST_UTIL.getHBaseAdmin().split(regions.get(1).getSecond(), regions.get(1).getFirst(),
          new byte[]{(byte)'1'});
    } catch (IOException ex) {
      gotException = true;
    }
    assertTrue(gotException);
    gotException = false;
    // Try merging a replica with another. Should fail.
    try {
      TEST_UTIL.getHBaseAdmin().mergeRegions(regions.get(1).getFirst().getEncodedNameAsBytes(),
          regions.get(2).getFirst().getEncodedNameAsBytes(), true);
    } catch (IllegalArgumentException m) {
      gotException = true;
    }
    assertTrue(gotException);
    // Try going to the master directly (that will skip the check in admin)
    try {
      DispatchMergingRegionsRequest request = RequestConverter
          .buildDispatchMergingRegionsRequest(regions.get(1).getFirst().getEncodedNameAsBytes(),
              regions.get(2).getFirst().getEncodedNameAsBytes(), true);
      TEST_UTIL.getHBaseAdmin().getConnection().getMaster().dispatchMergingRegions(null, request);
    } catch (ServiceException m) {
      Throwable t = m.getCause();
      do {
        if (t instanceof MergeRegionException) {
          gotException = true;
          break;
        }
        t = t.getCause();
      } while (t != null);
    }
    assertTrue(gotException);
    gotException = false;
    // Try going to the regionservers directly
    // first move the region to the same regionserver
    if (!regions.get(2).getSecond().equals(regions.get(1).getSecond())) {
      TEST_UTIL.moveRegionAndWait(regions.get(2).getFirst(), regions.get(1).getSecond());
    }
    try {
      AdminService.BlockingInterface admin = TEST_UTIL.getHBaseAdmin().getConnection()
          .getAdmin(regions.get(1).getSecond());
      ProtobufUtil.mergeRegions(null, admin, regions.get(1).getFirst(), regions.get(2).getFirst(),
        true, null);
    } catch (MergeRegionException mm) {
      gotException = true;
    }
    assertTrue(gotException);
  }

  /**
   * Test case to validate whether parent's replica regions are cleared from AM's memory after
   * SPLIT/MERGE.
   */
  @Test
  public void testRegionStateCleanupFromAMMemoryAfterRegionSplitAndMerge() throws Exception {
    final TableName tableName =
        TableName.valueOf("testRegionStateCleanupFromAMMemoryAfterRegionSplitAndMerge");
    createReplicaTable(tableName, "f".getBytes());
    final int regionReplication = admin.getTableDescriptor(tableName).getRegionReplication();

    List<Pair<HRegionInfo, ServerName>> regions = MetaTableAccessor.getTableRegionsAndLocations(
      TEST_UTIL.getZooKeeperWatcher(), TEST_UTIL.getConnection(), tableName);
    assertEquals(9, regions.size());
    final int primaryRegionCount = regions.size() / regionReplication;

    final AssignmentManager am = TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager();
    List<HRegionInfo> splitRegions =
        am.getRegionStates().getRegionByStateOfTable(tableName).get(RegionState.State.SPLIT);
    assertEquals(0, splitRegions.size());

    // Validate region split
    byte[] regionName = regions.get(0).getFirst().getRegionName();
    try {
      TEST_UTIL.getHBaseAdmin().split(regionName, Bytes.toBytes('2'));
    } catch (IllegalArgumentException ex) {
      fail("Exception occured during region split" + ex);
    }

    // Wait for replica region to become online
    TEST_UTIL.waitFor(60000, 500, new Waiter.Predicate<IOException>() {
      @Override
      public boolean evaluate() throws IOException {
        return am.getRegionStates().getRegionByStateOfTable(tableName).get(RegionState.State.OPEN)
            .size() == (primaryRegionCount + 1) * regionReplication;
      }
    });

    regions = MetaTableAccessor.getTableRegionsAndLocations(TEST_UTIL.getZooKeeperWatcher(),
      TEST_UTIL.getConnection(), tableName);
    assertEquals(12, regions.size());
    final int primaryRegionCountAfterSplit = regions.size() / regionReplication;

    // Split region after region split
    splitRegions =
        am.getRegionStates().getRegionByStateOfTable(tableName).get(RegionState.State.SPLIT);
    // Parent's replica region should be removed from AM's memory.
    assertEquals(1, splitRegions.size());

    // Validate region merge
    HRegionInfo regionA = regions.get(3).getFirst();
    HRegionInfo regionB = regions.get(6).getFirst();
    try {
      TEST_UTIL.getHBaseAdmin().mergeRegions(regionA.getRegionName(), regionB.getRegionName(),
        true);
    } catch (IllegalArgumentException ex) {
      fail("Exception occured during region merge" + ex);
    }

    // Wait for replica regions to become online
    TEST_UTIL.waitFor(60000, 500, new Waiter.Predicate<IOException>() {
      @Override
      public boolean evaluate() throws IOException {
        return am.getRegionStates().getRegionByStateOfTable(tableName).get(RegionState.State.OPEN)
            .size() == (primaryRegionCountAfterSplit - 1) * regionReplication;
      }
    });

    regions = MetaTableAccessor.getTableRegionsAndLocations(TEST_UTIL.getZooKeeperWatcher(),
      TEST_UTIL.getConnection(), tableName);
    assertEquals(9, regions.size());
    // Offline region after region merge
    List<HRegionInfo> offlineRegions =
        am.getRegionStates().getRegionByStateOfTable(tableName).get(RegionState.State.OFFLINE);
    // Parent's replica region should be removed from AM's memory.
    assertEquals(0, offlineRegions.size());
  }

  private byte[] createReplicaTable(TableName tableName, byte[] cf) throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.setRegionReplication(3);
    HColumnDescriptor hcd = new HColumnDescriptor(cf);
    desc.addFamily(hcd);
    byte[][] splitRows = new byte[2][];
    splitRows[0] = new byte[] { (byte) '4' };
    splitRows[1] = new byte[] { (byte) '7' };
    TEST_UTIL.getHBaseAdmin().createTable(desc, splitRows);
    return cf;
  }

  /**
   * HADOOP-2156
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  @Test (expected=IllegalArgumentException.class, timeout=300000)
  public void testEmptyHTableDescriptor() throws IOException {
    admin.createTable(new HTableDescriptor());
  }

  @Test (expected=IllegalArgumentException.class, timeout=300000)
  public void testInvalidHColumnDescriptor() throws IOException {
     new HColumnDescriptor("/cfamily/name");
  }

  /*
   * Test DFS replication for column families, where one CF has default replication(3) and the other
   * is set to 1.
   */
  @Test(timeout = 300000)
  public void testHFileReplication() throws Exception {
    TableName name = TableName.valueOf("testHFileReplication");
    String fn1 = "rep1";
    HColumnDescriptor hcd1 = new HColumnDescriptor(fn1);
    hcd1.setDFSReplication((short) 1);
    String fn = "defaultRep";
    HColumnDescriptor hcd = new HColumnDescriptor(fn);
    HTableDescriptor htd = new HTableDescriptor(name);
    htd.addFamily(hcd);
    htd.addFamily(hcd1);
    Table table = TEST_UTIL.createTable(htd, null);
    TEST_UTIL.waitTableAvailable(name);
    Put p = new Put(Bytes.toBytes("defaultRep_rk"));
    byte[] q1 = Bytes.toBytes("q1");
    byte[] v1 = Bytes.toBytes("v1");
    p.addColumn(Bytes.toBytes(fn), q1, v1);
    List<Put> puts = new ArrayList<Put>(2);
    puts.add(p);
    p = new Put(Bytes.toBytes("rep1_rk"));
    p.addColumn(Bytes.toBytes(fn1), q1, v1);
    puts.add(p);
    try {
      table.put(puts);
      admin.flush(name);

      List<HRegion> regions = TEST_UTIL.getMiniHBaseCluster().getRegions(name);
      for (HRegion r : regions) {
        Store store = r.getStore(Bytes.toBytes(fn));
        for (StoreFile sf : store.getStorefiles()) {
          assertTrue(sf.toString().contains(fn));
          assertTrue("Column family " + fn + " should have 3 copies",
            FSUtils.getDefaultReplication(TEST_UTIL.getTestFileSystem(), sf.getPath()) == (sf
                .getFileInfo().getFileStatus().getReplication()));
        }

        store = r.getStore(Bytes.toBytes(fn1));
        for (StoreFile sf : store.getStorefiles()) {
          assertTrue(sf.toString().contains(fn1));
          assertTrue("Column family " + fn1 + " should have only 1 copy", 1 == sf.getFileInfo()
              .getFileStatus().getReplication());
        }
      }
    } finally {
      if (admin.isTableEnabled(name)) {
        admin.disableTable(name);
        admin.deleteTable(name);
      }
    }
  }

  @Test
  public void testMergeRegions() throws Exception {
    TableName tableName = TableName.valueOf("testMergeWithFullRegionName");
    HColumnDescriptor cd = new HColumnDescriptor("d");
    HTableDescriptor td = new HTableDescriptor(tableName);
    td.addFamily(cd);
    byte[][] splitRows = new byte[2][];
    splitRows[0] = new byte[]{(byte)'3'};
    splitRows[1] = new byte[]{(byte)'6'};
    try {
      TEST_UTIL.createTable(td, splitRows);
      TEST_UTIL.waitTableAvailable(tableName);

      List<HRegionInfo> tableRegions;
      HRegionInfo regionA;
      HRegionInfo regionB;

      // merge with full name
      tableRegions = admin.getTableRegions(tableName);
      assertEquals(3, admin.getTableRegions(tableName).size());
      regionA = tableRegions.get(0);
      regionB = tableRegions.get(1);
      admin.mergeRegions(regionA.getRegionName(), regionB.getRegionName(), false);
      Thread.sleep(1000);
      assertEquals(2, admin.getTableRegions(tableName).size());

      // merge with encoded name
      tableRegions = admin.getTableRegions(tableName);
      regionA = tableRegions.get(0);
      regionB = tableRegions.get(1);
      admin.mergeRegions(regionA.getEncodedNameAsBytes(), regionB.getEncodedNameAsBytes(), false);
      Thread.sleep(1000);
      assertEquals(1, admin.getTableRegions(tableName).size());
    } finally {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
  }
}
