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
package org.apache.hadoop.hbase.regionserver.wal.bookkeeper;

import static org.junit.Assert.fail;
import dlshade.org.apache.bookkeeper.client.BKException;
import dlshade.org.apache.bookkeeper.client.BookKeeper;
import dlshade.org.apache.bookkeeper.client.LedgerHandle;
import dlshade.org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import dlshade.org.apache.zookeeper.CreateMode;
import dlshade.org.apache.zookeeper.KeeperException;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.Entry;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALUtils;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ SmallTests.class })
public class TestLedgerLogWriterAndReader extends BookKeeperClusterTestCase {
  private static final Log LOG = LogFactory.getLog(TestLedgerLogWriterAndReader.class);

  private final Configuration conf;
  String LOG_ROOT = "/hbase/WALs";

  @Rule
  public TestName name = new TestName();

  public TestLedgerLogWriterAndReader() {
    super(3);
    conf = HBaseConfiguration.create();
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    String zkConnectString = zkUtil.getZooKeeperConnectString();
    conf.set("hbase.ledger.meta.zk.quorums", zkConnectString);
    LedgerLogSystem.setInstance(conf, this.bkc.getConf());
  }

  @Test
  public void testWriteReadWithoutCompression()
      throws BKException, IOException, InterruptedException, KeeperException {
    testLedgerLogWriterAndReader(false);
  }

  @Test
  public void testWriteReadWithCompression()
      throws BKException, IOException, InterruptedException, KeeperException {
    testLedgerLogWriterAndReader(true);
  }

  private void testLedgerLogWriterAndReader(boolean compressEnabled)
      throws dlshade.org.apache.bookkeeper.client.BKException, InterruptedException, IOException,
    KeeperException {
    conf.setBoolean(BKConstants.LEDGER_COMPRESSED, compressEnabled);

    BookKeeper.DigestType digestType = BookKeeper.DigestType.MAC;
    byte[] password = Bytes.toBytes("hbase");
    TableName fakeTableName = TableName.valueOf("fakeTableName");
    byte[] regionName = Bytes.toBytes("fakeRegionName");
    long fakeSequenceId = 100L;

    zkc.create("/WALs", null, zkc.getACL("/", null), CreateMode.PERSISTENT);
    LedgerLogSystem logSystem = LedgerLogSystem.getInstance(conf);
    LedgerHandle writeLedger = logSystem.createLog("/WALs/" + name.getMethodName());
    LOG.info("Ledger id: " + writeLedger.getId());
    LedgerLogWriter writer = new LedgerLogWriter(conf, writeLedger,
      "/WALs/" + name.getMethodName());

    WALKey walKey = new WALKey(regionName, fakeTableName, fakeSequenceId);
    WALEdit walEdit = new WALEdit();
    Entry fakeEntry = new Entry(walKey, walEdit);
    writer.append(fakeEntry);
    writer.sync();
    Assert.assertNotEquals(0, writeLedger.getLength());

    // We need to wait the data be persisted in bookie.
    Thread.sleep(1000);
    LedgerHandle readHandle = bkc.openLedger(writeLedger.getId(), digestType, password);
    Assert.assertNotEquals(0, readHandle.getLedgerMetadata().getLength());

    LedgerLogReader reader = new LedgerLogReader(conf, bkc.getConf(), bkc, readHandle,
      "/WALs/" + name.getMethodName());
    Entry readEntry = reader.next();
    // As there is no edit, the readEntry should be null.
    Assert.assertNull(readEntry);

    writeLedger = logSystem.createLog("/WALs/testEntryWithEdits");
    writer = new LedgerLogWriter(conf, writeLedger, "/WALs/testEntryWithEdits");

    Cell fakeCell = new KeyValue(
      Bytes.toBytes("row"), Bytes.toBytes("cf"), Bytes.toBytes("cq"),
      System.currentTimeMillis(), Bytes.toBytes("value")
    );
    walEdit.add(fakeCell);

    writer.append(fakeEntry);
    writer.sync();

    readHandle = bkc.openLedger(writeLedger.getId(), digestType, password);
    reader = new LedgerLogReader(conf, bkc.getConf(), bkc, readHandle,
      "/WALs/testEntryWithEdits");

    readEntry = reader.next();

    Assert.assertEquals(0, readEntry.getKey().compareTo(fakeEntry.getKey()));
    Assert.assertEquals(fakeEntry.getEdit().getCells().size(),
      readEntry.getEdit().getCells().size());
  }

  @Test
  public void testWriteLengthCorrectness()
      throws IOException, InterruptedException, KeeperException, BKException {
    TableName fakeTableName = TableName.valueOf("fakeTableName");
    byte[] regionName = Bytes.toBytes("fakeRegionName");
    long fakeSequenceId = 100L;

    zkc.create("/WALs", null, zkc.getACL("/", null), CreateMode.PERSISTENT);
    LedgerLogSystem logSystem = LedgerLogSystem.getInstance(conf);
    LedgerHandle writeLedger = logSystem.createLog("/WALs/" + name.getMethodName());
    LedgerLogWriter writer = new LedgerLogWriter(conf, writeLedger,
      "/WALs/" + name.getMethodName());

    WALKey walKey = new WALKey(regionName, fakeTableName, fakeSequenceId);
    WALEdit walEdit = new WALEdit();
    Entry fakeEntry = new Entry(walKey, walEdit);
    writer.append(fakeEntry);
    writer.sync();

    String logName = writer.getLogName();
    Assert.assertEquals(0, logSystem.getLogDataSize(logName));
    writer.close();

    long actualLength = logSystem.getLogDataSize(logName);
    Assert.assertTrue(actualLength > 0);
  }

  @Test
  public void testCreateExistingWriter()
      throws IOException, InterruptedException, KeeperException, BKException {
    zkc.create("/hbase", null, zkc.getACL("/", null), CreateMode.PERSISTENT);
    zkc.create("/hbase/WALs", null, zkc.getACL("/", null), CreateMode.PERSISTENT);

    LedgerLogSystem logSystem = LedgerLogSystem.getInstance(conf);
    LedgerHandle writeLedger =
      logSystem.createLog(ZKUtil.joinZNode(LOG_ROOT, name.getMethodName()));
    LedgerLogWriter writer = new LedgerLogWriter(conf, writeLedger,
      ZKUtil.joinZNode(LOG_ROOT, name.getMethodName()));

    try {
      LedgerLogWriter writer2 =
        new LedgerLogWriter(conf, ZKUtil.joinZNode(LOG_ROOT, name.getMethodName()));
      fail("We should fail when creating an existing methods.");
    } catch (Exception e) {
      // We should arrive here.
      Assert.assertTrue(e instanceof IllegalArgumentIOException);
    }

    logSystem.deleteLog(writer.getLogName());

    logSystem.createLog(ZKUtil.joinZNode(LOG_ROOT,
      (name.getMethodName() + WALUtils.SPLITTING_EXT)));

    try {
      LedgerLogWriter writer3 = new LedgerLogWriter(conf,
        ZKUtil.joinZNode(ZKUtil.joinZNode(LOG_ROOT, name.getMethodName()), "log"));
      fail("We should fail when creating an existing methods.");
    } catch (Exception e) {
      // We should arrive here.
      Assert.assertTrue(e instanceof IllegalArgumentIOException);
    }
  }
}
