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
import dlshade.org.apache.bookkeeper.client.LedgerHandle;
import dlshade.org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import dlshade.org.apache.zookeeper.CreateMode;
import dlshade.org.apache.zookeeper.KeeperException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.LedgerUtil;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestLedgerLogSystem extends BookKeeperClusterTestCase {
  private static final Log LOG = LogFactory.getLog(TestLedgerLogSystem.class);
  private static final String WAL_ROOT = BKConstants.DEFAULT_LEDGER_ROOT_PATH;

  private LedgerLogSystem ledgerLogSystem;
  private static Configuration conf;
  public TestLedgerLogSystem() {
    super(3);
    conf = HBaseConfiguration.create();
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

    conf.set("hbase.ledger.meta.zk.quorums", zkConnectString);
    conf.set(BKConstants.LEDGER_ROOT_PATH, "/hbase");

    zkc.create(WAL_ROOT, new byte[0], zkc.getACL("/", null), CreateMode.PERSISTENT);

    ledgerLogSystem = LedgerLogSystem.setInstance(conf, LedgerUtil.getBKClientConf(conf));
  }

  @Test
  public void testCreatePathWithActualData()
    throws IOException, InterruptedException, KeeperException,
    org.apache.zookeeper.KeeperException {
    byte[] data = Bytes.toBytes("data");
    String createdPath = ZKUtil.joinZNode(WAL_ROOT, "testCreatePathWithActualData");
    ledgerLogSystem.createPathRecursive(createdPath, data);
    // The root node has no data.
    Assert.assertEquals(0, zkc.getData(WAL_ROOT, false, null).length);

    byte[] readData = RecoverableZooKeeper.removeMetaData(zkc.getData(createdPath, false, null));
    // The target node holds the actual data.
    Assert.assertTrue(Bytes.equals(readData, data));
  }

  @Test
  public void testLockUnlockPath()
      throws IOException, InterruptedException, org.apache.zookeeper.KeeperException {
    String createdPath = ZKUtil.joinZNode(WAL_ROOT, "testLockUnlockPath");
    ledgerLogSystem.createPathRecursive(createdPath, new byte[0]);
    Assert.assertTrue(ledgerLogSystem.tryLockPath(createdPath));
    Assert.assertFalse(ledgerLogSystem.tryLockPath(createdPath));
    Assert.assertTrue(ledgerLogSystem.isLocked(createdPath));
    Assert.assertTrue(ledgerLogSystem.unlockPath(createdPath));

    try {
      Assert.assertTrue(ledgerLogSystem.tryLockPath(createdPath));
      ledgerLogSystem.deleteRecursively(createdPath);
      fail("Should not be able to delete a locked path.");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IllegalStateException);
    }

    try {
      Assert.assertTrue(ledgerLogSystem.unlockPath(createdPath));
      ledgerLogSystem.deleteRecursively(createdPath);
    } catch (Exception e) {
      fail("Should be able to delete log without a lock.");
    }
  }

  @Test
  public void testRenameLogCorrectness() throws IOException, InterruptedException, KeeperException {
    String createdPath = ZKUtil.joinZNode(WAL_ROOT, "testRenameLogCorrectness");
    String newPath = ZKUtil.joinZNode(WAL_ROOT, "renamed");
    byte[] data = Bytes.toBytes("data");
    ledgerLogSystem.createPathRecursive(createdPath, data);
    ledgerLogSystem.renamePath(createdPath, newPath);
    Assert.assertTrue(Bytes.equals(RecoverableZooKeeper.removeMetaData(
      zkc.getData(newPath, false, null)), data));
  }

  @Test
  public void testDeleteLogPath() throws IOException {
    String createdPath = ZKUtil.joinZNode(WAL_ROOT, "testDeleteLogPath");
    ledgerLogSystem.createPathRecursive(createdPath, Bytes.toBytes("data"));
    Assert.assertTrue(ledgerLogSystem.logExists(createdPath));

    ledgerLogSystem.deleteRecursively(createdPath);
    Assert.assertFalse(ledgerLogSystem.logExists(createdPath));
  }

  @Test
  public void testLogMetadataCorrectness() throws BKException, IOException, InterruptedException {
    String logPath = ZKUtil.joinZNode(WAL_ROOT, "testLogMetadataCorrectness");
    LedgerHandle lh = ledgerLogSystem.createLog(logPath);

    LogMetadata metadata = ledgerLogSystem.getLogMetadata(logPath);
    Assert.assertEquals(1, metadata.getLedgerIds().size());

    long ledgerIdInMeta = metadata.getLedgerIdIterator().next();
    Assert.assertEquals(lh.getId(), ledgerIdInMeta);
    Assert.assertFalse(metadata.isClosed());

    ledgerLogSystem.closeLog(logPath);
    metadata = ledgerLogSystem.getLogMetadata(logPath);
    Assert.assertTrue(metadata.isClosed());
  }

  // This test is for multi wal cases in RS.
  @Test
  public void testConcurrentCreateAndDelete()
      throws IOException, InterruptedException, org.apache.zookeeper.KeeperException {
    String logPath1 = "/hbase/WALs/testConcurrentCreatePath/log1";
    String logPath2 = "/hbase/WALs/testConcurrentCreatePath/log2";
    byte[] data1 = Bytes.toBytes("data1");
    byte[] data2 = Bytes.toBytes("data2");

    Thread t1 = new Thread(() -> {
      try {
        ledgerLogSystem.createPathRecursive(logPath1, data1);
      } catch (IOException e) {
        fail("Should not be exceptional when create two log concurrently.");
      }
    });

    Thread t2 = new Thread(() -> {
      try {
        ledgerLogSystem.createPathRecursive(logPath2, data2);
      } catch (IOException e) {
        fail("Should not be exceptional when create two log concurrently.");
      }
    });

    t1.start();
    t2.start();

    t1.join();
    t2.join();

    Assert.assertTrue(ledgerLogSystem.logExists(logPath1));
    Assert.assertTrue(ledgerLogSystem.logExists(logPath2));
    Assert.assertTrue(Bytes.equals(data1, ledgerLogSystem.getDataOfPath(logPath1)));
    Assert.assertTrue(Bytes.equals(data2, ledgerLogSystem.getDataOfPath(logPath2)));
  }
}
