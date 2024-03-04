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
package org.apache.hadoop.hbase.master.cleaner;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import dlshade.org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import dlshade.org.apache.zookeeper.CreateMode;
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
import org.apache.hadoop.hbase.regionserver.wal.bookkeeper.BKConstants;
import org.apache.hadoop.hbase.regionserver.wal.bookkeeper.LedgerLogSystem;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.LedgerUtil;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;

@Category(MediumTests.class)
public class TestLedgerLogCleaner extends BookKeeperClusterTestCase  {
  private static final Log LOG = LogFactory.getLog(TestLedgerLogCleaner.class);
  private static final String WAL_ROOT = BKConstants.DEFAULT_LEDGER_ROOT_PATH;

  private final Configuration conf = HBaseConfiguration.create();

  @Rule
  public TestName name = new TestName();

  public TestLedgerLogCleaner() {
    super(3);
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    Properties prop = new Properties();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    try (InputStream stream = loader.getResourceAsStream("bkConfig.properties")) {
      prop.load(stream);
    }

    String zkConnectionString = zkUtil.getZooKeeperConnectString();

    prop.setProperty("zkServers", zkConnectionString);
    LOG.info("zk address: " + zkConnectionString);
    URL url = loader.getResource("bkConfig.properties");
    assert url != null;
    try (OutputStream output = Files.newOutputStream(Paths.get(url.getPath()))) {
      prop.store(output, null);
    }

    conf.set("hbase.ledger.meta.zk.quorums", zkConnectionString);
    conf.set(BKConstants.LEDGER_ROOT_PATH, WAL_ROOT);

    zkc.create(WAL_ROOT, new byte[0], zkc.getACL("/", null), CreateMode.PERSISTENT);

    LedgerLogSystem.setInstance(conf, LedgerUtil.getBKClientConf(conf));
  }

  @Test
  public void testDeleteLog() throws Exception {
    String fakeLogName = ZKUtil.joinZNode(LedgerUtil.getLedgerArchivePath(WAL_ROOT),
      name.getMethodName());

    LedgerLogSystem ledgerLogSystem = LedgerLogSystem.getInstance(conf);
    ledgerLogSystem.createLog(fakeLogName);

    int cleanerInterval = 1000; // 1 second.
    LedgerLogCleaner cleaner = new LedgerLogCleaner(conf, name.getMethodName(), null,
      cleanerInterval);
    Assert.assertTrue(ledgerLogSystem.logExists(fakeLogName));
    Assert.assertTrue(cleaner.runCleaner());
    Assert.assertFalse(ledgerLogSystem.logExists(fakeLogName));
  }

  @Test
  public void testDeleteLogWithIOException() throws Exception {
    String fakeLogName = ZKUtil.joinZNode(LedgerUtil.getLedgerArchivePath(WAL_ROOT),
      name.getMethodName());

    LedgerLogSystem ledgerLogSystem = LedgerLogSystem.getInstance(conf);
    ledgerLogSystem.createLog(fakeLogName);

    LedgerLogSystem spy = Mockito.spy(ledgerLogSystem);
    doThrow(new IOException("IOException for testing")).when(spy).
      getLogUnderPath(ZKUtil.getParent(fakeLogName));

    int cleanerInterval = 1000; // 1 second.
    LedgerLogCleaner cleaner = new LedgerLogCleaner(conf, name.getMethodName(), null,
      cleanerInterval, spy);


    assertTrue(ledgerLogSystem.logExists(fakeLogName));
    assertFalse(cleaner.runCleaner());
    // Sleep 6 seconds to wait for log deletion.
    assertTrue(ledgerLogSystem.logExists(fakeLogName));
  }
}
