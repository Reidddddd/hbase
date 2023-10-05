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
package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertEquals;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationIndependentConsumer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestReplicationIndependentConsumer {

  private static final Log LOG = LogFactory.getLog(TestReplicationIndependentConsumer.class);
  protected static Configuration conf1 = HBaseConfiguration.create();
  protected static Configuration conf2;
  protected static HBaseTestingUtility utility1;
  protected static HBaseTestingUtility utility2;
  private static final TableName tableName = TableName.valueOf("table_rep_independent");
  private static final byte[] famName = Bytes.toBytes("cf1");
  private static final byte[] qualName = Bytes.toBytes("q1");
  protected static final int NB_ROWS_IN_BATCH = 100;
  protected static final long SLEEP_TIME = 500;
  protected static final int NB_RETRIES = 10;

  private HTableDescriptor tableDescSource, tableDescTarget;
  protected Table tableSource, tableTarget;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    // We don't want too many edits per batch sent to the ReplicationEndpoint to trigger
    // sufficient number of events. But we don't want to go too low because
    // HBaseInterClusterReplicationEndpoint partitions entries into batches and we want
    // more than one batch sent to the peer cluster for better testing.
    conf1.setInt("replication.source.size.capacity", 102400);
    conf1.setLong("replication.source.sleepforretries", 100);
    conf1.setInt("hbase.regionserver.maxlogs", 10);
    conf1.setLong("hbase.master.logcleaner.ttl", 10);
    conf1.setInt("zookeeper.recovery.retry", 1);
    conf1.setInt("zookeeper.recovery.retry.intervalmill", 10);
    conf1.setBoolean(HConstants.REPLICATION_ENABLE_KEY, HConstants.REPLICATION_ENABLE_DEFAULT);
    conf1.setBoolean("dfs.support.append", true);
    conf1.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);
    conf1.setInt("replication.stats.thread.period.seconds", 5);
    conf1.setBoolean("hbase.tests.use.shortcircuit.reads", false);
    conf1.setLong("replication.sleep.before.failover", 2000);
    conf1.setInt("replication.source.maxretriesmultiplier", 10);
    conf1.setFloat("replication.source.ratio", 1.0f);
    conf1.setBoolean("replication.source.eof.autorecovery", true);
    conf1.setBoolean("replication.source.rs.enable.failover", false);
    conf1.setBoolean("replication.source.only.produce", true);

    utility1 = new HBaseTestingUtility(conf1);
    utility1.startMiniZKCluster();
    MiniZooKeeperCluster miniZK = utility1.getZkCluster();
    // Have to reget conf1 in case zk cluster location different
    // than default
    conf1 = utility1.getConfiguration();
    LOG.info("Setup first Zk");

    // Base conf2 on conf1 so it gets the right zk cluster.
    conf2 = HBaseConfiguration.create(conf1);
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");
    conf2.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    conf2.setBoolean(HConstants.REPLICATION_ENABLE_KEY, HConstants.REPLICATION_ENABLE_DEFAULT);
    conf2.setBoolean("dfs.support.append", true);
    conf2.setBoolean("hbase.tests.use.shortcircuit.reads", false);

    utility2 = new HBaseTestingUtility(conf2);
    utility2.setZkCluster(miniZK);

    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey());

    LOG.info("Setup second Zk");
    utility1.startMiniCluster(1);
    // Have a bunch of slave servers, because inter-cluster shipping logic uses number of sinks
    // as a component in deciding maximum number of parallel batches to send to the peer cluster.
    utility2.startMiniCluster(1);
  }

  @Before
  public void setUp() throws Exception {
    setupReplication();
  }

  @After
  public void tearDown() throws IOException, ReplicationException, InterruptedException {
    ReplicationAdmin repAdmin = new ReplicationAdmin(conf1);
    repAdmin.removePeer("1");
    repAdmin.close();
    utility1.deleteTable(tableName);
    utility2.deleteTable(tableName);

  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    utility1.shutdownMiniCluster();
    utility2.shutdownMiniCluster();
    utility1.shutdownMiniZKCluster();
  }

  @Test(timeout = 300000)
  public void testReplicationConsumer() throws Exception {
    LOG.debug("putAndReplicateRows");
    // add rows to Master cluster,
    Put p;

    // 100 rows to tableSource
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      p = new Put(Bytes.toBytes("row_round1" + i));
      p.add(famName, qualName, Bytes.toBytes("val" + i));
      tableSource.put(p);
    }

    // ensure replication completed
    Thread.sleep(SLEEP_TIME);
    int rowCountSource = utility1.countRows(tableSource);
    int rowCountTarget = utility2.countRows(tableTarget);
    assertEquals("has 100 rows on source", 100, rowCountSource);
    assertEquals("has 0 rows on slave", 0, rowCountTarget);

    ReplicationIndependentConsumer.setConfigure(conf1);
    String[] arguments = new String[] { null };
    ReplicationIndependentConsumer consumer0 = new ReplicationIndependentConsumer();
    Thread thread0 = new Thread(() -> {
      try {
        consumer0.run(arguments);
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          LOG.info("consumer0 interrupted.");
        } else {
          throw new RuntimeException(e);
        }
      }
    });
    thread0.setName("consumer0");
    thread0.start();
    ReplicationIndependentConsumer consumer1 = new ReplicationIndependentConsumer();
    Thread thread1 = new Thread(() -> {
      try {
        consumer1.run(arguments);
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          LOG.info("consumer1 interrupted.");
        } else {
          throw new RuntimeException(e);
        }
      }
    });
    thread1.setName("consumer1");
    thread1.start();

    for (int i = 0; i < NB_RETRIES; i++) {
      rowCountTarget = utility2.countRows(tableTarget);
      if (i == NB_RETRIES - 1) {
        assertEquals("has 100 rows on source and target", rowCountSource,
          rowCountTarget);
      }
      if (rowCountSource == rowCountTarget) {
        break;
      }
      Thread.sleep(SLEEP_TIME);
    }

    int workedConsumerCount = 0;
    if (consumer0.isLockedRS()) {
      workedConsumerCount ++;
    }
    if (consumer1.isLockedRS()) {
      workedConsumerCount ++;
    }
    assertEquals("Should only 1 consumer works", workedConsumerCount, 1);

    utility1.getHBaseCluster().stopRegionServer(0);
    utility1.getHBaseCluster().startRegionServer();
    utility1.waitUntilAllRegionsAssigned(tableName);
    // 100 rows to tableSource
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      p = new Put(Bytes.toBytes("row_round2" + i));
      p.add(famName, qualName, Bytes.toBytes("val" + i));
      tableSource.put(p);
    }
    rowCountSource = utility1.countRows(tableSource);
    LOG.info(tableSource.getName().getNameAsString() + "rows count = " + rowCountSource);
    for (int i = 0; i < NB_RETRIES; i++) {
      rowCountTarget = utility2.countRows(tableTarget);
      if (i == NB_RETRIES - 1) {
        assertEquals("has 200 rows on source and target", rowCountSource,
          rowCountTarget);
      }
      if (rowCountSource == rowCountTarget) {
        break;
      }
      Thread.sleep(SLEEP_TIME);
    }
    LOG.info("done");
    consumer0.stop("");
    consumer1.stop("");
  }

  private void setupReplication() throws Exception {
    HColumnDescriptor fam;

    tableDescSource = new HTableDescriptor(tableName);
    fam = new HColumnDescriptor(famName);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    tableDescSource.addFamily(fam);

    tableDescTarget = new HTableDescriptor(tableName);
    fam = new HColumnDescriptor(famName);
    tableDescTarget.addFamily(fam);

    ReplicationAdmin admin1 = new ReplicationAdmin(conf1);
    ReplicationAdmin admin2 = new ReplicationAdmin(conf2);

    Admin ha = new HBaseAdmin(conf1);
    ha.createTable(tableDescSource);
    ha.close();

    ha = new HBaseAdmin(conf2);
    ha.createTable(tableDescTarget);
    ha.close();

    utility1.waitUntilAllRegionsAssigned(tableName);
    utility2.waitUntilAllRegionsAssigned(tableName);


    // Get HTable from Master
    tableSource = new HTable(conf1, tableName);
    tableSource.setWriteBufferSize(1024);

    // Get HTable from Peer1
    tableTarget = new HTable(conf2, tableName);
    tableTarget.setWriteBufferSize(1024);

    int rowCountSource = utility1.countRows(tableSource);
    int rowCountTarget = utility2.countRows(tableTarget);
    assertEquals(0, rowCountSource);
    assertEquals(0, rowCountTarget);

    /**
     * set M-S : Master: utility1 Slave1: utility2
     */
    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey());
    admin1.addPeer("1", rpc);

    admin1.close();
    admin2.close();

    rowCountSource = utility1.countRows(tableSource);
    rowCountTarget = utility2.countRows(tableTarget);
    assertEquals(0, rowCountSource);
    assertEquals(0, rowCountTarget);
  }
}
