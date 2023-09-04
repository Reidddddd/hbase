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
package org.apache.hadoop.hbase.rsgroup;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test the rsgroup auto moving at the regionserver startup.
 */
@Category({ MediumTests.class })
public class TestRSGroupAutoMoving extends TestRSGroupsBase {

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.getConfiguration().setBoolean("hbase.k8s.enabled", true);
    TEST_UTIL.getConfiguration().setFloat(
      "hbase.master.balancer.stochastic.tableSkewCost", 6000);
    TEST_UTIL.getConfiguration().set(
      HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
      RSGroupBasedLoadBalancer.class.getName());
    TEST_UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      RSGroupAdminEndpoint.class.getName() + "," + CPMasterObserver.class.getName());
    TEST_UTIL.getConfiguration().setBoolean(
      HConstants.ZOOKEEPER_USEMULTI,
      true);
    TEST_UTIL.startMiniCluster(NUM_SLAVES_BASE - 1);
    TEST_UTIL.getConfiguration().setInt(
      ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART,
      NUM_SLAVES_BASE - 1);
    TEST_UTIL.getConfiguration().setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    initialize();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    tearDownAfterClass();
  }

  @Before
  public void beforeMethod() throws Exception {
    setUpBeforeMethod();
  }

  @After
  public void afterMethod() throws Exception {
    tearDownAfterMethod();
  }

  @Test
  public void testGroupAutoMoving() throws IOException, InterruptedException {
    String rsgroupName = "moveToThisGroup";
    rsGroupAdmin.addRSGroup(rsgroupName);
    MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    cluster.getConfiguration().set("hbase.regionserver.group", rsgroupName);

    JVMClusterUtil.RegionServerThread rsThread1 = cluster.startRegionServer();
    rsThread1.waitForServerOnline();

    HRegionServer server1 = rsThread1.getRegionServer();
    RSGroupInfo rsGroupInfo = rsGroupAdmin.getRSGroupInfo(rsgroupName);
    ServerName serverName1 = server1.getServerName();

    Address server1Address = serverName1.getAddress();
    Assert.assertTrue(rsGroupInfo.containsServer(server1Address));

    JVMClusterUtil.RegionServerThread rsThread2 = cluster.startRegionServer();
    rsThread2.waitForServerOnline();

    HRegionServer server2 = rsThread2.getRegionServer();
    ServerName serverName2 = server2.getServerName();
    Address server2Address = serverName2.getAddress();
    rsGroupInfo = rsGroupAdmin.getRSGroupInfo(rsgroupName);
    Assert.assertTrue(rsGroupInfo.containsServer(server2Address));

    cluster.killRegionServer(serverName1);
    cluster.killRegionServer(serverName2);

    // Wait for the server expiration.
    Thread.sleep(10000);

    rsGroupInfo = rsGroupAdmin.getRSGroupInfo(rsgroupName);
    Assert.assertFalse(rsGroupInfo.containsServer(server1Address));
    Assert.assertFalse(rsGroupInfo.containsServer(server2Address));
  }
}
