/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.rsgroup;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@Category({ MediumTests.class })
public class TestRegionServerGracefulStopper extends TestRSGroupsBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionServerGracefulStopper.class);

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().set("hbase.coprocessor.regionserver.classes",
      "org.apache.hadoop.hbase.coprocessor.RegionServerGracefulStopper");
    setUpTestBeforeClass();
  }

  @AfterClass
  public static void tearDownAfterTest() throws Exception {
    tearDownAfterClass();
  }

  @Test
  public void testStopperHook() throws Exception {
    TableName tableName = TableName.valueOf("testStopperHook");
    byte[] family = Bytes.toBytes("cf");
    TEST_UTIL.createMultiRegionTable(tableName, family, 100);
    rsGroupAdmin.addRSGroup("backup");
    HRegionServer regionServer = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
//    regionServer.getRegionServerCoprocessorHost().load(RegionServerGracefulStopper.class,
//      0, regionServer.getConfiguration());

    regionServer.stop("stop for testing");
    TEST_UTIL.waitUntilAllRegionsAssigned(tableName);

    Thread.sleep(1000);
    RSGroupInfo rsGroupInfo = rsGroupAdmin.getRSGroupInfo("backup");

    Assert.assertNotNull(rsGroupInfo);
    Assert.assertTrue(rsGroupInfo.getServers().contains(regionServer.getServerName().getAddress()));
    Assert.assertTrue(regionServer.getRegions().isEmpty());

    TEST_UTIL.deleteTable(tableName);
    TEST_UTIL.getMiniHBaseCluster().startRegionServer(regionServer.getServerName().getHostname(),
      regionServer.getServerName().getPort());
    rsGroupInfo = rsGroupAdmin.getRSGroupInfo("backup");
    Assert.assertTrue(rsGroupInfo.containsServer(regionServer.getServerName().getAddress()));
    rsGroupAdmin.moveServers(Sets.newHashSet(regionServer.getServerName().getAddress()), "default");
    TEST_UTIL.waitUntilNoRegionsInTransition();
    rsGroupAdmin.removeRSGroup("backup");
  }
}
