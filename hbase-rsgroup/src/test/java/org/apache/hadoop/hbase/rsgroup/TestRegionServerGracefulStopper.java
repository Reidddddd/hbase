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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.RegionServerGracefulStopper;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class })
public class TestRegionServerGracefulStopper extends TestRSGroupsBase {

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean("hbase.k8s.enabled", true);
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
    regionServer.getRegionServerCoprocessorHost().loadInstance(RegionServerGracefulStopper.class,
      0, regionServer.getConfiguration());

    regionServer.stop("stop for testing");
    TEST_UTIL.waitUntilAllRegionsAssigned(tableName);

    RSGroupInfo rsGroupInfo = rsGroupAdmin.getRSGroupInfo("backup");

    Assert.assertNotNull(rsGroupInfo);
    Assert.assertTrue(rsGroupInfo.getServers().contains(regionServer.getServerName().getAddress()));
    Assert.assertTrue(regionServer.getOnlineRegions().isEmpty());

    TEST_UTIL.deleteTable(tableName);
    TEST_UTIL.getMiniHBaseCluster().startRegionServer(regionServer.getServerName().getHostname(),
      regionServer.getServerName().getPort());
    rsGroupInfo = rsGroupAdmin.getRSGroupInfo("backup");
    Assert.assertTrue(rsGroupInfo.getServers().isEmpty());
    rsGroupAdmin.removeRSGroup("backup");
  }
}
