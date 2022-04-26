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
package org.apache.hadoop.hbase.thrift;

import static org.apache.hadoop.hbase.thrift.Constants.COALESCE_INC_KEY;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.util.ConnectionCacheWithAuthToken;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Unit testing for ThriftServerRunner.HBaseServiceHandler with Digest-MD5 auth is on.
 */
@Category({ ClientTests.class, LargeTests.class})
public class TestThriftServerWithDigestAuth extends TestThriftServer {
  private static final Log LOG = LogFactory.getLog(TestThriftServerWithDigestAuth.class);

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.getConfiguration().setBoolean(COALESCE_INC_KEY, false);
    UTIL.getConfiguration().setInt("hbase.client.retries.number", 3);

    String credential = "U0hCYXMAAABAYWU1ZGViODIyZTBkNzE5OTI5MDA0NzFhNzE5OWQw"
        + "ZDk1YjhlN2M5ZDA1YzQwYTgyNDVhMjgxZmQyYzFkNjY4NDEyMzQ1Ng==";
    UserGroupInformation.setLoginUser(
        UserGroupInformation.createUserForTesting("testuser", new String[] {"testusergroup"}));

    UTIL.getConfiguration().set(User.HBASE_SECURITY_CONF_KEY, "digest");
    UTIL.getConfiguration().set(User.DIGEST_PASSWORD_KEY, credential);

    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  /**
   * Runs all of the tests under a single JUnit test method.  We
   * consolidate all testing to one method because HBaseClusterTestCase
   * is prone to OutOfMemoryExceptions when there are three or more
   * JUnit test methods.
   */
  @Test
  public void testAll() throws Exception {
    // Run all tests
    doTestTableCreateDrop();
    doTestThriftMetrics();
    doTestTableMutations();
    doTestTableTimestampsAndColumns();
    doTestTableScanners();
    doTestGetTableRegions();
    doTestFilterRegistration();
    doTestGetRegionInfo();
    doTestIncrements();
    doTestAppend();
    doTestCheckAndPut();
  }

  private ThriftHBaseServiceHandler getTestHandler() throws IOException {
    ThriftHBaseServiceHandler handler = new ThriftHBaseServiceHandler(UTIL.getConfiguration(),
        UserProvider.instantiate(UTIL.getConfiguration()));
    handler.connectionCache.setEffectiveUser("testuser");
    ((ConnectionCacheWithAuthToken) handler.connectionCache).setPassword("123456");
    return handler;
  }

  @Override
  public void doTestTableCreateDrop() throws Exception {
    doTestTableCreateDrop(getTestHandler());
  }

  @Override
  public void doTestTableMutations() throws Exception {
    doTestTableMutations(getTestHandler());
  }

  @Override
  public void doTestTableTimestampsAndColumns() throws Exception {
    doTestTableTimestampsAndColumns(getTestHandler());
  }

  @Override
  public void doTestTableScanners() throws Exception {
    doTestTableScanners(getTestHandler());
  }

  @Override
  public void doTestGetTableRegions() throws Exception {
    doTestGetTableRegions(getTestHandler());
  }

  @Override
  public void doTestGetRegionInfo() throws Exception {
    doTestGetRegionInfo(getTestHandler());
  }

  @Override
  public void doTestIncrements() throws Exception {
    ThriftHBaseServiceHandler handler = getTestHandler();
    createTestTables(handler);
    doTestIncrements(handler);
    dropTestTables(handler);
  }

  @Override
  public void doTestAppend() throws Exception {
    doTestAppend(getTestHandler());
  }

  @Override
  public void doTestCheckAndPut() throws Exception {
    doTestCheckAndPut(getTestHandler());
  }

  @Override
  public void testMetricsWithException() throws Exception {
    testMetricsWithException(getTestHandler());
  }

  @Override
  public void doTestThriftMetrics() throws Exception {
    LOG.info("START doTestThriftMetrics");
    Configuration conf = UTIL.getConfiguration();
    ThriftMetrics metrics = new ThriftMetrics(conf, ThriftMetrics.ThriftServerType.ONE);
    Hbase.Iface handler = getHandlerForMetricsTest(metrics, conf);
    doTestThriftMetrics(handler, metrics);
  }

  private Hbase.Iface getHandlerForMetricsTest(ThriftMetrics metrics, Configuration conf)
      throws IOException {
    ThriftHBaseServiceHandler handler = new MySlowHBaseHandler(conf);
    handler.connectionCache.setEffectiveUser("testuser");
    ((ConnectionCacheWithAuthToken) handler.connectionCache).setPassword("123456");

    return HbaseHandlerMetricsProxy.newInstance(handler, metrics, conf);
  }
}
