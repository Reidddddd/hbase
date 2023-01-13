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
package org.apache.hadoop.hbase.master.cleaner;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import java.io.IOException;
import java.net.URI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.distributedlog.shaded.DLMTestUtil;
import org.apache.distributedlog.shaded.DistributedLogConfiguration;
import org.apache.distributedlog.shaded.TestDistributedLogBase;
import org.apache.distributedlog.shaded.api.namespace.Namespace;
import org.apache.distributedlog.shaded.api.namespace.NamespaceBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category(SmallTests.class)
public class TestDistributedLogCleaner extends TestDistributedLogBase {
  private static final Log LOG = LogFactory.getLog(TestDistributedLogCleaner.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private URI uri;
  private Namespace namespace;

  @Before
  public void setUp() throws Exception {
    this.uri = DLMTestUtil.createDLMURI(zkPort, "");
    ensureURICreated(this.uri);
    this.namespace = NamespaceBuilder.newBuilder()
      .conf(new DistributedLogConfiguration())
      .uri(uri)
      .build();
  }

  @Test
  public void testDeleteLog() throws Exception {
    Configuration conf = new Configuration(UTIL.getConfiguration());
    conf.setInt("hbase.cleaner.scan.dir.concurrent.size", 1);
    conf.set("distributedlog.znode.parent", "/messaging/distributedlog");
    conf.set("distributedlog.zk.quorum", zkServers);

    String fakeLogName = "oldWALs/testlog";
    namespace.createLog(fakeLogName);
    int cleanerInterval = 1000; // 1 second.
    DistributedLogCleaner cleaner = new DistributedLogCleaner(cleanerInterval,
      null, conf, new Path("oldWALs"));
    assertTrue(namespace.logExists(fakeLogName));
    assertTrue(cleaner.runCleaner());
    // Sleep 6 seconds to wait for log deletion.
    assertFalse(namespace.logExists(fakeLogName));
  }

  @Test
  public void testDeleteLogWithIOException() throws Exception {
    Configuration conf = new Configuration(UTIL.getConfiguration());
    conf.setInt("hbase.cleaner.scan.dir.concurrent.size", 1);
    conf.set("distributedlog.znode.parent", "/messaging/distributedlog");
    conf.set("distributedlog.zk.quorum", zkServers);

    String fakeLogName = "oldWALs/testlog";
    namespace.createLog(fakeLogName);
    Namespace spyNamespace = Mockito.spy(namespace);
    doThrow(new IOException("IOException for testing")).when(spyNamespace)
      .deleteLog(fakeLogName);

    int cleanerInterval = 1000; // 1 second.
    DistributedLogCleaner cleaner = new DistributedLogCleaner(cleanerInterval,
      null, conf, new Path("oldWALs"), spyNamespace);

    assertTrue(namespace.logExists(fakeLogName));
    assertFalse(cleaner.runCleaner());
    // Sleep 6 seconds to wait for log deletion.
    assertTrue(namespace.logExists(fakeLogName));

  }
}
