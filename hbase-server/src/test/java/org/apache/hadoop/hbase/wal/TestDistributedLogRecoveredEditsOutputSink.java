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
package org.apache.hadoop.hbase.wal;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.doCallRealMethod;
import dlshade.org.apache.distributedlog.DLMTestUtil;
import dlshade.org.apache.distributedlog.TestDistributedLogBase;
import dlshade.org.apache.distributedlog.api.namespace.Namespace;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.regionserver.wal.DistributedLogAccessor;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({ SmallTests.class})
public class TestDistributedLogRecoveredEditsOutputSink extends TestDistributedLogBase {
  URI uri;
  Configuration hbaseConf;
  Namespace namespace;

  @Before
  public void before() throws Exception {
    uri = DLMTestUtil.createDLMURI(zkPort, "");
    ensureURICreated(uri);
    hbaseConf = HBaseConfiguration.create();
    hbaseConf.set("distributedlog.znode.parent", "/messaging/distributedlog");
    hbaseConf.set("distributedlog.zk.quorum", zkServers);

    namespace = DistributedLogAccessor.getInstance(hbaseConf).getNamespace();
  }

  @Test
  public void testCreateWAPWithEmptyOldLog() throws Exception {
    String emptyLog = "EmptyLog";
    String logStreamName = DistributedLogAccessor.getDistributedLogStreamName(hbaseConf);
    namespace.createLog(emptyLog);

    DistributedLogRecoveredEditsOutputSink outputSink =
      Mockito.mock(DistributedLogRecoveredEditsOutputSink.class);
    doCallRealMethod().when(outputSink).handleOldExistingLog(namespace, emptyLog, logStreamName);

    try {
      outputSink.handleOldExistingLog(namespace, emptyLog, logStreamName);
    } catch (Exception e) {
      fail("Should not have exception when we create WAP for an existing log path." + e);
    }
  }
}
