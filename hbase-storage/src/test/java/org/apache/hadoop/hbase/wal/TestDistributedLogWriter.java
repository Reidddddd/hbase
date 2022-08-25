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
package org.apache.hadoop.hbase.wal;

import static org.junit.Assert.assertFalse;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.distributedlog.shaded.AppendOnlyStreamReader;
import org.apache.distributedlog.shaded.DLMTestUtil;
import org.apache.distributedlog.shaded.DistributedLogConfiguration;
import org.apache.distributedlog.shaded.LocalDLMEmulator;
import org.apache.distributedlog.shaded.TestDistributedLogBase;
import org.apache.distributedlog.shaded.api.namespace.Namespace;
import org.apache.distributedlog.shaded.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.shaded.exceptions.EndOfStreamException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.DistributedLogAccessor;
import org.apache.hadoop.hbase.regionserver.wal.DistributedLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * The BK provided mini bookie cluster does not work.
 * TODO: Refine this UT in the future.
 */
@Category(MediumTests.class)
public class TestDistributedLogWriter extends TestDistributedLogBase {
  private static final Log LOG = LogFactory.getLog(TestDistributedLogWriter.class);

  private static final byte[] ROW = Bytes.toBytes("row");
  private static final byte[] CF = Bytes.toBytes("cf");
  private static final byte[] QUALIFIER = Bytes.toBytes("qualifier");
  private static final byte[] VALUE = Bytes.toBytes("value");
  private static final String FAKE_LOG = "fakeLog";

  private URI uri;
  private Namespace namespace;

  @Rule
  public final TestName runtime = new TestName();

  @Before
  public void setUp() throws Exception {
    this.uri = DLMTestUtil.createDLMURI(zkPort, "");
    ensureURICreated(this.uri);
    this.namespace = NamespaceBuilder.newBuilder()
      .conf(new DistributedLogConfiguration())
      .uri(uri)
      .build();
  }

  public TestDistributedLogWriter() {
    super();
  }

  /**
   * Test the DistributedProtobufLogWriter could write bytes into bookies.
   */
  @Test
  public void testCreateWriter() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    // The following two DistributedLog related parameters are initialized by the super class.
    // We just copy them to our hbase configuration.
    conf.set("distributedlog.znode.parent", "/messaging/distributedlog");
    conf.set("distributedlog.zk.quorum", zkServers);
    conf.setClass("hbase.regionserver.hlog.writer.impl", DistributedLogWriter.class,
      Writer.class);

    Writer writer = WALUtils.createWriter(conf, null, new Path(runtime.getMethodName()), false);
    WALEdit cols = new WALEdit();
    cols.add(new KeyValue(ROW, CF, QUALIFIER, VALUE));

    HRegionInfo hri = new HRegionInfo(TableName.valueOf(runtime.getMethodName()),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    WALKey key = new WALKey(hri.getEncodedNameAsBytes(), hri.getTable());

    Entry entry = new Entry(key, cols);
    writer.append(entry);
    writer.sync();
    writer.close();

    AppendOnlyStreamReader reader = DistributedLogAccessor.getInstance(conf).getNamespace().openLog(
      runtime.getMethodName()).getAppendOnlyStreamReader();

    List<Integer> res = new ArrayList<>();
    while (true) {
      try {
        res.add(reader.read());
      } catch (EndOfStreamException e) {
        break;
      }
    }
    assertFalse("We should read something from the distributed log.", res.isEmpty());
  }
}
