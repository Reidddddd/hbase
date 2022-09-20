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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.net.URI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.distributedlog.shaded.DLMTestUtil;
import org.apache.distributedlog.shaded.TestDistributedLogBase;
import org.apache.distributedlog.shaded.api.namespace.Namespace;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.Entry;
import org.apache.hadoop.hbase.wal.Reader;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALUtils;
import org.apache.hadoop.hbase.wal.Writer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * WAL tests that can be reused across providers.
 */
@Category(MediumTests.class)
public class TestDistributedLogCorrectness extends TestDistributedLogBase {
  private static final Log LOG = LogFactory.getLog(TestProtobufLog.class);
  private Configuration conf;

  private URI uri;
  private Namespace namespace;

  @Rule
  public final TestName currentTest = new TestName();

  @Before
  public void setUp() throws Exception {
    conf = HBaseConfiguration.create();
    this.uri = DLMTestUtil.createDLMURI(zkPort, "");
    ensureURICreated(this.uri);

    // The following two DistributedLog related parameters are initialized by the super class.
    // We just copy them to our hbase configuration.
    conf.set("distributedlog.znode.parent", "/messaging/distributedlog");
    conf.set("distributedlog.zk.quorum", zkServers);
    conf.setClass("hbase.regionserver.hlog.writer.impl", DistributedLogWriter.class,
      Writer.class);
    conf.setClass("hbase.regionserver.hlog.reader.impl", DistributedLogReader.class,
      Reader.class);

    this.namespace = DistributedLogAccessor.getInstance(conf).getNamespace();
  }

  @After
  public void tearDown() throws Exception {
    namespace.close();
  }

  /**
   * Reads the WAL with and without WALTrailer.
   */
  @Test
  public void testWALTrailer() throws IOException {
    // read With trailer.
    doRead(true);
    // read without trailer
    doRead(false);
  }

  /**
   * Appends entries in the WAL and reads it.
   * @param withTrailer If 'withTrailer' is true, it calls a close on the WALwriter before reading
   *          so that a trailer is appended to the WAL. Otherwise, it starts reading after the sync
   *          call. This means that reader is not aware of the trailer. In this scenario, if the
   *          reader tries to read the trailer in its next() call, it returns false from
   *          ProtoBufLogReader.
   */
  private void doRead(boolean withTrailer) throws IOException {
    final int columnCount = 5;
    final int recordCount = 5;
    final TableName tableName =
      TableName.valueOf("tablename");
    final byte[] row = Bytes.toBytes("row");
    long timestamp = System.currentTimeMillis();
    Path path = new Path("tempwal");
    // delete the log if already exists, for test only
    String pathString = path.toString();
    if (namespace.logExists(pathString)) {
      namespace.deleteLog(pathString);
    }
    Writer writer = null;
    DistributedLogReader reader = null;
    try {
      HRegionInfo hri = new HRegionInfo(tableName,
        HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      HTableDescriptor htd = new HTableDescriptor(tableName);
      // Write log in pb format.
      writer = WALUtils.createWALWriter(null, path, conf);
      for (int i = 0; i < recordCount; ++i) {
        WALKey key = new WALKey(
          hri.getEncodedNameAsBytes(), tableName, i, timestamp, HConstants.DEFAULT_CLUSTER_ID);
        WALEdit edit = new WALEdit();
        for (int j = 0; j < columnCount; ++j) {
          if (i == 0) {
            htd.addFamily(new HColumnDescriptor("column" + j));
          }
          String value = i + "" + j;
          edit.add(new KeyValue(row, row, row, timestamp, Bytes.toBytes(value)));
        }
        writer.append(new Entry(key, edit));
      }
      ((DistributedLogWriter) writer).force();
      if (withTrailer) {
        writer.close();
      }

      // Now read the log using standard means.
      reader = (DistributedLogReader) WALUtils.createReader(null, path, conf);
      if (withTrailer) {
        assertNotNull(reader.trailer);
      } else {
        assertNull(reader.trailer);
      }
      for (int i = 0; i < recordCount; ++i) {
        Entry entry = reader.next();
        assertNotNull(entry);
        assertEquals(columnCount, entry.getEdit().size());
        assertArrayEquals(hri.getEncodedNameAsBytes(), entry.getKey().getEncodedRegionName());
        assertEquals(tableName, entry.getKey().getTablename());
        int idx = 0;
        for (Cell val : entry.getEdit().getCells()) {
          assertTrue(Bytes.equals(row, val.getRow()));
          String value = i + "" + idx;
          assertArrayEquals(Bytes.toBytes(value), val.getValue());
          idx++;
        }
      }
      Entry entry = reader.next();
      assertNull(entry);
    } finally {
      if (writer != null) {
        writer.close();
      }
      if (reader != null) {
        reader.close();
      }
    }
  }
}
