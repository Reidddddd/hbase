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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.distributedlog.shaded.DLMTestUtil;
import org.apache.distributedlog.shaded.DistributedLogConfiguration;
import org.apache.distributedlog.shaded.TestDistributedLogBase;
import org.apache.distributedlog.shaded.api.namespace.Namespace;
import org.apache.distributedlog.shaded.api.namespace.NamespaceBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.DistributedLogReader;
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
 * Test DistributedLog based WALWriter and Reader.
 */
@Category(MediumTests.class)
public class TestDistributedLogWriterAndReader extends TestDistributedLogBase {
  private static final Log LOG = LogFactory.getLog(TestDistributedLogWriterAndReader.class);

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

  public TestDistributedLogWriterAndReader() {
    super();
  }

  /**
   * Test the DistributedProtobufLogWriter could write bytes into bookies.
   */
  @Test
  public void testCorrectness() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    // The following two DistributedLog related parameters are initialized by the super class.
    // We just copy them to our hbase configuration.
    conf.set("distributedlog.znode.parent", "/messaging/distributedlog");
    conf.set("distributedlog.zk.quorum", zkServers);
    conf.setClass("hbase.regionserver.hlog.writer.impl", DistributedLogWriter.class,
      Writer.class);
    conf.setClass("hbase.regionserver.hlog.reader.impl", DistributedLogReader.class,
      Reader.class);

    Writer writer = WALUtils.createWriter(conf, null, new Path(runtime.getMethodName()), false);
    WALEdit cols = new WALEdit();
    cols.add(new KeyValue(ROW, CF, QUALIFIER, VALUE));
    List<Cell> cells = getRandomCells(10);
    for (Cell cell : cells) {
      cols.add(cell);
    }

    HRegionInfo hri = new HRegionInfo(TableName.valueOf(runtime.getMethodName()),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    WALKey key = new WALKey(hri.getEncodedNameAsBytes(), hri.getTable());

    Entry entry = new Entry(key, cols);
    writer.append(entry);
    writer.sync();
    writer.close();

    Reader reader = WALUtils.createReader(null, new Path(runtime.getMethodName()), conf);
    Entry readEntry = reader.next();

    assertEquals(readEntry.toString(), entry.toString());

    List<Cell> writeCells = entry.getEdit().getCells();
    List<Cell> readCells = readEntry.getEdit().getCells();

    assertEquals(writeCells.size(), readCells.size());

    for (int i = 0; i < writeCells.size(); i++) {
      assertTrue(CellComparator.equals(writeCells.get(i), readCells.get(i)));
    }
  }

  @Test
  public void testCorrectnessWithoutWALCompression() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    // The following two DistributedLog related parameters are initialized by the super class.
    // We just copy them to our hbase configuration.
    conf.set("distributedlog.znode.parent", "/messaging/distributedlog");
    conf.set("distributedlog.zk.quorum", zkServers);
    conf.setBoolean("hbase.regionserver.wal.enablecompression", false);
    conf.setClass("hbase.regionserver.hlog.writer.impl", DistributedLogWriter.class, Writer.class);
    conf.setClass("hbase.regionserver.hlog.reader.impl", DistributedLogReader.class, Reader.class);

    Writer writer = WALUtils.createWriter(conf, null, new Path(runtime.getMethodName()), false);
    WALEdit cols = new WALEdit();
    cols.add(new KeyValue(ROW, CF, QUALIFIER, VALUE));
    List<Cell> cells = getRandomCells(10);
    for (Cell cell : cells) {
      cols.add(cell);
    }

    HRegionInfo hri = new HRegionInfo(TableName.valueOf(runtime.getMethodName()), HConstants.EMPTY_START_ROW,
      HConstants.EMPTY_END_ROW);
    WALKey key = new WALKey(hri.getEncodedNameAsBytes(), hri.getTable());

    Entry entry = new Entry(key, cols);
    writer.append(entry);
    writer.sync();
    writer.close();

    Reader reader = WALUtils.createReader(null, new Path(runtime.getMethodName()), conf);
    Entry readEntry = reader.next();

    assertEquals(readEntry.toString(), entry.toString());

    List<Cell> writeCells = entry.getEdit().getCells();
    List<Cell> readCells = readEntry.getEdit().getCells();

    assertEquals(writeCells.size(), readCells.size());

    for (int i = 0; i < writeCells.size(); i++) {
      assertTrue(CellComparator.equals(writeCells.get(i), readCells.get(i)));
    }
  }

  private List<Cell> getRandomCells(int num) {
    List<Cell> res = new ArrayList<>(num);
    for (int i = 0; i < num; i++) {
      byte[] row = new byte[ThreadLocalRandom.current().nextInt(1, 10)];
      byte[] family = new byte[ThreadLocalRandom.current().nextInt(1, 10)];
      byte[] qualifier = new byte[ThreadLocalRandom.current().nextInt(1, 10)];
      byte[] value = new byte[ThreadLocalRandom.current().nextInt(1, 10)];
      ThreadLocalRandom.current().nextBytes(row);
      ThreadLocalRandom.current().nextBytes(family);
      ThreadLocalRandom.current().nextBytes(qualifier);
      ThreadLocalRandom.current().nextBytes(value);
      res.add(new KeyValue(row, family, qualifier, value));
    }
    return res;
  }
}
