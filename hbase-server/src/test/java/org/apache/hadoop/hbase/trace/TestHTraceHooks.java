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
package org.apache.hadoop.hbase.trace;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.htrace.core.POJOSpanReceiver;
import org.apache.htrace.core.Sampler;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanId;
import org.apache.htrace.core.TraceScope;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestHTraceHooks {
  private static final Log LOG = LogFactory.getLog(TestHTraceHooks.class);

  private static final byte[] FAMILY_BYTES = "family".getBytes();
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static POJOSpanReceiver rcvr;

  @BeforeClass
  public static void before() throws Exception {
    TEST_UTIL.startMiniCluster(2, 3);
    TEST_UTIL.waitUntilNoRegionsInTransition();
    rcvr = new POJOSpanReceiver(new HBaseHTraceConfiguration(TEST_UTIL.getConfiguration()));
    TraceUtil.addReceiver(rcvr);
    TraceUtil.addSampler(new Sampler() {
      @Override
      public boolean next() {
        return true;
      }
    });
  }

  @AfterClass
  public static void after() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    TraceUtil.removeReceiver(rcvr);
    rcvr = null;
  }

  @Test
  public void testTraceCreateTable() throws Exception {
    Table table;
    Span createTableSpan;
    try (TraceScope scope = TraceUtil.createTrace("creating table")) {
      createTableSpan = scope.getSpan();
      table = TEST_UTIL.createTable(TableName.valueOf("table"),
        FAMILY_BYTES);
    }

    // Some table creation is async.  Need to make sure that everything is full in before
    // checking to see if the spans are there.
    TEST_UTIL.waitFor(10000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return rcvr == null || rcvr.getSpans().size() >= 5;
      }
    });

    Collection<Span> spans = Sets.newHashSet(rcvr.getSpans());
    TraceTree traceTree = new TraceTree(spans);
    List<Span> roots =
      new LinkedList<>(traceTree.getSpansByParent().find(createTableSpan.getSpanId()));
    LOG.info("Span id is: " + createTableSpan.getSpanId());

    assertEquals(3, roots.size());
    assertEquals("creating table", createTableSpan.getDescription());

    if (spans != null) {
      assertTrue(spans.size() > 5);
    }

    Put put = new Put("row".getBytes());
    put.addColumn(FAMILY_BYTES, "col".getBytes(), "value".getBytes());

    Span putSpan;
    try (TraceScope scope = TraceUtil.createTrace("doing put")) {
      putSpan = scope.getSpan();
      table.put(put);
    }

    spans = rcvr.getSpans();
    traceTree = new TraceTree(spans);
    roots.clear();
    roots.addAll(traceTree.getSpansByParent().find(putSpan.getSpanId()));
    assertEquals(1, roots.size());
  }
}
