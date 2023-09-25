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

package org.apache.hadoop.hbase.schema;

import java.util.Iterator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(SmallTests.class)

public class TestSchemaBasic {
  private static final Logger LOG = LoggerFactory.getLogger(TestSchemaBasic.class);

  @Test
  public void testSchema() {
    TableName table = TableName.valueOf("test:schema");
    Schema schema = new Schema(table);

    // Test family exists
    schema.addFamily(Bytes.toBytes("column"));
    schema.addFamily(Bytes.toBytes("family"));
    schema.addFamily(Bytes.toBytes("qualifier"));

    Assert.assertTrue(schema.containFamily(new Famy("column")));
    Assert.assertTrue(schema.containFamily(new Famy(Bytes.toBytes("column"))));
    Assert.assertTrue(schema.containFamily(Bytes.toBytes("column")));
    Assert.assertTrue(schema.containFamily(new Famy("family")));
    Assert.assertTrue(schema.containFamily(new Famy("qualifier")));
    Assert.assertFalse(schema.containFamily(new Famy("fake")));

    // Test column exists
    schema.addColumn(Bytes.toBytes("true"), Bytes.toBytes("exist"));
    Assert.assertTrue(schema.containFamily(new Famy("true")));
    Assert.assertTrue(schema.containColumn(Bytes.toBytes("true"), Bytes.toBytes("exist")));
    Assert.assertFalse(schema.containColumn(Bytes.toBytes("true"), Bytes.toBytes("fake")));
    Assert.assertFalse(schema.containColumn(Bytes.toBytes("false"), Bytes.toBytes("exist")));
  }

  @Test
  public void testSchemaIterator() {
    TableName table = TableName.valueOf("test:iterator");
    Schema schema = new Schema(table);
    Famy fa = new Famy(Bytes.toBytes("fa"));
    Famy fb = new Famy(Bytes.toBytes("fb"));
    Famy ga = new Famy(Bytes.toBytes("ga"));
    Famy gb = new Famy(Bytes.toBytes("gb"));

    // Add columns
    schema.addColumn(fa.getFamily(), Bytes.toBytes("c"));
    schema.addColumn(fa.getFamily(), Bytes.toBytes("d"));
    schema.addColumn(fa.getFamily(), Bytes.toBytes("a"));
    schema.addColumn(fa.getFamily(), Bytes.toBytes("e"));
    schema.addColumn(fb.getFamily(), Bytes.toBytes("b"));
    schema.addColumn(ga.getFamily(), Bytes.toBytes("f"));
    schema.addColumn(gb.getFamily(), Bytes.toBytes("g"));

    // Iterate over it, the sequence is
    // fa:a, fa:c, fa:d, fa:e,
    // fb:b,
    // ga:f,
    // gb:g
    Iterator<Column> columnIt = schema.iterator();
    Assert.assertTrue(columnIt.hasNext());
    Assert.assertEquals(new Column(fa, new Qualy(Bytes.toBytes("a"))), columnIt.next());
    Assert.assertTrue(columnIt.hasNext());
    Assert.assertEquals(new Column(fa, new Qualy(Bytes.toBytes("c"))), columnIt.next());
    Assert.assertTrue(columnIt.hasNext());
    Assert.assertEquals(new Column(fa, new Qualy(Bytes.toBytes("d"))), columnIt.next());
    Assert.assertTrue(columnIt.hasNext());
    Assert.assertEquals(new Column(fa, new Qualy(Bytes.toBytes("e"))), columnIt.next());
    Assert.assertTrue(columnIt.hasNext());
    Assert.assertEquals(new Column(fb, new Qualy(Bytes.toBytes("b"))), columnIt.next());
    Assert.assertTrue(columnIt.hasNext());
    Assert.assertEquals(new Column(ga, new Qualy(Bytes.toBytes("f"))), columnIt.next());
    Assert.assertTrue(columnIt.hasNext());
    Assert.assertEquals(new Column(gb, new Qualy(Bytes.toBytes("g"))), columnIt.next());
    Assert.assertFalse(columnIt.hasNext());
  }

}
