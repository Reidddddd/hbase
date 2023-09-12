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
    // Add columns
    schema.addColumn(Bytes.toBytes("fa"), Bytes.toBytes("c"));
    schema.addColumn(Bytes.toBytes("fa"), Bytes.toBytes("d"));
    schema.addColumn(Bytes.toBytes("fa"), Bytes.toBytes("a"));
    schema.addColumn(Bytes.toBytes("fa"), Bytes.toBytes("e"));
    schema.addColumn(Bytes.toBytes("fb"), Bytes.toBytes("b"));
    schema.addColumn(Bytes.toBytes("ga"), Bytes.toBytes("f"));
    schema.addColumn(Bytes.toBytes("gb"), Bytes.toBytes("g"));
    // Iterate over it
    Iterator<byte[]> rowkeyIterator = schema.iterator();
    byte[] previous = null;
    byte[] current;
    while (rowkeyIterator.hasNext()) {
      if (previous == null) {
        previous = rowkeyIterator.next();
        LOG.info("Row key: " + Bytes.toString(previous));
        // The first one should be table name
        Assert.assertTrue(Bytes.equals(previous, table.getName()));
        continue;
      }
      current = rowkeyIterator.next();
      Assert.assertTrue(Bytes.compareTo(previous, current) < 0);
      LOG.info("Row key: " + Bytes.toString(current));
      previous = current;
    }
  }

}
