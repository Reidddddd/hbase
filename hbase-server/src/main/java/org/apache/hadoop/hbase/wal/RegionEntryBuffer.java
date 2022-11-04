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

import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A buffer of some number of edits for a given region.
 * This accumulates edits and also provides a memory optimization in order to
 * share a single byte array instance for the table and region name.
 * Also tracks memory usage of the accumulated edits.
 */
@InterfaceAudience.Private
public class RegionEntryBuffer implements HeapSize {
  long heapInBuffer = 0;
  List<Entry> entryBuffer;
  TableName tableName;
  byte[] encodedRegionName;

  RegionEntryBuffer(TableName tableName, byte[] region) {
    this.tableName = tableName;
    this.encodedRegionName = region;
    this.entryBuffer = new LinkedList<Entry>();
  }

  long appendEntry(Entry entry) {
    internify(entry);
    entryBuffer.add(entry);
    long incrHeap = entry.getEdit().heapSize() + ClassSize.align(2 * ClassSize.REFERENCE);
      // WALKey pointers
      // TODO linkedlist entry
    heapInBuffer += incrHeap;
    return incrHeap;
  }

  private void internify(Entry entry) {
    WALKey k = entry.getKey();
    k.internTableName(this.tableName);
    k.internEncodedRegionName(this.encodedRegionName);
  }

  @Override
  public long heapSize() {
    return heapInBuffer;
  }

  public byte[] getEncodedRegionName() {
    return encodedRegionName;
  }

  public List<Entry> getEntryBuffer() {
    return entryBuffer;
  }

  public TableName getTableName() {
    return tableName;
  }
}
