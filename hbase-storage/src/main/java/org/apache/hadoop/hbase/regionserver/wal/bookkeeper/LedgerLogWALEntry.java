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
package org.apache.hadoop.hbase.regionserver.wal.bookkeeper;

import java.io.IOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.mvcc.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.wal.GenericWALEntry;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class LedgerLogWALEntry extends GenericWALEntry {
  private final long entryId;

  public LedgerLogWALEntry(long sequence, long entryId, WALKey key, WALEdit edit,
      HTableDescriptor htd, HRegionInfo hri, boolean inMemstore,
      MultiVersionConcurrencyControl.WriteEntry we) throws IOException {
    super(sequence, key, edit, htd, hri, inMemstore, we);
    this.entryId = entryId;
  }

  long getEntryId() {
    return entryId;
  }
}
