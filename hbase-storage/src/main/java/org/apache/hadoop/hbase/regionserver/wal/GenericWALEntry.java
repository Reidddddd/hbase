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
package org.apache.hadoop.hbase.regionserver.wal;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.mvcc.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.apache.hadoop.hbase.wal.Entry;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A WAL Entry for {@link FSHLog} implementation.  Immutable.
 * A subclass of {@link Entry} that carries extra info across the ring buffer such as
 * region sequence id (we want to use this later, just before we write the WAL to ensure region
 * edits maintain order).  The extra info added here is not 'serialized' as part of the WALEdit
 * hence marked 'transient' to underline this fact.  It also adds mechanism so we can wait on
 * the assign of the region sequence id.  See #stampRegionSequenceId().
 */
@InterfaceAudience.Private
class GenericWALEntry extends Entry {
  // The below data members are denoted 'transient' just to highlight these are not persisted;
  // they are only in memory and held here while passing over the ring buffer.
  private final transient long sequence;
  private final transient boolean inMemstore;
  private final transient HTableDescriptor htd;
  private final transient HRegionInfo hri;
  private final Set<byte[]> familyNames;

  GenericWALEntry(final long sequence, final WALKey key, final WALEdit edit,
                  final HTableDescriptor htd, final HRegionInfo hri, final boolean inMemstore,
                  final MultiVersionConcurrencyControl.WriteEntry we) throws IOException {
    super(key, edit);
    this.inMemstore = inMemstore;
    this.htd = htd;
    this.hri = hri;
    this.sequence = sequence;
    if (inMemstore) {
      // construct familyNames here to reduce the work of log sinker.
      // Also stamp region sequenceId in the same round to improve performance.
      if (edit.getFamilies() != null) {
        familyNames = edit.getFamilies();
        if (!edit.isReplay()) {
          long regionSequenceId = we.getWriteNumber();
          for (Cell cell : edit.getCells()) {
            CellUtil.setSequenceId(cell, regionSequenceId);
          }
        }
      } else {
        familyNames = getFamilyNamesFromEdit(edit, we.getWriteNumber());
      }
    } else {
      this.familyNames = Collections.<byte[]> emptySet();
    }
    this.getKey().setWriteEntry(we);
  }

  private Set<byte[]> getFamilyNamesFromEdit(WALEdit edit, long regionSequenceId)
      throws IOException {
    ArrayList<Cell> cells = edit.getCells();
    if (CollectionUtils.isEmpty(cells)) {
      return Collections.emptySet();
    } else {
      Set<byte[]> familySet = Sets.newTreeSet(Bytes.BYTES_COMPARATOR);
      for (Cell cell : cells) {
        if (!CellUtil.matchingFamily(cell, WALEdit.METAFAMILY)) {
          familySet.add(CellUtil.cloneFamily(cell));
        }
        // We checked inMemory here before. But this function is only invoked for inMemory case.
        // So, the check condition is removed.
        if (!edit.isReplay()) {
          CellUtil.setSequenceId(cell, regionSequenceId);
        }
      }
      return Collections.unmodifiableSet(familySet);
    }
  }

  @Override
  public String toString() {
    return "sequence=" + this.sequence + ", " + super.toString();
  };

  boolean isInMemstore() {
    return this.inMemstore;
  }

  HTableDescriptor getHTableDescriptor() {
    return this.htd;
  }

  HRegionInfo getHRegionInfo() {
    return this.hri;
  }

  /**
   * @return The sequence on the ring buffer when this edit was added.
   */
  long getSequence() {
    return this.sequence;
  }

  /**
   * @return the family names which are effected by this edit.
   */
  Set<byte[]> getFamilyNames() {
    return familyNames;
  }
}
