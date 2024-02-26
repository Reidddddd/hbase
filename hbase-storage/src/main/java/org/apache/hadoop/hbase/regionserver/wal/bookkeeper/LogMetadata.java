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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Class to store basic meta info of a log. The information is store as: "n" long values + one byte
 * where each long value represent one of the underlying ledgerIds and the final byte means, which
 * kind of compression this log used.
 * Thread unsafe. Not use concurrently.
 * Use {@link LedgerLogSystem#getLogMetadata(String)} to get the latest updated data.
 * Use {@link LedgerLogSystem#updateLogMetadata(String, LogMetadata)} to update the metadata.
 * The data consistency is guaranteed by zookeeper.
 */
@InterfaceAudience.Private
public class LogMetadata {
  private final List<Long> ledgerIds;
  private final boolean compressed;
  private boolean isClosed;
  private long dataSize;

  /**
   * The byte array is formatted as:
   * ledgerId (long) | ledgerId (long) | ... | length (long) | compressed (byte) | isClosed (byte)
   */
  LogMetadata(byte[] data) {
    if (data == null || data.length < Bytes.SIZEOF_LONG) {
      throw new IllegalArgumentException("The meta data should be at least one long type.");
    }
    this.ledgerIds = new ArrayList<>();
    int numOfLedgers = (data.length - Bytes.SIZEOF_LONG) / Bytes.SIZEOF_LONG;
    for (int i = 0; i < numOfLedgers; i++) {
      ledgerIds.add(Bytes.toLong(data, i * Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG));
    }

    this.dataSize = Bytes.toLong(data, numOfLedgers, Bytes.SIZEOF_LONG);
    // The compressed first check the data length matches our format mentioned in previous comment.
    // Then check the corresponding byte for compressed and closed.
    this.compressed = data.length % Bytes.SIZEOF_LONG == 2 && data[data.length - 2] != 0;
    this.isClosed = data.length % Bytes.SIZEOF_LONG == 2 && data[data.length - 1] != 0;
  }

  LogMetadata(long ledgerId, boolean compressed) {
    this.ledgerIds = new ArrayList<>();
    ledgerIds.add(ledgerId);
    this.compressed = compressed;
    this.isClosed = false;
  }

  public void addLedger(long ledgerId) {
    ledgerIds.add(ledgerId);
  }

  private byte[] serialize() {
    // N ledgerId + length + compressed + isClosed
    byte[] data = new byte[Bytes.SIZEOF_LONG * (ledgerIds.size() + 1) + 2];
    for (int i = 0; i < ledgerIds.size(); i++) {
      Bytes.putLong(data, i * Bytes.SIZEOF_LONG, ledgerIds.get(i));
    }
    Bytes.putLong(data, ledgerIds.size(), dataSize);
    Bytes.putByte(data, data.length - 2, compressed ? (byte) 1 : (byte) 0);
    Bytes.putByte(data, data.length - 1, isClosed ? (byte) 1 : (byte) 0);
    return data;
  }

  public List<Long> getLedgerIds() {
    return ledgerIds;
  }

  public boolean isCompressed() {
    return compressed;
  }

  public byte[] toBytes() {
    return serialize();
  }

  public Iterator<Long> getLedgerIdIterator() {
    return ledgerIds.listIterator();
  }

  public boolean isClosed() {
    return isClosed;
  }

  public void setClosed() {
    isClosed = true;
  }

  public long getDataSize() {
    return dataSize;
  }

  public void setDataSize(long dataSize) {
    this.dataSize = dataSize;
  }
}
