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

import com.google.common.annotations.VisibleForTesting;
import dlshade.org.apache.bookkeeper.client.BKException;
import dlshade.org.apache.bookkeeper.client.LedgerHandle;
import dlshade.org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import dlshade.org.apache.distributedlog.exceptions.NotYetImplementedException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.wal.ReaderBase;
import org.apache.hadoop.hbase.regionserver.wal.bookkeeper.compress.DisableDecompressor;
import org.apache.hadoop.hbase.regionserver.wal.bookkeeper.compress.LedgerEntryDecompressor;
import org.apache.hadoop.hbase.regionserver.wal.bookkeeper.compress.Lz4Decompressor;
import org.apache.hadoop.hbase.util.LedgerUtil;
import org.apache.hadoop.hbase.wal.Entry;
import org.apache.hadoop.hbase.wal.ServiceBasedReader;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class LedgerLogReader extends ReaderBase implements ServiceBasedReader {
  private static final Log LOG = LogFactory.getLog(LedgerLogReader.class);

  // Indicate which entryId we will read next.
  private final AtomicLong cursor = new AtomicLong(0);

  private Configuration conf;
  private LedgerHandle ledgerHandle;
  private LedgerLogSystem logSystem;
  private String logName;
  private LedgerEntryDecompressor decompressor;
  private LogMetadata metadata;
  private Iterator<Long> ledgerIdIterator;

  // Constructor for reflection.
  public LedgerLogReader() {

  }

  @Override
  public void init(Configuration conf, String logName) throws URISyntaxException, IOException {
    this.conf = conf;
    this.logName = logName;
    logSystem = LedgerLogSystem.getInstance(conf);
    metadata = logSystem.getLogMetadata(logName);
    ledgerIdIterator = metadata.getLedgerIdIterator();
    if (ledgerIdIterator.hasNext()) {
      ledgerHandle = logSystem.openLedgerToRead(ledgerIdIterator.next());
    }
    decompressor = logSystem.getLogMetadata(logName).isCompressed()
      ? new Lz4Decompressor() : new DisableDecompressor();
  }

  @VisibleForTesting
  public LedgerLogReader(Configuration conf, LedgerHandle ledgerHandle, String logName)
      throws IOException {
    this.conf = conf;
    this.ledgerHandle = ledgerHandle;
    this.logSystem = LedgerLogSystem.getInstance(conf);
    this.logName = logName;
    decompressor = logSystem.getLogMetadata(logName).isCompressed()
      ? new Lz4Decompressor() : new DisableDecompressor();
  }

  @Override
  protected String initReader(InputStream stream) throws IOException {
    return null;
  }

  @Override
  protected void initAfterCompression() throws IOException {

  }

  @Override
  protected void initAfterCompression(String cellCodecClsName) throws IOException {

  }

  @Override
  protected boolean hasCompression() {
    return false;
  }

  @Override
  protected boolean hasTagCompression() {
    return false;
  }

  @Override
  protected boolean readNext(Entry entry) throws IOException {
    if (ledgerHandle == null) {
      // Failed at initial phase. This is an empty log.
      return false;
    }

    byte[] compressed = null;
    while (compressed == null) {
      try {
        LastConfirmedAndEntry lca =
          ledgerHandle.readLastAddConfirmedAndEntry(cursor.get(), 60000, true);
        if (cursor.get() > ledgerHandle.getLastAddConfirmed()) {
          if (hasNoMoreLedger()) {
            return false;
          }
        } else {
          cursor.incrementAndGet();
          compressed = lca.getEntry().getEntryBytes();
        }
      } catch (Exception e) {
        if (e instanceof BKException.BKNoSuchEntryException) {
          // We arrived the end of file, switch to next ledger if possible. Return false otherwise.
          if (hasNoMoreLedger()) {
            return false;
          }
        } else if (e instanceof InterruptedException) {
          throw new IOException(e);
        }
      }
    }

    byte[] data = decompressor.decompress(compressed);
    try {
      if (data == null || data.length == 0) {
        return false;
      }
      return LedgerUtil.parseToEntry(data, entry);
    } catch (Exception e) {
      String message = " while reading " + logName + " met unexpected exception at ledgerId: "
        + ledgerHandle.getId();
      throw new IOException(message, e);
    }
  }

  // Check whether we have more ledger to read and roll to the next ledger.
  // Return true if we have, false otherwise.
  private boolean hasNoMoreLedger() throws IOException {
    if (ledgerIdIterator.hasNext()) {
      ledgerHandle = logSystem.openLedgerToRead(ledgerIdIterator.next());
      return false;
    }
    return true;
  }

  @Override
  public void seek(long pos) throws IOException {
    throw new NotYetImplementedException("Ledger Reader does not support seek.");
  }

  @Override
  public long getPosition() throws IOException {
    throw new NotYetImplementedException("Ledger Reader does not support getPosition.");
  }

  @Override
  public void reset() throws IOException {
    metadata = logSystem.getLogMetadata(logName);
    ledgerIdIterator = metadata.getLedgerIdIterator();
    if (hasNoMoreLedger()) {
      throw new IllegalStateException("Log " + logName + " has no underlying ledger to read.");
    }
    cursor.set(0);
  }

  @Override
  public void close() throws IOException {
    try {
      if (ledgerHandle != null) {
        ledgerHandle.close();
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

}
