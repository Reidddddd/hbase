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
import dlshade.org.apache.bookkeeper.client.api.WriteFlag;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.regionserver.wal.bookkeeper.compress.DisableCompressor;
import org.apache.hadoop.hbase.regionserver.wal.bookkeeper.compress.LedgerEntryCompressor;
import org.apache.hadoop.hbase.regionserver.wal.bookkeeper.compress.Lz4Compressor;
import org.apache.hadoop.hbase.util.LedgerUtil;
import org.apache.hadoop.hbase.wal.Entry;
import org.apache.hadoop.hbase.wal.ServiceBasedWriter;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

@InterfaceAudience.Private
public class LedgerLogWriter implements ServiceBasedWriter {
  private static final Log LOG = LogFactory.getLog(LedgerLogWriter.class);

  // The highest unsynced txId. Auto incremental.
  // Only used when append none GenericWALEntry.
  private final AtomicLong txId = new AtomicLong(0);
  private final AtomicLong writtenSize = new AtomicLong(0);
  private final AtomicBoolean writerClosed = new AtomicBoolean(false);
  private final AtomicInteger bkCode = new AtomicInteger(BKException.Code.OK);
  private final Executor executor = Executors.newVirtualThreadPerTaskExecutor();
  private final Object rollLedgerObj = new Object();

  private LedgerLogSystem logSystem;
  private Configuration conf;
  private String logName;
  private LedgerEntryCompressor compressor;
  private AtomicLong highestSyncedSequenceId;
  private LogMetadata metadata;

  private volatile LedgerHandle ledgerHandle;

  // Constructor for reflection.
  public LedgerLogWriter() {

  }

  public LedgerLogWriter(Configuration conf, String logName)
      throws IOException, URISyntaxException {
    init(conf, logName);
  }

  public void setHighestSyncedSequenceId(AtomicLong highestSyncedSequenceId) {
    this.highestSyncedSequenceId = highestSyncedSequenceId;
  }

  // Only used for UTs.
  @VisibleForTesting
  public LedgerLogWriter(Configuration conf, LedgerHandle ledgerHandle, String logName,
      LedgerLogSystem ledgerLogSystem) throws IOException {
    logSystem = ledgerLogSystem;
    this.conf = conf;
    this.ledgerHandle = ledgerHandle;
    this.logName = logName;
    metadata = logSystem.getLogMetadata(logName);
    compressor = conf.getBoolean(BKConstants.LEDGER_COMPRESSED,
      BKConstants.DEFAULT_LEDGER_COMPRESSED) ? new Lz4Compressor() : new DisableCompressor();
  }

  @Override
  public void sync() throws IOException {
    try {
      ledgerHandle.force().join();
      checkException();
    } catch (CompletionException e) {
      Throwable t = e.getCause();
      throw t instanceof IOException ? (IOException) t : new IOException(t);
    }
  }

  @Override
  public void append(Entry entry) throws IOException {
    if (writerClosed.get()) {
      throw new IOException("Log writer: " + logName + " is closed.");
    }

    // We do not support wal compression here.
    // Using compression of BK instead.
    WALProtos.LedgerEntry ledgerEntry = LedgerUtil.toLedgerEntryWithoutCompression(entry);
    byte[] data = compressor.compress(ledgerEntry.toByteArray());

    // In prod we must only accept LedgerLogWALEntry.
    // But for some UT cases, we need to be compatible with customized WALEntry.
    final long entryId = entry instanceof LedgerLogWALEntry ?
      ((LedgerLogWALEntry) entry).getEntryId() : txId.getAndIncrement();
    long sequenceId = entry instanceof LedgerLogWALEntry ?
      ((LedgerLogWALEntry) entry).getSequence() : txId.get();

    appendBytes(entryId, data, sequenceId);
  }

  @Override
  public long getLength() throws IOException {
    return writtenSize.get();
  }

  // Do not close ledgerHandle in this function !!!
  // The close will makes few ongoing add entry requests at bookie side fail.
  @Override
  public void close() throws IOException {
    try {
      writerClosed.compareAndSet(false, true);
      if (ledgerHandle != null) {
        LOG.info("Ledger closing: " + getLogName() + " id: " + getLedgerId() + " length: "
          + ledgerHandle.getLength() + " but written size:" + writtenSize.get());
        ledgerHandle.force().get();
      }
    } catch (Exception e) {
      if (e instanceof ExecutionException) {
        Throwable bke = e.getCause();
        if (bke instanceof BKException) {
          logBKExceptionInClose((BKException) bke);
        }
      } else {
        LOG.warn("Unexpected exception met when close log " + logName, e);
      }
    } finally {
      // We need this check as the node should be already renamed or deleted.
      if (logSystem.logExists(logName)) {
        metadata.setClosed();
        metadata.setDataSize(writtenSize.get());
        logSystem.updateLogMetadata(logName, metadata);
      }
      logSystem.unlockPath(logName);
    }
  }

  private void logBKExceptionInClose(BKException e) {
    if (e instanceof BKException.BKLedgerClosedException) {
      // The ledger is already closed.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Trying to close ledger: " + getLedgerId() + " but it is already closed.");
      }
      return;
    }
    StringBuilder sb = new StringBuilder("BKException detected when close ledger with id: ");
    sb.append(ledgerHandle.getId())
      .append(" logPath: ")
      .append(logName)
      .append("\n");
    if (e instanceof BKException.BKNoSuchLedgerExistsException) {
      // The ledger is removed by other instance (master?) Ignore.
      sb.append("Underlying ledger is already removed, why this happen? ledgerId: ")
        .append(ledgerHandle.getId())
        .append(" logPath: ")
        .append(logName);
    } else if (e instanceof BKException.BKLedgerRecoveryException ||
               e instanceof BKException.BKLedgerFencedException) {
      sb.append("Ledger is already closed or unwritable.");
    }
    LOG.warn(sb.toString(), e);
  }

  public String getLogName() {
    return logName;
  }

  @Override
  public String toString() {
    return getLogName();
  }

  public long getLedgerId() {
    return this.ledgerHandle.getId();
  }

  private void checkException() throws IOException {
    int code = bkCode.get();
    if (code != BKException.Code.OK) {
      BKException bke = BKException.create(code);
      LOG.error("Got throwable when write ledger id: " + getLedgerId() + " logName: "
        + logName, bke);
      throw new IOException(BKException.create(code));
    }
  }

  @Override
  public void init(Configuration conf, String logName) throws URISyntaxException, IOException {
    try {
      this.conf = conf;
      this.logSystem = LedgerLogSystem.getInstance(conf);
      this.logName = logName;
      ledgerHandle = logSystem.createLog(logName);
      if (!logSystem.tryLockPath(logName)) {
        throw new IllegalStateException("The new created log: " + logName +
          " is locked by others.");
      }

      metadata = logSystem.getLogMetadata(logName);
      compressor = conf.getBoolean(BKConstants.LEDGER_COMPRESSED,
        BKConstants.DEFAULT_LEDGER_COMPRESSED) ? new Lz4Compressor() : new DisableCompressor();

      // Watch the log node in ZK, we mark us closed as soon as the node is removed.
      logSystem.watchLogPath(logName, new LogWatcher());
    } catch (Exception e) {
      throw e instanceof IOException ? (IOException) e : new IOException(e);
    }
  }

  long getNextEntryId() {
    return txId.getAndIncrement();
  }

  private void appendBytes(long entryId, byte[] data, long sequenceIdToUpdate) throws IOException {
    ledgerHandle.asyncAddEntry(entryId, data, (returnCode, handle, entryID, contextObj) -> {
      if (returnCode != BKException.Code.OK) {
        // Failed.
        BKException bke = BKException.create(returnCode);
        LOG.info("Async append failed due to: " + bke);
        if (bke instanceof BKException.BKLedgerFencedException) {
          // Someone is reading this ledger wal splitting is started. Block following writes.
          try {
            rollLedgerIfNeeded(handle.getId());
            // Succeed rolling ledger and append the data into the new ledger.
            executor.execute(() -> {
              try {
                appendBytes(entryId, data, sequenceIdToUpdate);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
          } catch (Exception e) {
            LOG.info("Met exception when roll ledger for log: " + logName
              + " this is serious may cause data lost.", e);
            // Failed roll ledger, release the path lock.
            logSystem.unlockPath(logName);
          }
        }
      } else {
        // Success
        if (highestSyncedSequenceId != null) {
          highestSyncedSequenceId.updateAndGet(curMax -> Math.max(sequenceIdToUpdate, curMax));
        }
      }
    }, null);
    writtenSize.addAndGet(data.length);
  }

  private void rollLedgerIfNeeded(long currentLedgerId) throws IOException {
    if (currentLedgerId == ledgerHandle.getId()) {
      // We should roll.
      synchronized (rollLedgerObj) {
        // Double check.
        if (currentLedgerId == ledgerHandle.getId()) {
          try {
            LedgerHandle backupHandle = logSystem.createNewLedger();
            backupHandle.getWriteFlags().clear();
            backupHandle.getWriteFlags().add(WriteFlag.DEFERRED_SYNC);
            metadata.addLedger(backupHandle.getId());
            logSystem.updateLogMetadata(logName, metadata);
            if (!ledgerHandle.isClosed()) {
              ledgerHandle.closeAsync();
            }

            ledgerHandle = backupHandle;
            LOG.info("The underlying ledger: " + ledgerHandle.getId() + " of log: " + logName
              + " is closed, creating new ledger: " + ledgerHandle.getId());
          } catch (Exception e) {
            throw new IOException(e);
          }
        }
      }
    }
  }

  private class LogWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
      try {
        if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
          // We are renamed by master to do splitting.
          writerClosed.compareAndSet(false, true);
        } else if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
          metadata = logSystem.getLogMetadata(logName);
          if (metadata.isClosed()) {
            // Once the log is closed, there will be no change on the metadata.
            // No need to register us once more.
            LOG.info("Log " + logName + " is closed by someone...");
            if (!writerClosed.get()) {
              writerClosed.compareAndSet(false, true);
              // Master is closing us to do splitting.
              close();
            }
            writerClosed.compareAndSet(false, true);
          } else {
            // Re-watch the meta data.
            logSystem.watchLogPath(logName, this);
          }
        }
      } catch (IOException | InterruptedException | KeeperException e) {
        throw new IllegalStateException(e);
      }
    }
  }
}
