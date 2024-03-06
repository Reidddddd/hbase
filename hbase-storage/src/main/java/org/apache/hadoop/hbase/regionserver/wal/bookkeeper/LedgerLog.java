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

import static org.apache.hadoop.hbase.HConstants.HREGION_OLDLOGDIR_NAME;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.mvcc.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALBase;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.LedgerUtil;
import org.apache.hadoop.hbase.util.LogNameFilter;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALUtils;
import org.apache.hadoop.hbase.wal.Writer;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A WAL implementation based on pure ledger of BK.
 */
@InterfaceAudience.Private
public class LedgerLog extends WALBase {
  private static final Log LOG = LogFactory.getLog(LedgerLog.class);
  private static final String LEDGER_ROLL_SIZE =
    "hbase.regionserver.ledger.rollsize";
  private static final long DEFAULT_LEDGER_ROLL_SIZE = 134217728; // 128 MB

  private final ReentrantReadWriteLock appendLock = new ReentrantReadWriteLock();
  private final LedgerLogSystem logSystem;
  // The ZNode path of this server.
  private final String logPath;
  private final String logArchivePath;
  private final AtomicLong txId = new AtomicLong(0);
  private final AtomicLong highestUnsyncedTxId = new AtomicLong(0);

  public LedgerLog(Configuration conf, List<WALActionsListener> listeners, String prefix,
      String suffix, String serverName, String rootPath) throws IOException {
    super(conf, listeners, prefix, suffix);
    this.logSystem = LedgerLogSystem.getInstance(conf);
    this.logPath = LedgerUtil.getLedgerLogPath(rootPath, serverName);
    this.logArchivePath = ZKUtil.joinZNode(rootPath, HREGION_OLDLOGDIR_NAME);
    logrollsize = conf.getLong(LEDGER_ROLL_SIZE, DEFAULT_LEDGER_ROLL_SIZE);
    prefixLogStr = ZKUtil.joinZNode(logPath, serverName) + WALUtils.WAL_FILE_NAME_DELIMITER;
    logNameFilter = new LogNameFilter(prefixLogStr, logNameSuffix);
    // Create the first writer.
    rollWriter();
  }

  @Override
  protected boolean checkExists(String logName) throws IOException {
    return logSystem.logExists(logName);
  }

  @Override
  public Writer createWriterInstance(String newWriterName) throws IOException {
    try {
      Writer writer = WALUtils.createWriter(conf, null, new Path(newWriterName), false,
        LedgerLogWriter.class);
      if (writer instanceof LedgerLogWriter) {
        ((LedgerLogWriter) writer).setHighestSyncedSequenceId(highestSyncedSequence);
      }
      return writer;
    } catch (Exception e) {
      if (e instanceof IOException) {
        throw (IOException) e;
      } else {
        throw new IOException(e);
      }
    }
  }

  @Override
  protected void init() {

  }

  @Override
  public String getLogFullName() {
    return writer.toString();
  }

  @Override
  protected void nextWriterInit(Writer nextWriter) {
    // Currently nothing to init.
  }

  @Override
  protected String replaceWriter(String oldPathStr, String newPathStr, Writer nextWriter)
      throws IOException {
    CompletableFuture<Void> closeWriterFuture = null;
    try {
      appendLock.writeLock().lock();
      Writer localWriter = this.writer;
      this.writer = nextWriter;
      closeWriterFuture = asyncCloseWriter(localWriter, oldPathStr, newPathStr, nextWriter);

      return newPathStr;
    } finally {
      appendLock.writeLock().unlock();
      if (closeWriterFuture != null) {
        try {
          closeWriterFuture.join();
        } catch (CompletionException e) {
          Throwable t = e.getCause();
          throw t instanceof IOException ? (IOException) t : new IOException(t);
        }
      }
    }
  }

  @Override
  public void archiveLogUnities(List<String> logsToArchive) throws IOException {
    if (logsToArchive == null || logsToArchive.isEmpty()) {
      return;
    }
    for (String log : logsToArchive) {
      String newPath = ZKUtil.joinZNode(this.logArchivePath, ZKUtil.getNodeName(log));
      if (!this.listeners.isEmpty()) {
        for (WALActionsListener i : this.listeners) {
          i.preLogArchive(new Path(log), new Path(newPath));
        }
      }

      long logLength = logSystem.getLogDataSize(log);
      logSystem.closeLog(log);
      logSystem.unlockPath(log);
      logSystem.renamePath(log, newPath);
      this.totalLogSize.addAndGet(-logLength);
      this.byWalRegionSequenceIds.remove(log);

      if (!this.listeners.isEmpty()) {
        for (WALActionsListener i : this.listeners) {
          i.postLogArchive(new Path(log), new Path(newPath));
        }
      }
    }
  }

  @Override
  public void shutdown() throws IOException {
    if (shutdown.compareAndSet(false, true)) {
      try {
        // Prevent all further flushing and rolling.
        closeBarrier.stopAndDrainOps();
      } catch (InterruptedException e) {
        LOG.error("Exception while waiting for cache flushes and log rolls", e);
        Thread.currentThread().interrupt();
      }
    }

    // Tell our listeners that the log is closing
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.logCloseRequested();
      }
    }
    this.closed = true;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Closing WAL writer in " + getLogFullName());

    }
    if (this.writer != null) {
      this.writer.close();
      this.writer = null;
    }
  }

  @Override
  public void close() throws IOException {
    shutdown();
    try {
      // We only archive the logs under this server.
      List<String> logNames =
        logSystem.getLogUnderPath(Collections.singletonList(logPath), logNameFilter);
      int numOfLogs = 0;
      for (String logName : logNames) {
        String archivePath = ZKUtil.joinZNode(logArchivePath, ZKUtil.getNodeName(logName));
        // Tell our listeners that a log is going to be archived.
        if (!this.listeners.isEmpty()) {
          for (WALActionsListener i : this.listeners) {
            i.preLogArchive(new Path(logName), new Path(archivePath));
          }
        }
        this.logSystem.renamePath(logName, archivePath);
        numOfLogs += 1;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Renamed " + numOfLogs + " ledger logs to old WALs.");
      }
      LOG.info("Closed WAL: " + toString());
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public long append(HTableDescriptor htd, HRegionInfo info, WALKey key, WALEdit edits,
      boolean inMemstore) throws IOException {
    if (this.closed) {
      throw new IOException("Cannot append; log is closed");
    }
    long start = EnvironmentEdgeManager.currentTime();
    try {
      appendLock.readLock().lock();
      // Step 1. Prepare the mvcc and entry object.
      final MutableLong txIdHolder = new MutableLong();
      final MutableLong entryIdHolder = new MutableLong();
      // We need sync here. Otherwise, the mvcc write id may mismatch with txId.
      long regionSequenceId = -1;
      MultiVersionConcurrencyControl.WriteEntry we =
        key.getMvcc().begin(() -> {
          txIdHolder.setValue(txId.getAndIncrement());
            entryIdHolder.setValue(((LedgerLogWriter) writer).getNextEntryId());
          }
        );
      long txid = txIdHolder.longValue();
      long entryId = entryIdHolder.longValue();

      regionSequenceId = we.getWriteNumber();
      LedgerLogWALEntry walEntry = new LedgerLogWALEntry(txid, entryId, key, edits, htd, info,
        inMemstore, we);

      if (edits.isEmpty()) {
        // Do not need to wait outside.
        return -1;
      }

      // Step 2. Append prepared entry to writer and update metrics.
      // We only write entry with edits.
      coprocessorPreWALWrite(info, key, edits);
      walListenerBeforeWrite(htd, key, edits);

      try {
        writer.append(walEntry);
      } catch (Exception e) {
        LOG.warn("Request log roll for exception: ", e);
        requestLogRoll();
        if (e instanceof IOException) {
          throw (IOException) e;
        } else {
          throw new IOException(e);
        }
      }

      highestUnsyncedTxId.updateAndGet(curMax -> Math.max(walEntry.getSequence(), curMax));
      sequenceIdAccounting.update(key.getEncodedRegionName(), walEntry.getFamilyNames(),
        regionSequenceId, walEntry.isInMemstore());
      coprocessorHost.postWALWrite(walEntry.getHRegionInfo(), walEntry.getKey(),
        walEntry.getEdit());

      // Update metrics.
      postAppend(walEntry, EnvironmentEdgeManager.currentTime() - start);
      numEntries.incrementAndGet();

      return walEntry.getSequence();
    } finally {
      appendLock.readLock().unlock();
    }
  }

  @Override
  public void sync() throws IOException {
    long start = System.nanoTime();
    try {
      appendLock.readLock().lock();
      writer.sync();
      checkLogRoll();
    } catch (Exception e) {
      LOG.warn("Request log roll for exception: ", e);
      requestLogRoll();
    } finally {
      appendLock.readLock().unlock();
    }
    postSync(System.nanoTime() - start, 1);
  }

  @Override
  public void sync(long txid) throws IOException {
    if (highestSyncedSequence.get() >= txid){
      // Already sync'd.
      return;
    }
    sync();
  }

  @Override
  protected long getUnflushedEntriesCount() {
    long highestSynced = this.highestSyncedSequence.get();
    return highestSynced > this.highestUnsyncedTxId.get() ? 0 :
      this.highestUnsyncedTxId.get() - highestSynced;
  }
}
