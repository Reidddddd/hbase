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

import static org.apache.hadoop.hbase.wal.WALUtils.WAL_FILE_NAME_DELIMITER;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.management.MemoryUsage;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.util.HeapMemorySizeUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.DrainBarrier;
import org.apache.hadoop.hbase.util.LogNameFilter;
import org.apache.hadoop.hbase.wal.Entry;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.Writer;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public abstract class WALBase implements WAL {
  private static final Log LOG = LogFactory.getLog(WALBase.class);
  private static final int DEFAULT_SLOW_SYNC_TIME_MS = 100; // in ms
  private static final int DEFAULT_WAL_SYNC_TIMEOUT_MS = 5 * 60 * 1000; // in ms, 5min

  /**
   * The highest known outstanding unsync'd WALEdit sequence number where sequence number is the
   * ring buffer sequence.  Maintained by the ring buffer consumer.
   */
  protected volatile long highestUnsyncedSequence = -1;

  /**
   * Updated to the ring buffer sequence of the last successful sync call.  This can be less than
   * {@link #highestUnsyncedSequence} for case where we have an append where a sync has not yet
   * come in for it.  Maintained by the syncing threads.
   */
  protected final AtomicLong highestSyncedSequence = new AtomicLong(0);

  protected final WALCoprocessorHost coprocessorHost;

  /**
   * conf object
   */
  protected final Configuration conf;

  /** Listeners that are called on WAL events. */
  protected final List<WALActionsListener> listeners = new CopyOnWriteArrayList<>();

  // If > than this size, roll the log.
  protected long logrollsize;

  protected final int slowSyncNs;

  protected final long walSyncTimeout;

  /**
   * Class that does accounting of sequenceids in WAL subsystem. Holds oldest outstanding
   * sequence id as yet not flushed as well as the most recent edit sequence id appended to the
   * WAL. Has facility for answering questions such as "Is it safe to GC a WAL?".
   */
  protected SequenceIdAccounting sequenceIdAccounting = new SequenceIdAccounting();

  /**
   * Current log file.
   */
  protected volatile Writer writer;

  /**
   * This lock makes sure only one log roll runs at a time. Should not be taken while any other
   * lock is held. We don't just use synchronized because that results in bogus and tedious
   * findbugs warning when it thinks synchronized controls writer thread safety.  It is held when
   * we are actually rolling the log.  It is checked when we are looking to see if we should roll
   * the log or not.
   */
  protected final ReentrantLock rollWriterLock = new ReentrantLock(true);

  protected volatile boolean closed = false;
  protected final AtomicBoolean shutdown = new AtomicBoolean(false);

  // The timestamp (in ms) when the log entry (hfile or a distributed log) was created.
  protected final AtomicLong logNum = new AtomicLong(-1);

  // Number of transactions in the current Wal.
  protected final AtomicInteger numEntries = new AtomicInteger(0);

  /**
   * The total size of wal
   */
  protected AtomicLong totalLogSize = new AtomicLong(0);

  protected final AtomicInteger closeErrorCount = new AtomicInteger();

  protected final float memstoreRatio;

  /*
   * If more than this many logs, force flush of oldest region to oldest edit
   * goes to disk.  If too many and we crash, then will take forever replaying.
   * Keep the number of logs tidy.
   */
  protected final int maxLogs;

  /**
   * Prefix of a WAL file, usually the region server name it is hosted on.
   */
  protected final String logNamePrefix;

  /**
   * Suffix included on generated wal file names
   */
  protected final String logNameSuffix;

  /** Number of log close errors tolerated before we abort */
  protected final int closeErrorsTolerated;

  /**
   * Prefix used when checking for wal membership.
   */
  protected String prefixLogStr;

  /** The barrier used to ensure that close() waits for all log rolls and flushes to finish. */
  protected final DrainBarrier closeBarrier = new DrainBarrier();

  protected final ExecutorService closeExecutor = Executors.newCachedThreadPool(
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Close-WAL-Writer-%d").build());

  /**
   * WAL Comparator; it compares the timestamp (log logNum), present in the log file name.
   * Throws an IllegalArgumentException if used to compare paths from different wals.
   */
  protected final Comparator<String> LOG_NAME_COMPARATOR = new Comparator<String>() {
    @Override
    public int compare(String s1, String s2) {
      long t1 = getLogNumFromName(s1);
      long t2 = getLogNumFromName(s2);
      if (t1 == t2) {
        return 0;
      }
      return (t1 > t2) ? 1 : -1;
    }
  };

  protected NavigableMap<String, Map<byte[], Long>> byWalRegionSequenceIds =
    new ConcurrentSkipListMap<String, Map<byte[], Long>>(LOG_NAME_COMPARATOR);

  /**
   * Matches just those wal files that belong to this wal instance.
   */
  protected LogNameFilter logNameFilter;

  @Override
  public void registerWALActionsListener(final WALActionsListener listener) {
    this.listeners.add(listener);
  }

  @Override
  public boolean unregisterWALActionsListener(final WALActionsListener listener) {
    return this.listeners.remove(listener);
  }

  @Override
  public WALCoprocessorHost getCoprocessorHost() {
    return coprocessorHost;
  }

  public WALBase(Configuration conf, List<WALActionsListener> listeners, String prefix,
    String suffix) throws UnsupportedEncodingException {
    this.conf = conf;
    this.coprocessorHost = new WALCoprocessorHost(this, conf);

    // If prefix is null||empty then just name it wal
    this.logNamePrefix =
      prefix == null || prefix.isEmpty() ? "wal" : URLEncoder.encode(prefix, "UTF8");
    // we only correctly differentiate suffices when numeric ones start with '.'
    if (suffix != null && !(suffix.isEmpty()) && !(suffix.startsWith(WAL_FILE_NAME_DELIMITER))) {
      throw new IllegalArgumentException("WAL suffix must start with '" + WAL_FILE_NAME_DELIMITER +
        "' but instead was '" + suffix + "'");
    }
    this.closeErrorsTolerated = conf.getInt("hbase.regionserver.logroll.errors.tolerated", 0);
    this.logNameSuffix = (suffix == null) ? "" : URLEncoder.encode(suffix, "UTF8");

    // Register listeners.
    // TODO: Should this exist anymore?  We have CPs?
    if (listeners != null) {
      for (WALActionsListener i: listeners) {
        registerWALActionsListener(i);
      }
    }

    this.slowSyncNs =
      1000000 * conf.getInt("hbase.regionserver.hlog.slowsync.ms",
        DEFAULT_SLOW_SYNC_TIME_MS);
    this.walSyncTimeout = conf.getLong("hbase.regionserver.hlog.sync.timeout",
      DEFAULT_WAL_SYNC_TIMEOUT_MS);

    this.memstoreRatio = conf.getFloat(HeapMemorySizeUtil.MEMSTORE_SIZE_KEY,
      conf.getFloat(HeapMemorySizeUtil.MEMSTORE_SIZE_OLD_KEY,
        HeapMemorySizeUtil.DEFAULT_MEMSTORE_SIZE));
    this.maxLogs = conf.getInt("hbase.regionserver.maxlogs",
      Math.max(32, calculateMaxLogFiles(memstoreRatio, logrollsize)));
  }

  private int calculateMaxLogFiles(float memstoreSizeRatio, long logRollSize) {
    long max = -1L;
    final MemoryUsage usage = HeapMemorySizeUtil.safeGetHeapMemoryUsage();
    if (usage != null) {
      max = usage.getMax();
    }
    return Math.round(max * memstoreSizeRatio * 2 / logRollSize);
  }

  /**
   * A log file has a creation timestamp (in ms) in its file name ({@link #logNum}.
   * This helper method returns the creation timestamp from a given log file.
   * It extracts the timestamp assuming the filename is created with the
   * {@link org.apache.hadoop.hbase.regionserver.wal.filesystem.FSHLog#computeFilename(long logNum)}
   * method.
   * @return timestamp, as in the log file name.
   */
  protected long getLogNumFromName(String logName) {
    if (logName == null || logName.length() == 0) {
      throw new IllegalArgumentException("Log name can't be null");
    }
    if (!logNameFilter.accept(logName)) {
      throw new IllegalArgumentException("The log file " + logName +
        " doesn't belong to this WAL. (" + toString() + ")");
    }
    try {
      String chompedPath = logName.substring(prefixLogStr.length(),
        logName.length() - logNameSuffix.length());
      return Long.parseLong(chompedPath);
    } catch (StringIndexOutOfBoundsException e) {
      LOG.info(e);
    }
    return 0;
  }

  protected String getOldLogName() {
    long currentLogNum = this.logNum.get();
    String oldLogName = null;
    if (currentLogNum > 0) {
      // ComputeFilename  will take care of meta wal filename
      oldLogName = computeLogName(currentLogNum);
    } // I presume if currentLogNum is <= 0, this is first file and null for oldPath if fine?
    return oldLogName;
  }

  /**
   * This is a convenience method that computes a new filename with a given
   * file-number.
   * @param logNum to use
   * @return Path
   */
  protected String computeLogName(final long logNum) {
    if (logNum < 0) {
      throw new RuntimeException("WAL file number can't be < 0");
    }
    return prefixLogStr + logNum + logNameSuffix;
  }

  protected long postAppend(final Entry e, final long elapsedTime) throws IOException {
    long len = 0;
    if (!listeners.isEmpty()) {
      for (Cell cell : e.getEdit().getCells()) {
        len += CellUtil.estimatedSerializedSizeOf(cell);
      }
      for (WALActionsListener listener : listeners) {
        listener.postAppend(len, elapsedTime, e.getKey(), e.getEdit());
      }
    }
    return len;
  }

  protected void postSync(final long timeInNanos, final int handlerSyncs) {
    if (timeInNanos > this.slowSyncNs) {
      String msg = "Slow sync cost: " + timeInNanos / 1000000 + " ms.";
      LOG.info(msg);
    }
    if (!listeners.isEmpty()) {
      for (WALActionsListener listener : listeners) {
        listener.postSync(timeInNanos, handlerSyncs);
      }
    }
  }

  /**
   * Tell listeners about post log roll.
   */
  protected void tellListenersAboutPostLogRoll(final Path oldPath, final Path newPath)
    throws IOException {
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.postLogRoll(oldPath, newPath);
      }
    }
    coprocessorHost.postWALRoll(oldPath, newPath);
  }

  /**
   * retrieve the next path to use for writing.
   * Increments the internal logNum.
   */
  protected String getNewLogName() throws IOException {
    this.logNum.set(System.currentTimeMillis());
    String newLogName = getOldLogName();
    while (checkExists(newLogName)) {
      this.logNum.incrementAndGet();
      newLogName = getOldLogName();
    }
    return newLogName;
  }

  @Override
  public byte[][] rollWriter() throws IOException {
    return rollWriter(false);
  }

  @Override
  public byte [][] rollWriter(boolean force) throws IOException {
    rollWriterLock.lock();
    try {
      // Return if nothing to flush.
      if (!force && (this.writer != null && this.numEntries.get() <= 0)) {
        return null;
      }
      byte[][] regionsToFlush = null;
      if (this.closed) {
        LOG.debug("WAL closed. Skipping rolling of writer");
        return regionsToFlush;
      }
      if (!closeBarrier.beginOp()) {
        LOG.debug("WAL closing. Skipping rolling of writer");
        return regionsToFlush;
      }
      try {
        regionsToFlush = doRolling();
      } finally {
        closeBarrier.endOp();
      }
      return regionsToFlush;
    } finally {
      rollWriterLock.unlock();
    }
  }

  // TODO: we currently must use Path to support coprocessors. Need to change it in the future.
  protected byte[][] doRolling() throws IOException {
    byte [][] regionsToFlush = null;
    try {
      String oldLogName = getOldLogName();
      String newLogName = getNewLogName();
      // Any exception from here on is catastrophic, non-recoverable so we currently abort.
      Writer nextWriter = this.createWriterInstance(newLogName);
      nextWriterInit(nextWriter);

      Path newPath = newLogName == null ? null : new Path(newLogName);
      Path oldPath = oldLogName == null ? null : new Path(oldLogName);

      tellListenersAboutPreLogRoll(oldPath, newPath);
      // NewPath could be equal to oldPath if replaceWriter fails.
      newLogName = replaceWriter(oldLogName, newLogName, nextWriter);
      newPath = newLogName == null ? null : new Path(newLogName);
      tellListenersAboutPostLogRoll(oldPath, newPath);
      regionsToFlush = calculateRegionsToFlushForRolling();
    } catch (IOException e) {
      LOG.warn("Met exception to roll log writer. \n", e);
      throw e;
    }
    return regionsToFlush;
  }

  protected byte[][] calculateRegionsToFlushForRolling() throws IOException {
    byte[][] regionsToFlush = null;
    // Can we delete any of the old log files?
    if (getNumRolledLogFiles() > 0) {
      cleanOldLogs();
      regionsToFlush = findRegionsToForceFlush();
    }
    return regionsToFlush;
  }

  /**
   * Tell listeners about pre log roll.
   */
  private void tellListenersAboutPreLogRoll(final Path oldPath, final Path newPath)
    throws IOException {
    coprocessorHost.preWALRoll(oldPath, newPath);

    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.preLogRoll(oldPath, newPath);
      }
    }
  }

  // public only until class moves to o.a.h.h.wal
  /** @return the number of rolled log files */
  public int getNumRolledLogFiles() {
    return byWalRegionSequenceIds.size();
  }

  // public only until class moves to o.a.h.h.wal
  /** @return the number of log files in use */
  public int getNumLogFiles() {
    // +1 for current use log
    return getNumRolledLogFiles() + 1;
  }

  /**
   * Archive old logs. A WAL is eligible for archiving if all its WALEdits have been flushed.
   */
  protected void cleanOldLogs() throws IOException {
    List<String> logsToArchive = null;
    // For each log file, look at its Map of regions to highest sequence id; if all sequence ids
    // are older than what is currently in memory, the WAL can be GC'd.
    for (Map.Entry<String, Map<byte[], Long>> e : this.byWalRegionSequenceIds.entrySet()) {
      String log = e.getKey();
      Map<byte[], Long> sequenceNums = e.getValue();
      if (this.sequenceIdAccounting.areAllLower(sequenceNums)) {
        if (logsToArchive == null) {
          logsToArchive = new ArrayList<String>();
        }
        logsToArchive.add(log);
        if (LOG.isTraceEnabled()) {
          LOG.trace("WAL unity ready for archiving " + log);
        }
      }
    }

    archiveLogUnities(logsToArchive);
  }

  /**
   * If the number of un-archived WAL files is greater than maximum allowed, check the first
   * (oldest) WAL file, and returns those regions which should be flushed so that it can
   * be archived.
   * @return regions (encodedRegionNames) to flush in order to archive oldest WAL file.
   */
  protected byte[][] findRegionsToForceFlush() throws IOException {
    byte [][] regions = null;
    int logCount = getNumRolledLogFiles();
    if (logCount > this.maxLogs && logCount > 0) {
      Map.Entry<String, Map<byte[], Long>> firstWALEntry =
        this.byWalRegionSequenceIds.firstEntry();
      regions = this.sequenceIdAccounting.findLower(firstWALEntry.getValue());
    }
    if (regions != null) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < regions.length; i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(Bytes.toStringBinary(regions[i]));
      }
      LOG.info("Too many WALs; count=" + logCount + ", max=" + this.maxLogs +
        "; forcing flush of " + regions.length + " regions(s): " + sb.toString());
    }
    return regions;
  }

  @Override
  public Long startCacheFlush(final byte[] encodedRegionName, Set<byte[]> families) {
    if (!closeBarrier.beginOp()) {
      return null;
    }
    return this.sequenceIdAccounting.startCacheFlush(encodedRegionName, families);
  }

  @Override
  public void completeCacheFlush(final byte[] encodedRegionName) {
    this.sequenceIdAccounting.completeCacheFlush(encodedRegionName);
    closeBarrier.endOp();
  }

  @Override
  public void abortCacheFlush(byte[] encodedRegionName) {
    this.sequenceIdAccounting.abortCacheFlush(encodedRegionName);
    closeBarrier.endOp();
  }

  @Override
  public long getEarliestMemstoreSeqNum(byte[] encodedRegionName) {
    // Used by tests. Deprecated as too subtle for general usage.
    return this.sequenceIdAccounting.getLowestSequenceId(encodedRegionName);
  }

  @Override
  public long getEarliestMemstoreSeqNum(byte[] encodedRegionName, byte[] familyName) {
    // This method is used by tests and for figuring if we should flush or not because our
    // sequenceids are too old. It is also used reporting the master our oldest sequenceid for use
    // figuring what edits can be skipped during log recovery. getEarliestMemStoreSequenceId
    // from this.sequenceIdAccounting is looking first in flushingOldestStoreSequenceIds, the
    // currently flushing sequence ids, and if anything found there, it is returning these. This is
    // the right thing to do for the reporting oldest sequenceids to master; we won't skip edits if
    // we crash during the flush. For figuring what to flush, we might get requeued if our sequence
    // id is old even though we are currently flushing. This may mean we do too much flushing.
    return this.sequenceIdAccounting.getLowestSequenceId(encodedRegionName, familyName);
  }

  protected CompletableFuture<Void> asyncCloseWriter(Writer writer, String oldPath, String newPath,
    Writer nextWriter) {
    CompletableFuture<Void> future = new CompletableFuture<>();

    closeExecutor.execute(() -> {
      try {
        if (writer != null) {
          afterReplaceWriter(newPath, oldPath, writer.getLength());
          writer.close();
        }
        this.closeErrorCount.set(0);
        future.complete(null);
      } catch (IOException ioe) {
        int errors = closeErrorCount.incrementAndGet();
        if (!isUnflushedEntries() && (errors <= this.closeErrorsTolerated)) {
          LOG.warn("Riding over failed WAL close of " + oldPath + ", cause=\"" + ioe.getMessage()
            + "\", errors=" + errors
            + "; THIS FILE WAS NOT CLOSED BUT ALL EDITS SYNCED SO SHOULD BE OK");
          future.complete(null);
        } else {
          future.completeExceptionally(ioe);
        }
      }
    });

    return future;
  }

  protected boolean isUnflushedEntries() {
    return getUnflushedEntriesCount() > 0;
  }

  protected long getUnflushedEntriesCount() {
    long highestSynced = this.highestSyncedSequence.get();
    return highestSynced > this.highestUnsyncedSequence?
      0: this.highestUnsyncedSequence - highestSynced;
  }

  /**
   * @param sequence The sequence we ran the filesystem sync against.
   * @return Current highest synced sequence.
   */
  protected long updateHighestSyncedSequence(long sequence) {
    long currentHighestSyncedSequence;
    // Set the highestSyncedSequence IFF our current sequence id is the 'highest'.
    do {
      currentHighestSyncedSequence = highestSyncedSequence.get();
      if (currentHighestSyncedSequence >= sequence) {
        // Set the sync number to current highwater mark; might be able to let go more
        // queued sync futures
        sequence = currentHighestSyncedSequence;
        break;
      }
    } while (!highestSyncedSequence.compareAndSet(currentHighestSyncedSequence, sequence));
    return sequence;
  }

  // For subclasses to handle internal variables after writer rolled.
  protected void afterReplaceWriter(String newPathStr, String oldPathStr, long oldLogLength)
    throws IOException {
    int oldNumEntries = this.numEntries.get();
    this.numEntries.set(0);
    try {
      if (oldPathStr != null) {
        this.byWalRegionSequenceIds.put(oldPathStr, this.sequenceIdAccounting.resetHighest());
        this.totalLogSize.addAndGet(oldLogLength);
        LOG.info("Rolled WAL " + oldPathStr + " with entries=" + oldNumEntries + ", filesize="
          + StringUtils.byteDesc(oldLogLength) + "; new WAL " + newPathStr);
      } else {
        LOG.info("New WAL " + newPathStr);
      }
    } catch (Exception e) {
      // If we got exception, we failed to access distributed log. So that we revert the number of
      // entries here.
      this.numEntries.set(oldNumEntries);
      throw new IOException(e);
    }
  }

  public void requestLogRoll() {
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i: this.listeners) {
        // We do not care the number of replicas when we use DistributedLog to store WAL.
        i.logRollRequested(false);
      }
    }
  }

  protected void checkLogRoll() {
    try {
      if ((writer != null && writer.getLength() > logrollsize)) {
        requestLogRoll();
      }
    } catch (IOException e) {
      LOG.warn("Writer.getLength() failed; continuing", e);
    }
  }

  // Coprocessor hook.
  protected void coprocessorPreWALWrite(HRegionInfo hri, WALKey key, WALEdit edit)
    throws IOException {
    // Coprocessor hook.
    if (!coprocessorHost.preWALWrite(hri, key, edit)) {
      if (edit.isReplay()) {
        // Set replication scope null so that this won't be replicated
        key.setScopes(null);
      }
    }
  }

  protected void walListenerBeforeWrite(HTableDescriptor htd, WALKey key, WALEdit edit)
    throws IOException {
    if (!listeners.isEmpty()) {
      for (WALActionsListener i: listeners) {
        // TODO: Why does listener take a table description and CPs take a regioninfo?  Fix.
        i.visitLogEntryBeforeWrite(htd, key, edit);
      }
    }
  }

  // public only until class moves to o.a.h.h.wal
  /** @return the size of log files in use */
  public long getLogFileSize() {
    return this.totalLogSize.get();
  }

  protected abstract boolean checkExists(String logName) throws IOException;

  /**
   * This method allows subclasses to inject different writers without having to
   * extend other methods like rollWriter().
   *
   * @return Writer instance
   */
  protected abstract Writer createWriterInstance(final String newWriterName) throws IOException;

  // For subclasses to invoke in their own construction function.
  protected abstract void init();

  protected abstract String getLogFullName();

  // For subclass to do some initialisation when rolling writer.
  protected abstract void nextWriterInit(Writer nextWriter);

  protected abstract String replaceWriter(final String oldPathStr, final String newPathStr,
    Writer nextWriter) throws IOException;

  protected abstract void archiveLogUnities(List<String> logsToArchive) throws IOException;
}
