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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.lang.management.MemoryUsage;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.io.util.HeapMemorySizeUtil;
import org.apache.hadoop.hbase.mvcc.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.Entry;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALPrettyPrinter;
import org.apache.hadoop.hbase.wal.WALUtils;
import org.apache.hadoop.hbase.wal.Writer;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.util.StringUtils;
import org.apache.htrace.NullScope;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Implementation of {@link WAL} to go against {@link FileSystem}; i.e. keep WALs in HDFS.
 * Only one WAL is ever being written at a time.  When a WAL hits a configured maximum size,
 * it is rolled.  This is done internal to the implementation.
 *
 * <p>As data is flushed from the MemStore to other on-disk structures (files sorted by
 * key, hfiles), a WAL becomes obsolete. We can let go of all the log edits/entries for a given
 * HRegion-sequence id.  A bunch of work in the below is done keeping account of these region
 * sequence ids -- what is flushed out to hfiles, and what is yet in WAL and in memory only.
 *
 * <p>It is only practical to delete entire files. Thus, we delete an entire on-disk file
 * <code>F</code> when all of the edits in <code>F</code> have a log-sequence-id that's older
 * (smaller) than the most-recent flush.
 *
 * <p>To read an WAL, call {@link WALFactory#createReader(org.apache.hadoop.fs.FileSystem,
 * org.apache.hadoop.fs.Path)}.
 *
 * <h2>Failure Semantic</h2>
 * If an exception on append or sync, roll the WAL because the current WAL is now a lame duck;
 * any more appends or syncs will fail also with the same original exception. If we have made
 * successful appends to the WAL and we then are unable to sync them, our current semantic is to
 * return error to the client that the appends failed but also to abort the current context,
 * usually the hosting server. We need to replay the WALs. TODO: Change this semantic. A roll of
 * WAL may be sufficient as long as we have flagged client that the append failed. TODO:
 * replication may pick up these last edits though they have been marked as failed append (Need to
 * keep our own file lengths, not rely on HDFS).
 */
@InterfaceAudience.Private
public class FSHLog extends AbstractLog {
  // IMPLEMENTATION NOTES:
  //
  // At the core is a ring buffer.  Our ring buffer is the LMAX Disruptor.  It tries to
  // minimize synchronizations and volatile writes when multiple contending threads as is the case
  // here appending and syncing on a single WAL.  The Disruptor is configured to handle multiple
  // producers but it has one consumer only (the producers in HBase are IPC Handlers calling append
  // and then sync).  The single consumer/writer pulls the appends and syncs off the ring buffer.
  // When a handler calls sync, it is given back a future. The producer 'blocks' on the future so
  // it does not return until the sync completes.  The future is passed over the ring buffer from
  // the producer/handler to the consumer thread where it does its best to batch up the producer
  // syncs so one WAL sync actually spans multiple producer sync invocations.  How well the
  // batching works depends on the write rate; i.e. we tend to batch more in times of
  // high writes/syncs.
  //
  // Calls to append now also wait until the append has been done on the consumer side of the
  // disruptor.  We used to not wait but it makes the implemenation easier to grok if we have
  // the region edit/sequence id after the append returns.
  //
  // TODO: Handlers need to coordinate appending AND syncing.  Can we have the threads contend
  // once only?  Probably hard given syncs take way longer than an append.
  //
  // The consumer threads pass the syncs off to multiple syncing threads in a round robin fashion
  // to ensure we keep up back-to-back FS sync calls (FS sync calls are the long poll writing the
  // WAL).  The consumer thread passes the futures to the sync threads for it to complete
  // the futures when done.
  //
  // The 'sequence' in the below is the sequence of the append/sync on the ringbuffer.  It
  // acts as a sort-of transaction id.  It is always incrementing.
  //
  // The RingBufferEventHandler class hosts the ring buffer consuming code.  The threads that
  // do the actual FS sync are implementations of SyncRunner.  SafePointZigZagLatch is a
  // synchronization class used to halt the consumer at a safe point --  just after all outstanding
  // syncs and appends have completed -- so the log roller can swap the WAL out under it.

  private static final Log LOG = LogFactory.getLog(FSHLog.class);

  /**
   * file system instance
   */
  protected final FileSystem fs;

  /**
   * WAL directory, where all WAL files would be placed.
   */
  private final Path fullPathLogDir;

  /**
   * dir path where old logs are kept.
   */
  private final Path fullPathArchiveDir;

  /**
   * Matches just those wal files that belong to this wal instance.
   */
  private final PathFilter ourFiles;

  /**
   * Prefix of a WAL file, usually the region server name it is hosted on.
   */
  private final String logFilePrefix;

  /**
   * Suffix included on generated wal file names
   */
  private final String logFileSuffix;

  /**
   * Prefix used when checking for wal membership.
   */
  private final String prefixPathStr;

  /**
   * FSDataOutputStream associated with the current SequenceFile.writer
   */
  private FSDataOutputStream hdfs_out;

  // All about log rolling if not enough replicas outstanding.

  // Minimum tolerable replicas, if the actual value is lower than it, rollWriter will be triggered
  private final int minTolerableReplication;

  private final int lowReplicationRollLimit;

  // If > than this size, roll the log.
  private final long logrollsize;

  /*
   * If more than this many logs, force flush of oldest region to oldest edit
   * goes to disk.  If too many and we crash, then will take forever replaying.
   * Keep the number of logs tidy.
   */
  private final int maxLogs;

  /** Number of log close errors tolerated before we abort */
  private final int closeErrorsTolerated;

  private final AtomicInteger closeErrorCount = new AtomicInteger();

  private final ExecutorService closeExecutor = Executors.newCachedThreadPool(
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Close-WAL-Writer-%d").build());

  // Last time to check low replication on hlog's pipeline
  private volatile long lastTimeCheckLowReplication = EnvironmentEdgeManager.currentTime();

  /**
   * WAL Comparator; it compares the timestamp (log filenum), present in the log file name.
   * Throws an IllegalArgumentException if used to compare paths from different wals.
   */
  final Comparator<Path> LOG_NAME_COMPARATOR = new Comparator<Path>() {
    @Override
    public int compare(Path o1, Path o2) {
      long t1 = getFileNumFromFileName(o1);
      long t2 = getFileNumFromFileName(o2);
      if (t1 == t2) {
        return 0;
      }
      return (t1 > t2) ? 1 : -1;
    }
  };

  /**
   * Map of WAL log file to the latest sequence ids of all regions it has entries of.
   * The map is sorted by the log file creation timestamp (contained in the log file name).
   */
  private NavigableMap<Path, Map<byte[], Long>> byWalRegionSequenceIds =
    new ConcurrentSkipListMap<Path, Map<byte[], Long>>(LOG_NAME_COMPARATOR);

  /**
   * Constructor.
   *
   * @param fs filesystem handle
   * @param root path for stored and archived wals
   * @param logDir dir where wals are stored
   * @param conf configuration to use
   */
  public FSHLog(final FileSystem fs, final Path root, final String logDir, final Configuration conf)
      throws IOException {
    this(fs, root, logDir, HConstants.HREGION_OLDLOGDIR_NAME, conf, null, true, null, null);
  }

  /**
   * Create an edit log at the given <code>dir</code> location.
   *
   * You should never have to load an existing log. If there is a log at
   * startup, it should have already been processed and deleted by the time the
   * WAL object is started up.
   *
   * @param fs filesystem handle
   * @param rootDir path to where logs and oldlogs
   * @param logDir dir where wals are stored
   * @param archiveDir dir where wals are archived
   * @param conf configuration to use
   * @param listeners Listeners on WAL events. Listeners passed here will
   *        be registered before we do anything else; e.g. the
   *        Constructor {@link #rollWriter()}.
   * @param failIfWALExists If true IOException will be thrown if files related to this wal
   *        already exist.
   * @param prefix should always be hostname and port in distributed env and
   *        it will be URL encoded before being used.
   *        If prefix is null, "wal" will be used
   * @param suffix will be url encoded. null is treated as empty. non-empty must start with
   *        {@link WALUtils#WAL_FILE_NAME_DELIMITER}
   */
  public FSHLog(final FileSystem fs, final Path rootDir, final String logDir,
      final String archiveDir, final Configuration conf,
      final List<WALActionsListener> listeners,
      final boolean failIfWALExists, final String prefix, final String suffix)
    throws IOException {
    super(conf, listeners);
    this.fs = fs;
    this.fullPathLogDir = new Path(rootDir, logDir);
    this.fullPathArchiveDir = new Path(rootDir, archiveDir);

    if (!fs.exists(fullPathLogDir) && !fs.mkdirs(fullPathLogDir)) {
      throw new IOException("Unable to mkdir " + fullPathLogDir);
    }

    if (!fs.exists(this.fullPathArchiveDir)) {
      if (!fs.mkdirs(this.fullPathArchiveDir)) {
        throw new IOException("Unable to mkdir " + this.fullPathArchiveDir);
      }
    }

    // If prefix is null||empty then just name it wal
    this.logFilePrefix =
      prefix == null || prefix.isEmpty() ? "wal" : URLEncoder.encode(prefix, "UTF8");
    // we only correctly differentiate suffices when numeric ones start with '.'
    if (suffix != null && !(suffix.isEmpty()) && !(suffix.startsWith(WAL_FILE_NAME_DELIMITER))) {
      throw new IllegalArgumentException("WAL suffix must start with '" + WAL_FILE_NAME_DELIMITER +
          "' but instead was '" + suffix + "'");
    }
    // Now that it exists, set the storage policy for the entire directory of wal files related to
    // this FSHLog instance
    FSUtils.setStoragePolicy(fs, conf, this.fullPathLogDir, HConstants.WAL_STORAGE_POLICY,
      HConstants.DEFAULT_WAL_STORAGE_POLICY);
    this.logFileSuffix = (suffix == null) ? "" : URLEncoder.encode(suffix, "UTF8");
    this.prefixPathStr = new Path(fullPathLogDir,
        logFilePrefix + WAL_FILE_NAME_DELIMITER).toString();

    this.ourFiles = new PathFilter() {
      @Override
      public boolean accept(final Path fileName) {
        // The path should start with dir/<prefix> and end with our suffix
        final String fileNameString = fileName.toString();
        if (!fileNameString.startsWith(prefixPathStr)) {
          return false;
        }
        if (logFileSuffix.isEmpty()) {
          // in the case of the null suffix, we need to ensure the filename ends with a timestamp.
          return org.apache.commons.lang.StringUtils.isNumeric(
              fileNameString.substring(prefixPathStr.length()));
        } else if (!fileNameString.endsWith(logFileSuffix)) {
          return false;
        }
        return true;
      }
    };

    if (failIfWALExists) {
      final FileStatus[] walFiles = FSUtils.listStatus(fs, fullPathLogDir, ourFiles);
      if (null != walFiles && 0 != walFiles.length) {
        throw new IOException("Target WAL already exists within directory " + fullPathLogDir);
      }
    }

    // Get size to roll log at. Roll at 95% of HDFS block size so we avoid crossing HDFS blocks
    // (it costs a little x'ing bocks)
    final long blocksize = this.conf.getLong("hbase.regionserver.hlog.blocksize",
        FSUtils.getDefaultBlockSize(this.fs, this.fullPathLogDir));
    this.logrollsize =
      (long)(blocksize * conf.getFloat("hbase.regionserver.logroll.multiplier", 0.95f));

    float memstoreRatio = conf.getFloat(HeapMemorySizeUtil.MEMSTORE_SIZE_KEY,
      conf.getFloat(HeapMemorySizeUtil.MEMSTORE_SIZE_OLD_KEY,
        HeapMemorySizeUtil.DEFAULT_MEMSTORE_SIZE));
    boolean maxLogsDefined = conf.get("hbase.regionserver.maxlogs") != null;
    if(maxLogsDefined){
      LOG.warn("'hbase.regionserver.maxlogs' was deprecated.");
    }
    this.maxLogs = conf.getInt("hbase.regionserver.maxlogs",
        Math.max(32, calculateMaxLogFiles(memstoreRatio, logrollsize)));
    this.minTolerableReplication = conf.getInt("hbase.regionserver.hlog.tolerable.lowreplication",
        FSUtils.getDefaultReplication(fs, this.fullPathLogDir));
    this.lowReplicationRollLimit =
      conf.getInt("hbase.regionserver.hlog.lowreplication.rolllimit", 5);
    this.closeErrorsTolerated = conf.getInt("hbase.regionserver.logroll.errors.tolerated", 0);

    LOG.info("WAL configuration: blocksize=" + StringUtils.byteDesc(blocksize) +
      ", rollsize=" + StringUtils.byteDesc(this.logrollsize) +
      ", prefix=" + this.logFilePrefix + ", suffix=" + logFileSuffix + ", logDir=" +
      this.fullPathLogDir + ", archiveDir=" + this.fullPathArchiveDir);

    // rollWriter sets this.hdfs_out if it can.
    rollWriter();
  }

  private int calculateMaxLogFiles(float memstoreSizeRatio, long logRollSize) {
    long max = -1L;
    final MemoryUsage usage = HeapMemorySizeUtil.safeGetHeapMemoryUsage();
    if (usage != null) {
      max = usage.getMax();
    }
    int maxLogs = Math.round(max * memstoreSizeRatio * 2 / logRollSize);
    return maxLogs;
  }

  /**
   * Get the backing files associated with this WAL.
   * @return may be null if there are no files.
   */
  protected FileStatus[] getFiles() throws IOException {
    return FSUtils.listStatus(fs, fullPathLogDir, ourFiles);
  }

  /**
   * Currently, we need to expose the writer's OutputStream to tests so that they can manipulate
   * the default behavior (such as setting the maxRecoveryErrorCount value for example (see
   * {@link TestWALReplay#testReplayEditsWrittenIntoWAL()}). This is done using reflection on the
   * underlying HDFS OutputStream.
   * NOTE: This could be removed once Hadoop1 support is removed.
   * @return null if underlying stream is not ready.
   */
  @VisibleForTesting
  OutputStream getOutputStream() {
    FSDataOutputStream fsdos = this.hdfs_out;
    if (fsdos == null) {
      return null;
    }
    return fsdos.getWrappedStream();
  }

  @Override
  public byte [][] rollWriter() throws FailedLogCloseException, IOException {
    return rollWriter(false);
  }

  /**
   * retrieve the next path to use for writing.
   * Increments the internal filenum.
   */
  private Path getNewPath() throws IOException {
    this.filenum.set(System.currentTimeMillis());
    Path newPath = getCurrentFileName();
    while (fs.exists(newPath)) {
      this.filenum.incrementAndGet();
      newPath = getCurrentFileName();
    }
    return newPath;
  }

  Path getOldPath() {
    long currentFilenum = this.filenum.get();
    Path oldPath = null;
    if (currentFilenum > 0) {
      // ComputeFilename  will take care of meta wal filename
      oldPath = computeFilename(currentFilenum);
    } // I presume if currentFilenum is <= 0, this is first file and null for oldPath if fine?
    return oldPath;
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

  /**
   * Tell listeners about post log roll.
   */
  private void tellListenersAboutPostLogRoll(final Path oldPath, final Path newPath)
    throws IOException {
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.postLogRoll(oldPath, newPath);
      }
    }

    coprocessorHost.postWALRoll(oldPath, newPath);
  }

  /**
   * Run a sync after opening to set up the pipeline.
   */
  private void preemptiveSync(final ProtobufLogWriter nextWriter) {
    long startTimeNanos = System.nanoTime();
    try {
      nextWriter.sync();
      postSync(System.nanoTime() - startTimeNanos, 0);
    } catch (IOException e) {
      // optimization failed, no need to abort here.
      LOG.warn("pre-sync failed but an optimization so keep going", e);
    }
  }

  @Override
  public byte [][] rollWriter(boolean force) throws FailedLogCloseException, IOException {
    rollWriterLock.lock();
    try {
      // Return if nothing to flush.
      if (!force && (this.writer != null && this.numEntries.get() <= 0)) {
        return null;
      }
      byte [][] regionsToFlush = null;
      if (this.closed) {
        LOG.debug("WAL closed. Skipping rolling of writer");
        return regionsToFlush;
      }
      if (!closeBarrier.beginOp()) {
        LOG.debug("WAL closing. Skipping rolling of writer");
        return regionsToFlush;
      }
      TraceScope scope = Trace.startSpan("FSHLog.rollWriter");
      try {
        Path oldPath = getOldPath();
        Path newPath = getNewPath();
        // Any exception from here on is catastrophic, non-recoverable so we currently abort.
        Writer nextWriter = this.createWriterInstance(newPath);
        FSDataOutputStream nextHdfsOut = null;
        if (nextWriter instanceof ProtobufLogWriter) {
          nextHdfsOut = ((ProtobufLogWriter)nextWriter).getStream();
          // If a ProtobufLogWriter, go ahead and try and sync to force setup of pipeline.
          // If this fails, we just keep going.... it is an optimization, not the end of the world.
          preemptiveSync((ProtobufLogWriter)nextWriter);
        }
        tellListenersAboutPreLogRoll(oldPath, newPath);
        // NewPath could be equal to oldPath if replaceWriter fails.
        newPath = replaceWriter(oldPath, newPath, nextWriter, nextHdfsOut);
        tellListenersAboutPostLogRoll(oldPath, newPath);
        // Can we delete any of the old log files?
        if (getNumRolledLogFiles() > 0) {
          cleanOldLogs();
          regionsToFlush = findRegionsToForceFlush();
        }
      } finally {
        closeBarrier.endOp();
        assert scope == NullScope.INSTANCE || !scope.isDetached();
        scope.close();
      }
      return regionsToFlush;
    } finally {
      rollWriterLock.unlock();
    }
  }

  /**
   * This method allows subclasses to inject different writers without having to
   * extend other methods like rollWriter().
   *
   * @return Writer instance
   */
  protected Writer createWriterInstance(final Path path) throws IOException {
    return WALUtils.createWriter(conf, fs, path, false);
  }

  /**
   * Archive old logs. A WAL is eligible for archiving if all its WALEdits have been flushed.
   */
  private void cleanOldLogs() throws IOException {
    List<Path> logsToArchive = null;
    // For each log file, look at its Map of regions to highest sequence id; if all sequence ids
    // are older than what is currently in memory, the WAL can be GC'd.
    for (Map.Entry<Path, Map<byte[], Long>> e : this.byWalRegionSequenceIds.entrySet()) {
      Path log = e.getKey();
      Map<byte[], Long> sequenceNums = e.getValue();
      if (this.sequenceIdAccounting.areAllLower(sequenceNums)) {
        if (logsToArchive == null) {
          logsToArchive = new ArrayList<Path>();
        }
        logsToArchive.add(log);
        if (LOG.isTraceEnabled()) {
          LOG.trace("WAL file ready for archiving " + log);
        }
      }
    }
    if (logsToArchive != null) {
      for (Path p : logsToArchive) {
        this.totalLogSize.addAndGet(-this.fs.getFileStatus(p).getLen());
        archiveLogFile(p);
        this.byWalRegionSequenceIds.remove(p);
      }
    }
  }

  /**
   * If the number of un-archived WAL files is greater than maximum allowed, check the first
   * (oldest) WAL file, and returns those regions which should be flushed so that it can
   * be archived.
   * @return regions (encodedRegionNames) to flush in order to archive oldest WAL file.
   */
  byte[][] findRegionsToForceFlush() throws IOException {
    byte [][] regions = null;
    int logCount = getNumRolledLogFiles();
    if (logCount > this.maxLogs && logCount > 0) {
      Map.Entry<Path, Map<byte[], Long>> firstWALEntry =
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

  /**
   * Cleans up current writer closing it and then puts in place the passed in
   * <code>nextWriter</code>.
   *
   * In the case of creating a new WAL, oldPath will be null.
   *
   * In the case of rolling over from one file to the next, none of the params will be null.
   *
   * In the case of closing out this FSHLog with no further use newPath, nextWriter, and
   * nextHdfsOut will be null.
   *
   * @param oldPath may be null
   * @param newPath may be null
   * @param nextWriter may be null
   * @param nextHdfsOut may be null
   * @return the passed in <code>newPath</code>
   * @throws IOException if there is a problem flushing or closing the underlying FS
   */
  Path replaceWriter(final Path oldPath, final Path newPath, Writer nextWriter,
      final FSDataOutputStream nextHdfsOut) throws IOException {
    // Ask the ring buffer writer to pause at a safe point.  Once we do this, the writer
    // thread will eventually pause. An error hereafter needs to release the writer thread
    // regardless -- hence the finally block below.  Note, this method is called from the FSHLog
    // constructor BEFORE the ring buffer is set running so it is null on first time through
    // here; allow for that.
    SyncFuture syncFuture = null;
    SafePointZigZagLatch zigzagLatch = null;
    long sequence = -1L;
    if (this.ringBufferEventHandler != null) {
      // Get sequence first to avoid dead lock when ring buffer is full
      // Considering below sequence
      // 1. replaceWriter is called and zigzagLatch is initialized
      // 2. ringBufferEventHandler#onEvent is called and arrives at #attainSafePoint(long) then wait
      // on safePointReleasedLatch
      // 3. Since ring buffer is full, if we get sequence when publish sync, the replaceWriter
      // thread will wait for the ring buffer to be consumed, but the only consumer is waiting
      // replaceWriter thread to release safePointReleasedLatch, which causes a deadlock
      sequence = getSequenceOnRingBuffer();
      zigzagLatch = this.ringBufferEventHandler.attainSafePoint();
    }
    afterCreatingZigZagLatch();
    TraceScope scope = Trace.startSpan("FSHFile.replaceWriter");
    CompletableFuture<Void> closeFuture = null;
    try {
      // Wait on the safe point to be achieved.  Send in a sync in case nothing has hit the
      // ring buffer between the above notification of writer that we want it to go to
      // 'safe point' and then here where we are waiting on it to attain safe point.  Use
      // 'sendSync' instead of 'sync' because we do not want this thread to block waiting on it
      // to come back.  Cleanup this syncFuture down below after we are ready to run again.
      try {
        if (zigzagLatch != null) {
          // use assert to make sure no change breaks the logic that
          // sequence and zigzagLatch will be set together
          assert sequence > 0L : "Failed to get sequence from ring buffer";
          Trace.addTimelineAnnotation("awaiting safepoint");
          syncFuture = zigzagLatch.waitSafePoint(publishSyncOnRingBuffer(sequence));
        }
      } catch (FailedSyncBeforeLogCloseException e) {
        // If unflushed/unsynced entries on close, it is reason to abort.
        if (isUnflushedEntries()) {
          throw e;
        }
        LOG.warn("Failed sync-before-close but no outstanding appends; closing WAL: " +
          e.getMessage());
      }

      // It is at the safe point.  Swap out writer from under the blocked writer thread.
      Writer localWriter = this.writer;
      closeFuture = asyncCloseWriter(localWriter, oldPath);
      this.writer = nextWriter;
      this.hdfs_out = nextHdfsOut;
      int oldNumEntries = this.numEntries.get();
      this.numEntries.set(0);
      final String newPathString = (null == newPath ? null : FSUtils.getPath(newPath));
      if (oldPath != null) {
        this.byWalRegionSequenceIds.put(oldPath, this.sequenceIdAccounting.resetHighest());
        long oldFileLen = this.fs.getFileStatus(oldPath).getLen();
        this.totalLogSize.addAndGet(oldFileLen);
        LOG.info("Rolled WAL " + FSUtils.getPath(oldPath) + " with entries=" + oldNumEntries +
          ", filesize=" + StringUtils.byteDesc(oldFileLen) + "; new WAL " +
          newPathString);
      } else {
        LOG.info("New WAL " + newPathString);
      }
    } catch (InterruptedException ie) {
      // Perpetuate the interrupt
      Thread.currentThread().interrupt();
    } catch (IOException e) {
      long count = getUnflushedEntriesCount();
      LOG.error("Failed close of WAL writer " + oldPath + ", unflushedEntries=" + count, e);
      throw new FailedLogCloseException(oldPath + ", unflushedEntries=" + count, e);
    } finally {
      try {
        // Let the writer thread go regardless, whether error or not.
        if (zigzagLatch != null) {
          zigzagLatch.releaseSafePoint();
          // syncFuture will be null if we failed our wait on safe point above. Otherwise, if
          // latch was obtained successfully, the sync we threw in either trigger the latch or it
          // got stamped with an exception because the WAL was damaged and we could not sync. Now
          // the write pipeline has been opened up again by releasing the safe point, process the
          // syncFuture we got above. This is probably a noop but it may be stale exception from
          // when old WAL was in place. Catch it if so.
          if (syncFuture != null) {
            try {
              blockOnSync(syncFuture);
            } catch (IOException ioe) {
              if (LOG.isTraceEnabled()) {
                LOG.trace("Stale sync exception", ioe);
              }
            }
          }
        }
      } finally {
        if (closeFuture != null) {
          try {
            closeFuture.join();
          } catch (CompletionException e) {
            if (e.getCause() instanceof IOException) {
              throw (IOException) e.getCause();
            }
            throw e;
          }
        }
        scope.close();
      }
    }
    return newPath;
  }

  private CompletableFuture<Void> asyncCloseWriter(Writer writer, Path oldPath) {
    CompletableFuture<Void> future = new CompletableFuture<>();

    closeExecutor.execute(() -> {
      try {
        if (writer != null) {
          Trace.addTimelineAnnotation("closing writer");
          writer.close();
          Trace.addTimelineAnnotation("writer closed");
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

  long getUnflushedEntriesCount() {
    long highestSynced = this.highestSyncedSequence.get();
    return highestSynced > this.highestUnsyncedSequence?
      0: this.highestUnsyncedSequence - highestSynced;
  }

  boolean isUnflushedEntries() {
    return getUnflushedEntriesCount() > 0;
  }

  /*
   * only public so WALSplitter can use.
   * @return archived location of a WAL file with the given path p
   */
  public static Path getWALArchivePath(Path archiveDir, Path p) {
    return new Path(archiveDir, p.getName());
  }

  private void archiveLogFile(final Path p) throws IOException {
    Path newPath = getWALArchivePath(this.fullPathArchiveDir, p);
    // Tell our listeners that a log is going to be archived.
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.preLogArchive(p, newPath);
      }
    }
    LOG.info("Archiving " + p + " to " + newPath);
    if (!FSUtils.renameAndSetModifyTime(this.fs, p, newPath)) {
      throw new IOException("Unable to rename " + p + " to " + newPath);
    }
    // Tell our listeners that a log has been archived.
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.postLogArchive(p, newPath);
      }
    }
  }

  /**
   * This is a convenience method that computes a new filename with a given
   * file-number.
   * @param filenum to use
   * @return Path
   */
  protected Path computeFilename(final long filenum) {
    if (filenum < 0) {
      throw new RuntimeException("WAL file number can't be < 0");
    }
    String child = logFilePrefix + WAL_FILE_NAME_DELIMITER + filenum + logFileSuffix;
    return new Path(fullPathLogDir, child);
  }

  /**
   * This is a convenience method that computes a new filename with a given
   * using the current WAL file-number
   * @return Path
   */
  public Path getCurrentFileName() {
    return computeFilename(this.filenum.get());
  }

  /**
   * @return current file number (timestamp)
   */
  public long getFilenum() {
    return filenum.get();
  }

  @Override
  public String toString() {
    return "FSHLog " + logFilePrefix + ":" + logFileSuffix + "(num " + filenum + ")";
  }

/**
 * A log file has a creation timestamp (in ms) in its file name ({@link #filenum}.
 * This helper method returns the creation timestamp from a given log file.
 * It extracts the timestamp assuming the filename is created with the
 * {@link #computeFilename(long filenum)} method.
 * @return timestamp, as in the log file name.
 */
  protected long getFileNumFromFileName(Path fileName) {
    if (fileName == null) {
      throw new IllegalArgumentException("file name can't be null");
    }
    if (!ourFiles.accept(fileName)) {
      throw new IllegalArgumentException("The log file " + fileName +
        " doesn't belong to this WAL. (" + toString() + ")");
    }
    final String fileNameString = fileName.toString();
    String chompedPath = fileNameString.substring(prefixPathStr.length(),
        (fileNameString.length() - logFileSuffix.length()));
    return Long.parseLong(chompedPath);
  }

  @Override
  public void close() throws IOException {
    shutdown();
    final FileStatus[] files = getFiles();
    if (null != files && 0 != files.length) {
      for (FileStatus file : files) {
        Path p = getWALArchivePath(this.fullPathArchiveDir, file.getPath());
        // Tell our listeners that a log is going to be archived.
        if (!this.listeners.isEmpty()) {
          for (WALActionsListener i : this.listeners) {
            i.preLogArchive(file.getPath(), p);
          }
        }

        if (!FSUtils.renameAndSetModifyTime(fs, file.getPath(), p)) {
          throw new IOException("Unable to rename " + file.getPath() + " to " + p);
        }
        // Tell our listeners that a log was archived.
        if (!this.listeners.isEmpty()) {
          for (WALActionsListener i : this.listeners) {
            i.postLogArchive(file.getPath(), p);
          }
        }
      }
      LOG.debug("Moved " + files.length + " WAL file(s) to " +
        FSUtils.getPath(this.fullPathArchiveDir));
    }
    LOG.info("Closed WAL: " + toString());
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

      // Shutdown the disruptor.  Will stop after all entries have been processed.  Make sure we
      // have stopped incoming appends before calling this else it will not shutdown.  We are
      // conservative below waiting a long time and if not elapsed, then halting.
      if (this.disruptor != null) {
        long timeoutms = conf.getLong("hbase.wal.disruptor.shutdown.timeout.ms", 60000);
        try {
          this.disruptor.shutdown(timeoutms, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
          LOG.warn("Timed out bringing down disruptor after " + timeoutms + "ms; forcing halt " +
            "(It is a problem if this is NOT an ABORT! -- DATALOSS!!!!)");
          this.disruptor.halt();
          this.disruptor.shutdown();
        }
      }
      // With disruptor down, this is safe to let go.
      if (this.appendExecutor !=  null) {
        this.appendExecutor.shutdown();
      }

      if (syncFutureCache != null) {
        syncFutureCache.clear();
      }

      // Tell our listeners that the log is closing
      if (!this.listeners.isEmpty()) {
        for (WALActionsListener i : this.listeners) {
          i.logCloseRequested();
        }
      }
      this.closed = true;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Closing WAL writer in " + FSUtils.getPath(fullPathLogDir));
      }
      if (this.writer != null) {
        this.writer.close();
        this.writer = null;
      }
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NP_NULL_ON_SOME_PATH_EXCEPTION",
      justification="Will never be null")
  @Override
  public long append(final HTableDescriptor htd, final HRegionInfo hri, final WALKey key,
      final WALEdit edits, final boolean inMemstore) throws IOException {
    if (this.closed) {
      throw new IOException("Cannot append; log is closed");
    }
    // Make a trace scope for the append.  It is closed on other side of the ring buffer by the
    // single consuming thread.  Don't have to worry about it.
    TraceScope scope = Trace.startSpan("FSHLog.append");
    final MutableLong txidHolder = new MutableLong();
    final RingBuffer<RingBufferTruck> ringBuffer = disruptor.getRingBuffer();
    MultiVersionConcurrencyControl.WriteEntry we = key.getMvcc().begin(new Runnable() {
      @Override public void run() {
        txidHolder.setValue(ringBuffer.next());
      }
    });
    long txid = txidHolder.longValue();
    try {
      FSWALEntry entry = new FSWALEntry(txid, key, edits, htd, hri, inMemstore);
      entry.stampRegionSequenceId(we);
      ringBuffer.get(txid).loadPayload(entry, scope.detach());
    } finally {
      ringBuffer.publish(txid);
    }
    return txid;
  }

  /**
   * Schedule a log roll if needed.
   */
  @Override
  public void checkLogRoll() {
    // Will return immediately if we are in the middle of a WAL log roll currently.
    if (!rollWriterLock.tryLock()) {
      return;
    }
    boolean lowReplication;
    try {
      lowReplication = checkLowReplication();
    } finally {
      rollWriterLock.unlock();
    }
    try {
      if (lowReplication || (writer != null && writer.getLength() > logrollsize)) {
        requestLogRoll(lowReplication);
      }
    } catch (IOException e) {
      LOG.warn("Writer.getLength() failed; continuing", e);
    }
  }

  /*
   * @return true if number of replicas for the WAL is lower than threshold
   */
  private boolean checkLowReplication() {
    boolean logRollNeeded = false;
    this.lastTimeCheckLowReplication = EnvironmentEdgeManager.currentTime();
    // if the number of replicas in HDFS has fallen below the configured
    // value, then roll logs.
    try {
      int numCurrentReplicas = getLogReplication();
      if (numCurrentReplicas != 0 && numCurrentReplicas < this.minTolerableReplication) {
        if (this.lowReplicationRollEnabled) {
          if (this.consecutiveLogRolls.get() < this.lowReplicationRollLimit) {
            LOG.warn("HDFS pipeline error detected. " + "Found "
                + numCurrentReplicas + " replicas but expecting no less than "
                + this.minTolerableReplication + " replicas. "
                + " Requesting close of WAL. current pipeline: "
                + Arrays.toString(getPipeLine()));
            logRollNeeded = true;
            // If rollWriter is requested, increase consecutiveLogRolls. Once it
            // is larger than lowReplicationRollLimit, disable the
            // LowReplication-Roller
            this.consecutiveLogRolls.getAndIncrement();
          } else {
            LOG.warn("Too many consecutive RollWriter requests, it's a sign of "
                + "the total number of live datanodes is lower than the tolerable replicas.");
            this.consecutiveLogRolls.set(0);
            this.lowReplicationRollEnabled = false;
          }
        }
      } else if (numCurrentReplicas >= this.minTolerableReplication) {
        if (!this.lowReplicationRollEnabled) {
          // The new writer's log replicas is always the default value.
          // So we should not enable LowReplication-Roller. If numEntries
          // is lower than or equals 1, we consider it as a new writer.
          if (this.numEntries.get() <= 1) {
            return logRollNeeded;
          }
          // Once the live datanode number and the replicas return to normal,
          // enable the LowReplication-Roller.
          this.lowReplicationRollEnabled = true;
          LOG.info("LowReplication-Roller was enabled.");
        }
      }
    } catch (Exception e) {
      LOG.warn("DFSOutputStream.getNumCurrentReplicas failed because of " + e +
        ", continuing...");
    }
    return logRollNeeded;
  }

  private SyncFuture publishSyncOnRingBuffer(long sequence) {
    return publishSyncOnRingBuffer(sequence, null);
  }

  private long getSequenceOnRingBuffer() {
    return this.disruptor.getRingBuffer().next();
  }

  @InterfaceAudience.Private
  public SyncFuture publishSyncOnRingBuffer(Span span) {
    long sequence = this.disruptor.getRingBuffer().next();
    return publishSyncOnRingBuffer(sequence, span);
  }

  private SyncFuture publishSyncOnRingBuffer(long sequence, Span span) {
    SyncFuture syncFuture = getSyncFuture(sequence, span);
    try {
      RingBufferTruck truck = this.disruptor.getRingBuffer().get(sequence);
      truck.loadPayload(syncFuture);
    } finally {
      this.disruptor.getRingBuffer().publish(sequence);
    }
    return syncFuture;
  }

  // Sync all known transactions
  private Span publishSyncThenBlockOnCompletion(Span span) throws IOException {
    return blockOnSync(publishSyncOnRingBuffer(span));
  }

  private Span blockOnSync(final SyncFuture syncFuture) throws IOException {
    // Now we have published the ringbuffer, halt the current thread until we get an answer back.
    try {
      syncFuture.get(walSyncTimeout);
      return syncFuture.getSpan();
    } catch (TimeoutIOException tioe) {
      throw tioe;
    } catch (InterruptedException ie) {
      LOG.warn("Interrupted", ie);
      throw convertInterruptedExceptionToIOException(ie);
    } catch (ExecutionException e) {
      throw ensureIOException(e.getCause());
    }
  }

  private IOException convertInterruptedExceptionToIOException(final InterruptedException ie) {
    Thread.currentThread().interrupt();
    IOException ioe = new InterruptedIOException();
    ioe.initCause(ie);
    return ioe;
  }

  private SyncFuture getSyncFuture(final long sequence, Span span) {
    return syncFutureCache.getIfPresentOrNew().reset(sequence);
  }

  @Override
  protected void postSync(final long timeInNanos, final int handlerSyncs) {
    if (timeInNanos > this.slowSyncNs) {
      String msg =
          new StringBuilder().append("Slow sync cost: ")
              .append(timeInNanos / 1000000).append(" ms, current pipeline: ")
              .append(Arrays.toString(getPipeLine())).toString();
      Trace.addTimelineAnnotation(msg);
      LOG.info(msg);
    }
    if (!listeners.isEmpty()) {
      for (WALActionsListener listener : listeners) {
        listener.postSync(timeInNanos, handlerSyncs);
      }
    }
  }

  @Override
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


  /**
   * This method gets the datanode replication count for the current WAL.
   *
   * If the pipeline isn't started yet or is empty, you will get the default
   * replication factor.  Therefore, if this function returns 0, it means you
   * are not properly running with the HDFS-826 patch.
   */
  @VisibleForTesting
  int getLogReplication() {
    try {
      //in standalone mode, it will return 0
      if (this.hdfs_out instanceof HdfsDataOutputStream) {
        return ((HdfsDataOutputStream) this.hdfs_out).getCurrentBlockReplication();
      }
    } catch (IOException e) {
      LOG.info("", e);
    }
    return 0;
  }

  @Override
  public void sync() throws IOException {
    TraceScope scope = Trace.startSpan("FSHLog.sync");
    try {
      scope = Trace.continueSpan(publishSyncThenBlockOnCompletion(scope.detach()));
    } finally {
      assert scope == NullScope.INSTANCE || !scope.isDetached();
      scope.close();
    }
  }

  @Override
  public void sync(long txid) throws IOException {
    if (this.highestSyncedSequence.get() >= txid){
      // Already sync'd.
      return;
    }
    TraceScope scope = Trace.startSpan("FSHLog.sync");
    try {
      scope = Trace.continueSpan(publishSyncThenBlockOnCompletion(scope.detach()));
    } finally {
      assert scope == NullScope.INSTANCE || !scope.isDetached();
      scope.close();
    }
  }

  // public only until class moves to o.a.h.h.wal
  @Override
  public void requestLogRoll() {
    requestLogRoll(false);
  }

  private void requestLogRoll(boolean tooFewReplicas) {
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i: this.listeners) {
        i.logRollRequested(tooFewReplicas);
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

  // public only until class moves to o.a.h.h.wal
  /** @return the size of log files in use */
  public long getLogFileSize() {
    return this.totalLogSize.get();
  }

  @Override
  public Long startCacheFlush(final byte[] encodedRegionName, Set<byte[]> families) {
    if (!closeBarrier.beginOp()) {
      LOG.info("Flush not started for " + Bytes.toString(encodedRegionName) + "; server closing.");
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

  @VisibleForTesting
  boolean isLowReplicationRollEnabled() {
    return lowReplicationRollEnabled;
  }

  public static final long FIXED_OVERHEAD = ClassSize.align(
    ClassSize.OBJECT + (5 * ClassSize.REFERENCE) +
      ClassSize.ATOMIC_INTEGER + Bytes.SIZEOF_INT + (3 * Bytes.SIZEOF_LONG));

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

  private static IOException ensureIOException(final Throwable t) {
    return (t instanceof IOException)? (IOException)t: new IOException(t);
  }

  private static void usage() {
    System.err.println("Usage: FSHLog <ARGS>");
    System.err.println("Arguments:");
    System.err.println(" --dump  Dump textual representation of passed one or more files");
    System.err.println("         For example: " +
      "FSHLog --dump hdfs://example.com:9000/hbase/.logs/MACHINE/LOGFILE");
  }

  /**
   * Pass one or more log file names and it will either dump out a text version
   * on <code>stdout</code> or split the specified log files.
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      usage();
      System.exit(-1);
    }
    // either dump using the WALPrettyPrinter or split, depending on args
    if (args[0].compareTo("--dump") == 0) {
      WALPrettyPrinter.run(Arrays.copyOfRange(args, 1, args.length));
    } else if (args[0].compareTo("--perf") == 0) {
      LOG.fatal("Please use the WALPerformanceEvaluation tool instead. i.e.:");
      LOG.fatal("\thbase org.apache.hadoop.hbase.wal.WALPerformanceEvaluation --iterations " +
          args[1]);
      System.exit(-1);
    } else {
      usage();
      System.exit(-1);
    }
  }

  /**
   * This method gets the pipeline for the current WAL.
   */
  @VisibleForTesting
  DatanodeInfo[] getPipeLine() {
    if (this.hdfs_out != null) {
      if (this.hdfs_out.getWrappedStream() instanceof DFSOutputStream) {
        return ((DFSOutputStream) this.hdfs_out.getWrappedStream()).getPipeline();
      }
    }
    return new DatanodeInfo[0];
  }

  /**
   *
   * @return last time on checking low replication
   */
  public long getLastTimeCheckLowReplication() {
    return this.lastTimeCheckLowReplication;
  }
}
