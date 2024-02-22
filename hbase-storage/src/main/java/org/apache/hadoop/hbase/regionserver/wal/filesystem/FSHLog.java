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
package org.apache.hadoop.hbase.regionserver.wal.filesystem;

import static org.apache.hadoop.hbase.wal.WALUtils.WAL_FILE_NAME_DELIMITER;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.regionserver.wal.AbstractLog;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALPrettyPrinter;
import org.apache.hadoop.hbase.wal.WALUtils;
import org.apache.hadoop.hbase.wal.Writer;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.util.StringUtils;
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
   * FSDataOutputStream associated with the current SequenceFile.writer
   */
  private FSDataOutputStream hdfs_out;

  // All about log rolling if not enough replicas outstanding.

  // Minimum tolerable replicas, if the actual value is lower than it, rollWriter will be triggered
  private final int minTolerableReplication;

  private final int lowReplicationRollLimit;

  // Last time to check low replication on hlog's pipeline
  private volatile long lastTimeCheckLowReplication = EnvironmentEdgeManager.currentTime();

  private long hdfsBlockSize;

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
    super(conf, listeners, prefix, suffix);
    this.fs = fs;
    this.fullPathLogDir = new Path(rootDir, logDir);
    this.fullPathArchiveDir = new Path(rootDir, archiveDir);
    init();

    if (!fs.exists(fullPathLogDir) && !fs.mkdirs(fullPathLogDir)) {
      throw new IOException("Unable to mkdir " + fullPathLogDir);
    }

    if (!fs.exists(this.fullPathArchiveDir)) {
      if (!fs.mkdirs(this.fullPathArchiveDir)) {
        throw new IOException("Unable to mkdir " + this.fullPathArchiveDir);
      }
    }

    // Now that it exists, set the storage policy for the entire directory of wal files related to
    // this FSHLog instance
    FSUtils.setStoragePolicy(fs, conf, this.fullPathLogDir, HConstants.WAL_STORAGE_POLICY,
      HConstants.DEFAULT_WAL_STORAGE_POLICY);
    this.ourLogs = new LogNameFilter(this.prefixLogStr, logNameSuffix);

    if (failIfWALExists) {
      final FileStatus[] walFiles = FSUtils.listStatus(fs, fullPathLogDir,
        path -> ourLogs.accept(path.toString()));
      if (null != walFiles && 0 != walFiles.length) {
        throw new IOException("Target WAL already exists within directory " + fullPathLogDir);
      }
    }

    boolean maxLogsDefined = conf.get("hbase.regionserver.maxlogs") != null;
    if(maxLogsDefined){
      LOG.warn("'hbase.regionserver.maxlogs' was deprecated.");
    }

    this.minTolerableReplication = conf.getInt("hbase.regionserver.hlog.tolerable.lowreplication",
        FSUtils.getDefaultReplication(fs, this.fullPathLogDir));
    this.lowReplicationRollLimit =
      conf.getInt("hbase.regionserver.hlog.lowreplication.rolllimit", 5);

    LOG.info("WAL configuration: blocksize=" + StringUtils.byteDesc(hdfsBlockSize) +
      ", rollsize=" + StringUtils.byteDesc(this.logrollsize) +
      ", prefix=" + this.logNamePrefix + ", suffix=" + logNameSuffix + ", logDir=" +
      this.fullPathLogDir + ", archiveDir=" + this.fullPathArchiveDir);

    // rollWriter sets this.hdfs_out if it can.
    rollWriter();
    initDisruptor();
  }

  /**
   * Get the backing files associated with this WAL.
   * @return may be null if there are no files.
   */
  public FileStatus[] getFiles() throws IOException {
    return FSUtils.listStatus(fs, fullPathLogDir, path -> ourLogs.accept(path.toString()));
  }

  @Override
  protected void init() {
    // Get size to roll log at. Roll at 95% of HDFS block size so we avoid crossing HDFS blocks
    // (it costs a little x'ing bocks)
    try {
      hdfsBlockSize = this.conf.getLong("hbase.regionserver.hlog.blocksize",
        FSUtils.getDefaultBlockSize(this.fs, this.fullPathLogDir));
      logrollsize = (long)(hdfsBlockSize * conf.getFloat("hbase.regionserver.logroll.multiplier",
        0.95f));
      this.prefixLogStr =
        new Path(fullPathLogDir, logNamePrefix + WAL_FILE_NAME_DELIMITER).toString();
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to get hdfs block size. \n", e);
    }
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
  public OutputStream getOutputStream() {
    FSDataOutputStream fsdos = this.hdfs_out;
    if (fsdos == null) {
      return null;
    }
    return fsdos.getWrappedStream();
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
  protected CompletableFuture<Void> asyncCloseWriter(Writer writer, String oldPath, String newPath,
      Writer nextWriter) {
    this.hdfs_out = nextWriter instanceof ProtobufLogWriter ?
      ((ProtobufLogWriter) nextWriter).getStream() : null;
    return super.asyncCloseWriter(writer, oldPath, newPath, nextWriter);
  }

  @Override
  public Writer createWriterInstance(final String newWriterName) throws IOException {
    if (newWriterName == null) {
      throw new IOException("Cannot create writer with null name.");
    }
    Writer writer = WALUtils.createWriter(conf, fs, new Path(newWriterName), false);
    if (! (writer instanceof ProtobufLogWriter)) {
      throw new IllegalArgumentIOException("FSHlog must create " +
        ProtobufLogWriter.class.getName() + " but get: " + writer.getClass().getName());
    }
    return writer;
  }

  @Override
  protected void archiveLogUnities(List<String> logsToArchive) throws IOException {
    if (logsToArchive != null) {
      for (String logPath : logsToArchive) {
        Path p = new Path(logPath);
        this.totalLogSize.addAndGet(-this.fs.getFileStatus(p).getLen());
        archiveLogFile(p);
        this.byWalRegionSequenceIds.remove(p.toString());
      }
    }
  }

  private void archiveLogFile(final Path p) throws IOException {
    Path newPath = WALUtils.getWALArchivePath(this.fullPathArchiveDir, p);
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
   * @param logNum to use
   * @return Path
   */
  public Path computeFilename(final long logNum) {
    return new Path(fullPathLogDir, computeLogName(logNum));
  }

  /**
   * This is a convenience method that computes a new filename with a given
   * using the current WAL file-number
   * @return Path
   */
  public Path getCurrentFileName() {
    return computeFilename(this.logNum.get());
  }

  /**
   * @return current file number (timestamp)
   */
  public long getLogNum() {
    return logNum.get();
  }

  @Override
  public String toString() {
    return "FSHLog " + logNamePrefix + ":" + logNameSuffix + "(num " + logNum + ")";
  }

  @Override
  public void close() throws IOException {
    shutdown();
    final FileStatus[] files = getFiles();
    if (null != files && 0 != files.length) {
      for (FileStatus file : files) {
        Path p = WALUtils.getWALArchivePath(this.fullPathArchiveDir, file.getPath());
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
  protected String getLogFullName() {
    return FSUtils.getPath(fullPathLogDir);
  }

  @Override
  protected boolean checkExists(String logName) throws IOException {
    if (logName == null) {
      return false;
    }
    return fs.exists(new Path(logName));
  }

  @Override
  protected void nextWriterInit(Writer nextWriter) {
    if (nextWriter instanceof ProtobufLogWriter) {
      // If a ProtobufLogWriter, go ahead and try and sync to force setup of pipeline.
      // If this fails, we just keep going.... it is an optimization, not the end of the world.
      preemptiveSync((ProtobufLogWriter) nextWriter);
    }
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

  @Override
  protected void postSync(final long timeInNanos, final int handlerSyncs) {
    if (timeInNanos > this.slowSyncNs) {
      String msg =
          new StringBuilder().append("Slow sync cost: ")
              .append(timeInNanos / 1000000).append(" ms, current pipeline: ")
              .append(Arrays.toString(getPipeLine())).toString();
      LOG.info(msg);
    }
    if (!listeners.isEmpty()) {
      for (WALActionsListener listener : listeners) {
        listener.postSync(timeInNanos, handlerSyncs);
      }
    }
  }

  /**
   * This method gets the datanode replication count for the current WAL.
   *
   * If the pipeline isn't started yet or is empty, you will get the default
   * replication factor.  Therefore, if this function returns 0, it means you
   * are not properly running with the HDFS-826 patch.
   */
  @VisibleForTesting
  public int getLogReplication() {
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

  public static final long FIXED_OVERHEAD = ClassSize.align(
    ClassSize.OBJECT + (5 * ClassSize.REFERENCE) +
      ClassSize.ATOMIC_INTEGER + Bytes.SIZEOF_INT + (3 * Bytes.SIZEOF_LONG));

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
  public DatanodeInfo[] getPipeLine() {
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
