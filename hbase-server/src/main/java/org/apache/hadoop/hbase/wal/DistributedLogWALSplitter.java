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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.TextFormat;
import dlshade.org.apache.distributedlog.api.DistributedLogManager;
import dlshade.org.apache.distributedlog.api.namespace.Namespace;
import dlshade.org.apache.distributedlog.exceptions.LogEmptyException;
import dlshade.org.apache.distributedlog.exceptions.LogNotFoundException;
import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoordinatedStateException;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableStateManager;
import org.apache.hadoop.hbase.master.SplitLogManager;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.regionserver.LastSequenceId;
import org.apache.hadoop.hbase.regionserver.wal.DistributedLogAccessor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A {@link AbstractWALSplitter} based on DistributedLog.
 */
@InterfaceAudience.Private
public class DistributedLogWALSplitter extends AbstractWALSplitter {
  private static final Log LOG = LogFactory.getLog(DistributedLogWALSplitter.class);
  private static final String LOG_NAME_ERROR_MSG =
    "passed in string is for something other than a log entity.";

  private final Namespace walNamespace;

  protected DistributedLogWALSplitter(Configuration conf, WALFactory walFactory,
      LastSequenceId sequenceIdChecker, CoordinatedStateManager csm,
      ZooKeeperProtos.SplitLogTask.RecoveryMode mode) throws IOException {
    super(conf, walFactory, sequenceIdChecker, csm, mode);
    try {
      walNamespace = DistributedLogAccessor.getInstance(conf).getNamespace();
    } catch (Exception e) {
      LOG.warn("Failed accessing distributedlog with exception: \n", e);
      throw new IOException(e);
    }
    initOutputSink();
  }

  @Override
  protected void initOutputSink() {
    if (csm != null && this.distributedLogReplay) {
      outputSink = new DistributedLogReplayOutputSink(controller, entryBuffers, numWriterThreads,
        conf, disablingOrDisabledTables, lastFlushedSequenceIds, csm, regionMaxSeqIdInStores,
        failedServerName, this);
    } else {
      if (this.distributedLogReplay) {
        LOG.info("ZooKeeperWatcher is passed in as NULL so disable distrubitedLogRepaly.");
      }
      this.distributedLogReplay = false;
      if(splitWriterCreationBounded){
        outputSink = new BoundedDistributedLogWriterCreationOutputSink(controller, entryBuffers,
          numWriterThreads, conf, regionMaxSeqIdInStores, this);
      }else {
        outputSink = new DistributedLogRecoveredEditsOutputSink(controller, entryBuffers,
          numWriterThreads, conf, regionMaxSeqIdInStores, this);
      }
    }
  }

  /**
   * Splits a WAL file into region's recovered-edits log.
   * This is the main entry point for distributed log splitting from SplitLogWorker.
   * <p>
   * If the log file has N regions then N recovered.edits logs will be produced.
   * <p>
   * @param cp coordination state manager
   * @return false if it is interrupted by the progress-able.
   */
  public static boolean splitLog(String logName, Configuration conf,
      CancelableProgressable reporter, LastSequenceId idChecker, CoordinatedStateManager cp,
      ZooKeeperProtos.SplitLogTask.RecoveryMode mode, final WALFactory factory) throws IOException {
    DistributedLogWALSplitter splitter
      = new DistributedLogWALSplitter(conf, factory, idChecker, cp, mode);
    return splitter.splitLog(logName, reporter);
  }

  /**
   * log splitting implementation, splits one log file.
   * @param logNameWithParent should be an actual log file.
   */
  @VisibleForTesting
  boolean splitLog(String logNameWithParent, CancelableProgressable reporter) throws IOException {
    Preconditions.checkState(status == null);
    Preconditions.checkArgument(walNamespace.logExists(logNameWithParent), LOG_NAME_ERROR_MSG);
    boolean isCorrupted = false, outputSinkStarted = false, progressFailed = false;
    boolean skipErrors = conf.getBoolean(SPLIT_SKIP_ERRORS, SPLIT_SKIP_ERRORS_DEFAULT);
    int interval = conf.getInt("hbase.splitlog.report.interval.loglines", 1024);
    int editsCount = 0, editsSkipped = 0;
    status = TaskMonitor.get().createStatus(
      "Splitting log file " + logNameWithParent + "into a temporary staging area.");
    Reader in = null;
    long logLength = -1;
    if (this.outputSink instanceof DistributedLogRecoveredEditsOutputSink) {
      ((DistributedLogRecoveredEditsOutputSink) this.outputSink)
        .setLogInSplitting(logNameWithParent);
    }
    try {
      DistributedLogManager dlm = walNamespace.openLog(logNameWithParent);
      WALUtils.checkEndOfStream(dlm);
      logLength = dlm.getLogRecordCount() < 1 ? -1 : dlm.getLastTxId();
      LOG.info("Splitting wal: " + logNameWithParent + ", length=" + logLength);
      LOG.info("DistributedLogReplay = " + this.distributedLogReplay);
      status.setStatus("Opening log");
      if (reporter != null && !reporter.progress()) {
        progressFailed = true;
        return false;
      }
      try {
        dlm.close();
        in = getReader(logNameWithParent, logLength, skipErrors, reporter);
      } catch (CorruptedLogFileException | LogEmptyException e) {
        LOG.warn("Could not get reader, corrupted log file " + logNameWithParent, e);
        markCorruptedLog(logNameWithParent, e);
        isCorrupted = true;
      }
      if (in == null) {
        LOG.warn("Nothing to split in log file " + logNameWithParent);
        return true;
      }
      if (csm != null) {
        try {
          TableStateManager tsm = csm.getTableStateManager();
          disablingOrDisabledTables = tsm.getTablesInStates(
            ZooKeeperProtos.Table.State.DISABLED, ZooKeeperProtos.Table.State.DISABLING);
        } catch (CoordinatedStateException e) {
          throw new IOException("Can't get disabling/disabled tables", e);
        }
      }
      int numOpenedFilesBeforeReporting = conf.getInt("hbase.splitlog.report.openedfiles", 3);
      int numOpenedFilesLastCheck = 0;
      outputSink.setReporter(reporter);
      outputSink.startWriterThreads();
      outputSinkStarted = true;
      Entry entry;
      Long lastFlushedSequenceId = -1L;
      Path logPath = new Path(logNameWithParent);
      ServerName serverName =
        WALUtils.getServerNameFromWALDirectoryName(logPath, logPath.depth() > 1);
      failedServerName = (serverName == null) ? "" : serverName.getServerName();
      while ((entry = getNextLogLine(in, logNameWithParent, skipErrors)) != null) {
        byte[] region = entry.getKey().getEncodedRegionName();
        String encodedRegionNameAsStr = Bytes.toString(region);
        lastFlushedSequenceId = lastFlushedSequenceIds.get(encodedRegionNameAsStr);
        if (lastFlushedSequenceId == null) {
          if (this.distributedLogReplay) {
            ClusterStatusProtos.RegionStoreSequenceIds ids =
              csm.getSplitLogWorkerCoordination().getRegionFlushedSequenceId(failedServerName,
                encodedRegionNameAsStr);
            if (ids != null) {
              lastFlushedSequenceId = ids.getLastFlushedSequenceId();
              logDebugInfo("DLR Last flushed sequenceId for " + encodedRegionNameAsStr + ": " +
                TextFormat.shortDebugString(ids));
            }
          } else if (sequenceIdChecker != null) {
            ClusterStatusProtos.RegionStoreSequenceIds ids =
              sequenceIdChecker.getLastSequenceId(region);
            Map<byte[], Long> maxSeqIdInStores = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
            for (ClusterStatusProtos.StoreSequenceId storeSeqId : ids.getStoreSequenceIdList()) {
              maxSeqIdInStores.put(storeSeqId.getFamilyName().toByteArray(),
                storeSeqId.getSequenceId());
            }
            regionMaxSeqIdInStores.put(encodedRegionNameAsStr, maxSeqIdInStores);
            lastFlushedSequenceId = ids.getLastFlushedSequenceId();
            logDebugInfo("DLS Last flushed sequenceId for " + encodedRegionNameAsStr + ": " +
              TextFormat.shortDebugString(ids));
          }
          if (lastFlushedSequenceId == null) {
            lastFlushedSequenceId = -1L;
          }
          lastFlushedSequenceIds.put(encodedRegionNameAsStr, lastFlushedSequenceId);
        }
        // Don't send Compaction/Close/Open region events to recovered edit type sinks.
        if (lastFlushedSequenceId >= entry.getKey().getLogSeqNum() ||
          (entry.getEdit().isMetaEdit() && !outputSink.keepRegionEvent(entry))) {
          editsSkipped++;
          continue;
        }
        entryBuffers.appendEntry(entry);
        editsCount++;
        int moreWritersFromLastCheck = this.getNumOpenWriters() - numOpenedFilesLastCheck;
        // If sufficient edits have passed, check if we should report progress.
        if (editsCount % interval == 0
          || moreWritersFromLastCheck > numOpenedFilesBeforeReporting) {
          numOpenedFilesLastCheck = this.getNumOpenWriters();
          String countsStr = (editsCount - (editsSkipped + outputSink.getSkippedEdits()))
            + " edits, skipped " + editsSkipped + " edits.";
          status.setStatus("Split " + countsStr);
          if (reporter != null && !reporter.progress()) {
            progressFailed = true;
            return false;
          }
        }
      }
    } catch (InterruptedException ie) {
      throw (IOException) new InterruptedIOException().initCause(ie);
    } catch (CorruptedLogFileException e) {
      markCorruptedLog(logNameWithParent, e);
      isCorrupted = true;
    } catch (IOException e) {
      throw RemoteExceptionHandler.checkIOException(e);
    } finally {
      LOG.debug("Finishing writing output logs and closing down.");
      try {
        if (null != in) {
          in.close();
        }
      } catch (IOException exception) {
        LOG.warn("Could not close wal reader: " + exception.getMessage());
        LOG.debug("exception details", exception);
      }
      try {
        if (outputSinkStarted) {
          // Set progressFailed to true as the immediate following statement will reset its value
          // when finishWritingAndClose() throws exception, progressFailed has the right value
          progressFailed = true;
          progressFailed = outputSink.finishWritingAndClose() == null;
        }
      } finally {
        logCompleteInfo(editsCount, editsSkipped, logNameWithParent, logLength, isCorrupted,
          progressFailed);
      }
    }
    return !progressFailed;
  }

  private void markCorruptedLog(String logNameWithParent, Exception e) {
    LOG.warn("Could not parse, corrupted log file " + logNameWithParent, e);
    Path logPath = new Path(logNameWithParent);
    ZKSplitLog.markCorruptedDistributedLog(logPath.getParent(), logPath.getName(), walNamespace);
  }

  private void logDebugInfo(String info) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(info);
    }
  }

  private void logCompleteInfo(long editsCount, long editsSkipped, String logNameWithParent,
      long logLength, boolean isCorrupted, boolean progressFailed) {
    // See if length got updated post lease recovery
    String msg = "Processed " + editsCount + " edits across "
      + outputSink.getNumberOfRecoveredRegions() + " regions; edits skipped=" + editsSkipped
      + "; log file=" + logNameWithParent + ", length=" + logLength + ", corrupted="
      + isCorrupted + ", progress failed=" + progressFailed;
    LOG.info(msg);
    status.markComplete(msg);
  }

  /**
   * Create a new {@link Reader} for reading logs to split.
   *
   * @return A new Reader instance, caller should close
   */
  protected Reader getReader(String logName, long length, boolean skipErrors,
      CancelableProgressable reporter) throws IOException, CorruptedLogFileException {
    Reader in;

    // Check for possibly empty file. With appends, currently Hadoop reports a
    // zero length even if the file has been sync'd. Revisit if HDFS-376 or
    // HDFS-878 is committed.
    if (length <= 0) {
      LOG.warn("File " + logName + " might be still open, length is 0");
    }

    try {
      in = getReader(new Path(logName), reporter);
    } catch (LogEmptyException e) {
      if (length <= 0) {
        // TODO should we ignore an empty, not-last log file if skip.errors
        // is false? Either way, the caller should decide what to do. E.g.
        // ignore if this is the last log in sequence.
        // TODO is this scenario still possible if the log has been
        // recovered (i.e. closed)
        LOG.warn("Could not open " + logName + " for reading. Log is empty", e);
        return null;
      } else {
        // LogEmptyException being ignored
        return null;
      }
    } catch (IOException ioe) {
      if (ioe instanceof LogNotFoundException) {
        // A wal log may not exist anymore. Nothing can be recovered so move on
        LOG.warn("Log " + logName + " doesn't exist anymore. \n", ioe);
        return null;
      }
      if (!skipErrors || ioe instanceof InterruptedIOException) {
        throw ioe; // Don't mark the file corrupted if interrupted, or not skipErrors
      }
      CorruptedLogFileException t =
        new CorruptedLogFileException("skipErrors=true Could not open wal " +
          logName + " ignoring");
      t.initCause(ioe);
      throw t;
    }
    return in;
  }

  static private Entry getNextLogLine(Reader in, String logName, boolean skipErrors)
    throws CorruptedLogFileException, IOException {
    try {
      return in.next();
    } catch (EOFException eof) {
      // truncated files are expected if a RS crashes (see HBASE-2643)
      LOG.info("EOF from wal " + logName + ".  continuing");
      return null;
    } catch (IOException e) {
      // If the IOE resulted from bad file format,
      // then this problem is idempotent and retrying won't help
      if (e.getCause() != null &&
        (e.getCause() instanceof ParseException ||
          e.getCause() instanceof org.apache.hadoop.fs.ChecksumException)) {
        LOG.warn("Parse exception " + e.getCause().toString() + " from wal "
          + logName + ".  continuing");
        return null;
      }
      if (!skipErrors) {
        throw e;
      }
      CorruptedLogFileException t =
        new CorruptedLogFileException("skipErrors=true Ignoring exception" +
          " while parsing wal " + logName + ". Marking as corrupted");
      t.initCause(e);
      throw t;
    }
  }

  // A wrapper to split all sublogs of one node using the method used by distributed
  // log splitting. Used by tools and unit tests. It should be package private.
  // It is public only because UpgradeTo96 and TestWALObserver are in different packages,
  // which uses this method to do log splitting.
  public static List<Path> split(Path logPath, Path oldLogRoot, Configuration conf,
      final WALFactory factory, Namespace walNamespace) throws IOException {
    final Path[] logs = SplitLogManager.getLogList(conf, Collections.singletonList(logPath),
      walNamespace);
    List<Path> splits = new ArrayList<>();
    if (logs.length > 0) {
      for (Path log: logs) {
        DistributedLogWALSplitter s = new DistributedLogWALSplitter(conf, factory, null, null,
          ZooKeeperProtos.SplitLogTask.RecoveryMode.LOG_SPLITTING);
        if (s.splitLog(WALUtils.pathToDistributedLogName(log), null)) {
          finishSplitLogs(walNamespace, logPath, oldLogRoot, log, conf);
          if (s.outputSink.splits != null) {
            splits.addAll(s.outputSink.splits);
          }
        }
      }
    }
    WALUtils.deleteLogsUnderPath(walNamespace, WALUtils.pathToDistributedLogName(logPath),
      DistributedLogAccessor.getDistributedLogStreamName(conf), true);
    return splits;
  }

  public static void finishSplitLogs(String logName, Configuration conf) throws IOException {
    Namespace walNamespace = null;
    Path walRoot = new Path("/");
    Path oldLogDir = new Path(HConstants.HREGION_OLDLOGDIR_NAME);
    try {
      walNamespace = DistributedLogAccessor.getInstance(conf).getNamespace();
    } catch (Exception e) {
      throw new IOException(e);
    }
    finishSplitLogs(walNamespace, walRoot, oldLogDir, new Path(logName), conf);
  }

  private static void finishSplitLogs(final Namespace walNamespace, Path walRoot, Path oldLogDir,
      Path logPath, Configuration conf) throws IOException {
    List<Path> processedLogs = new ArrayList<Path>();
    List<Path> corruptedLogs = new ArrayList<Path>();

    if (ZKSplitLog.isCorruptedDistributedLog(walRoot, logPath.getName(), walNamespace)) {
      corruptedLogs.add(logPath);
    } else {
      processedLogs.add(logPath);
    }
    archiveLogs(corruptedLogs, processedLogs, oldLogDir, walNamespace, conf);
    Path stagingDir = ZKSplitLog.getSplitLogDir(walRoot, logPath.getName());
    WALUtils.deleteLogsUnderPath(walNamespace, WALUtils.pathToDistributedLogName(stagingDir),
      DistributedLogAccessor.getDistributedLogStreamName(conf), true);
  }

  /**
   * Moves processed logs to a oldLogDir after successful processing Moves
   * corrupted logs (any log that couldn't be successfully parsed to corruptDir
   * (.corrupt) for later investigation
   */
  private static void archiveLogs(final List<Path> corruptedLogs, final List<Path> processedLogs,
      final Path oldLogDir, final Namespace walNamespace, final Configuration conf)
    throws IOException {
    final Path corruptDir = new Path(conf.get("hbase.regionserver.hlog.splitlog.corrupt.dir",
      HConstants.CORRUPT_DIR_NAME));
    // this method can get restarted or called multiple times for archiving
    // the same log files.
    for (Path corrupted : corruptedLogs) {
      Path newPath = new Path(corruptDir, corrupted.getName());
      String logName = WALUtils.pathToDistributedLogName(corrupted);
      if (walNamespace.logExists(logName)) {
        try {
          WALUtils.checkEndOfStream(walNamespace, logName);
          walNamespace.renameLog(logName, WALUtils.pathToDistributedLogName(newPath)).get();
          LOG.warn("Moved corrupted log " + corrupted + " to " + newPath);
        } catch (Exception e) {
          LOG.warn("Failed to move corrupted log " + logName + " to " + newPath
            + " with exception:\n", e);
        }
      }
    }

    for (Path path : processedLogs) {
      Path newPath = WALUtils.getWALArchivePath(oldLogDir, path);
      String logName = WALUtils.pathToDistributedLogName(path);
      if (walNamespace.logExists(logName)) {
        try {
          WALUtils.checkEndOfStream(walNamespace, logName);
          walNamespace.renameLog(logName, WALUtils.pathToDistributedLogName(newPath)).get();
          LOG.info("Archived processed log " + logName + " to " + newPath);
        } catch (Exception e) {
          LOG.warn("Failed to archived processed log " + logName + " to " + newPath
            + " with exception:\n", e);
        }
      }
    }
  }
}
