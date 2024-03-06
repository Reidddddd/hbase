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
package org.apache.hadoop.hbase.wal.bookkeeper;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.TextFormat;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoordinatedStateException;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableStateManager;
import org.apache.hadoop.hbase.io.hfile.CorruptHFileException;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.regionserver.LastSequenceId;
import org.apache.hadoop.hbase.regionserver.wal.bookkeeper.LedgerLogReader;
import org.apache.hadoop.hbase.regionserver.wal.bookkeeper.LedgerLogSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.LedgerUtil;
import org.apache.hadoop.hbase.wal.AbstractWALSplitter;
import org.apache.hadoop.hbase.wal.BoundedLogWriterCreationOutputSink;
import org.apache.hadoop.hbase.wal.CorruptedLogFileException;
import org.apache.hadoop.hbase.wal.Entry;
import org.apache.hadoop.hbase.wal.LogRecoveredEditsOutputSink;
import org.apache.hadoop.hbase.wal.LogReplayOutputSink;
import org.apache.hadoop.hbase.wal.Reader;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALUtils;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;

@InterfaceAudience.Private
public class LedgerLogSplitter extends AbstractWALSplitter {
  private static final Log LOG = LogFactory.getLog(LedgerLogSplitter.class);

  private final LedgerLogSystem ledgerLogSystem;
  private final FileSystem outputFS;

  public LedgerLogSplitter(final WALFactory factory, Configuration conf,
      FileSystem outputFS, LedgerLogSystem ledgerLogSystem, LastSequenceId idChecker,
      CoordinatedStateManager csm, ZooKeeperProtos.SplitLogTask.RecoveryMode mode) {
    super(conf, factory, idChecker, csm, mode);
    this.outputFS = outputFS;
    this.ledgerLogSystem = ledgerLogSystem;
    initOutputSink();
  }

  @Override
  protected void initOutputSink() {
    if (csm != null && this.distributedLogReplay) {
      outputSink = new LogReplayOutputSink(controller, entryBuffers, numWriterThreads, conf,
        disablingOrDisabledTables, lastFlushedSequenceIds, csm, regionMaxSeqIdInStores,
        failedServerName, outputFS, this);
      return;
    }
    if (this.distributedLogReplay) {
      LOG.info("ZooKeeperWatcher is passed in as NULL so disable distrubitedLogRepaly.");
    }
    this.distributedLogReplay = false;
    outputSink = splitWriterCreationBounded ?
      new BoundedLogWriterCreationOutputSink(controller, entryBuffers, numWriterThreads,
        outputFS, conf, regionMaxSeqIdInStores, this) :
      new LogRecoveredEditsOutputSink(controller, entryBuffers, numWriterThreads,
        outputFS, conf, this, regionMaxSeqIdInStores);
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
  public static boolean splitLog(String logName, Configuration conf, FileSystem outputFS,
      CancelableProgressable reporter, LastSequenceId idChecker, CoordinatedStateManager cp,
      ZooKeeperProtos.SplitLogTask.RecoveryMode mode, final WALFactory factory)
      throws IOException {
    LedgerLogSplitter splitter = new LedgerLogSplitter(factory, conf, outputFS,
      LedgerLogSystem.getInstance(conf), idChecker, cp, mode);
    return splitter.splitLog(logName, reporter);
  }

  /**
   * log splitting implementation, splits one log file.
   * @param logNameWithParent should be an actual log file.
   */
  @VisibleForTesting
  public boolean splitLog(String logNameWithParent, CancelableProgressable reporter)
      throws IOException {
    Preconditions.checkState(status == null);
    LOG.info("Start splitting log: " + logNameWithParent);

    boolean isCorrupted = false;
    boolean outputSinkStarted = false;
    boolean progressFailed = false;
    boolean skipErrors = conf.getBoolean(SPLIT_SKIP_ERRORS, SPLIT_SKIP_ERRORS_DEFAULT);
    int interval = conf.getInt("hbase.splitlog.report.interval.loglines", 1024);
    int editsCount = 0;
    int editsSkipped = 0;
    long logLength = 0;
    long readedEntries = 0;

    status = TaskMonitor.get().createStatus("Splitting log file " + logNameWithParent
      + "into a temporary staging area.");

    // If the log is already closed, this invoke has no effect.
    // If the RS still hold the log, this method will inform the RS we are closing by zk.
    ledgerLogSystem.closeLog(logNameWithParent);
    try {
      // The log length could be 0 when the RS could not update the size info in the metadata.
      // This is no harm to us, that the length is only used for log.
      logLength = ledgerLogSystem.getLogDataSize(logNameWithParent);
    } catch (IOException e) {
      LOG.warn("Failed get log length of log: " + logNameWithParent + " nothing to split.");
      return false;
    }

    if (this.outputSink instanceof LogRecoveredEditsOutputSink) {
      ((LogRecoveredEditsOutputSink) this.outputSink)
        .setFileNameBeingSplit(new Path(logNameWithParent).getName());
    }

    Reader in = null;
    try {
      LOG.info("Splitting wal: " + logNameWithParent + ", length= " + logLength
        + " DistributedLogReplay = " + this.distributedLogReplay);
      status.setStatus("Opening log");
      if (reporter != null && !reporter.progress()) {
        progressFailed = true;
        return false;
      }

      try {
        in = getReader(new Path(logNameWithParent), reporter);
      } catch (IOException e) {
        if (e.getMessage().contains("Corrupted")) {
          throw new CorruptHFileException(logNameWithParent);
        } else {
          throw e;
        }
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

      int numOpenedFilesBeforeReporting =
        conf.getInt(SPLIT_LOG_REPORT_OPENED_FILES, DEFAULT_SPLIT_LOG_REPORT_OPENED_FILES);
      int numOpenedFilesLastCheck = 0;

      outputSink.setReporter(reporter);
      outputSink.startWriterThreads();
      outputSinkStarted = true;

      long lastFlushedSequenceId = -1L;
      Path logPath = new Path(logNameWithParent);
      ServerName serverName =
        WALUtils.getServerNameFromWALDirectoryName(logPath, logPath.depth() > 1);
      failedServerName = (serverName == null) ? "" : serverName.getServerName();

      Entry entry;
      while ((entry = getNextLogLine(in, logNameWithParent, skipErrors)) != null) {
        readedEntries += 1;
        lastFlushedSequenceId = doEntryReading(entry);
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
        if (editsCount % interval == 0 ||
            moreWritersFromLastCheck > numOpenedFilesBeforeReporting) {
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
      markCorruptedLog(ledgerLogSystem, logNameWithParent, e);
      isCorrupted = true;
    } catch (IOException e) {
      throw RemoteExceptionHandler.checkIOException(e);
    } finally {
      logDebugInfo("Finishing writing output logs and closing down.");
      LOG.info("Readed " + readedEntries + " entries from log: " + logNameWithParent);
      try {
        if (in != null) {
          in.close();
        }
      } catch (IOException exception) {
        LOG.warn("Could not close wal reader: " + exception.getMessage());
        logDebugInfo(exception.getMessage());
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

  private void markCorruptedLog(LedgerLogSystem ledgerLogSystem, String logNameWithParent,
      Exception e) {
    LOG.warn("Could not parse, corrupted log file " + logNameWithParent, e);
    Path logPath = new Path(logNameWithParent);
    ZKSplitLog.markCorruptedLedgerLog(ledgerLogSystem, ZKUtil.getParent(logNameWithParent),
      logPath.getName());
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

  private void logDebugInfo(String info) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(info);
    }
  }

  private long doEntryReading(Entry entry) throws IOException, InterruptedException {
    byte[] region = entry.getKey().getEncodedRegionName();
    String encodedRegionNameAsStr = Bytes.toString(region);
    Long lastFlushedSequenceId = lastFlushedSequenceIds.get(encodedRegionNameAsStr);
    if (lastFlushedSequenceId != null) {
      return lastFlushedSequenceId;
    }
    if (this.distributedLogReplay) {
      ClusterStatusProtos.RegionStoreSequenceIds ids = csm.getSplitLogWorkerCoordination()
        .getRegionFlushedSequenceId(failedServerName, encodedRegionNameAsStr);
      if (ids != null) {
        lastFlushedSequenceId = ids.getLastFlushedSequenceId();
        logDebugInfo("DLR Last flushed sequenceId for " + encodedRegionNameAsStr + ": " +
          TextFormat.shortDebugString(ids));
      }
    } else if (sequenceIdChecker != null) {
      ClusterStatusProtos.RegionStoreSequenceIds ids = sequenceIdChecker.getLastSequenceId(region);
      Map<byte[], Long> maxSeqIdInStores = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
      for (ClusterStatusProtos.StoreSequenceId storeSeqId : ids.getStoreSequenceIdList()) {
        maxSeqIdInStores.put(storeSeqId.getFamilyName().toByteArray(), storeSeqId.getSequenceId());
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
    return lastFlushedSequenceId;
  }

  /**
   * Create a new {@link Reader} for reading logs to split.
   * @return new Reader instance, caller should close
   */
  @Override
  protected Reader getReader(Path logPath, CancelableProgressable reporter) throws IOException {
    try {
      return WALUtils.createReader(null, logPath, null, conf, LedgerLogReader.class);
    } catch (Exception e) {
      if (e.getCause() instanceof KeeperException.NoNodeException) {
        // A wal log may not exist anymore. Nothing can be recovered so move on
        LOG.warn("Log " + logPath + " doesn't exist anymore.", e);
        return null;
      }
      throw new IOException(e);
    }
  }

  public static void finishSplitLogs(String logPath, Configuration conf)
      throws IOException {
    LOG.info("Finished splitting: " + logPath);
    String logRoot = LedgerUtil.getLedgerRootPath(conf);
    logPath = checkLogPath(logPath, conf);
    List<String> processedLogs = new ArrayList<>();
    List<String> corruptedLogs = new ArrayList<>();
    LedgerLogSystem ledgerLogSystem = LedgerLogSystem.getInstance(conf);

    if (ZKSplitLog.isCorruptedLedgerLog(ledgerLogSystem, logRoot, ZKUtil.getNodeName(logPath))) {
      corruptedLogs.add(logPath);
    } else {
      processedLogs.add(logPath);
    }

    archiveLogs(corruptedLogs, processedLogs, logRoot, LedgerUtil.getLedgerArchivePath(conf),
      ledgerLogSystem);
    String stagingPath = ZKUtil.joinZNode(
      ZKUtil.joinZNode(logRoot, HConstants.SPLIT_LOGDIR_NAME),
      ZKUtil.getNodeName(logPath)
    );
    ledgerLogSystem.deleteRecursively(stagingPath);
  }

  static String checkLogPath(String logPath, Configuration conf) {
    String walRoot = LedgerUtil.getLedgerRootPath(conf);
    if (logPath.startsWith(ZKSplitLog.TASK_PREFIX)) {
      String logRoot = LedgerUtil.getLedgerLogPath(walRoot);
      return ZKUtil.joinZNode(logRoot, logPath.substring(ZKSplitLog.TASK_PREFIX.length()));
    }
    return logPath;
  }

  /**
   * Moves processed logs to a oldLogDir after successful processing Moves
   * corrupted logs (any log that couldn't be successfully parsed to corruptDir
   * (.corrupt) for later investigation
   */
  private static void archiveLogs(final List<String> corruptedLogs,
      final List<String> processedLogs, final String walRootPath, final String archivePath,
      LedgerLogSystem ledgerLogSystem) throws IOException {
    final String corruptPath = ZKUtil.joinZNode(walRootPath, HConstants.CORRUPT_DIR_NAME);
    // this method can get restarted or called multiple times for archiving
    // the same log files.
    for (String corrupted : corruptedLogs) {
      String newPath = ZKUtil.joinZNode(corruptPath, ZKUtil.getNodeName(corrupted));
      if (ledgerLogSystem.logExists(corrupted)) {
        try {
          ledgerLogSystem.renamePath(corrupted, newPath);
          LOG.warn("Moved corrupted log " + corrupted + " to " + newPath);
        } catch (Exception e) {
          LOG.warn("Failed to move corrupted log: " + corrupted + " to " + newPath
            + " with exception:\n", e);
        }
      }
    }

    for (String processed : processedLogs) {
      String newPath = ZKUtil.joinZNode(archivePath, ZKUtil.getNodeName(processed));
      if (ledgerLogSystem.logExists(processed)) {
        try {
          ledgerLogSystem.renamePath(processed, newPath);
          LOG.info("Archived processed log " + processed + " to " + newPath);
        } catch (Exception e) {
          LOG.warn("Failed to archived processed log " + processed + " to " + newPath
            + " with exception:\n", e);
        }
      }
    }
  }

  // A wrapper to split all sublogs of one node using the method used by distributed
  // log splitting. Used by tools and unit tests. It should be package private.
  // It is public only because UpgradeTo96 and TestWALObserver are in different packages,
  // which uses this method to do log splitting.
  public static List<String> split(String logPath, Configuration conf, final WALFactory factory,
      LedgerLogSystem ledgerLogSystem, FileSystem outputFS) throws IOException {
    final List<String> logs = ledgerLogSystem.getLogUnderPath(Collections.singletonList(logPath));
    List<String> splits = new ArrayList<>();
    if (logs.size() > 0) {
      for (String log: logs) {
        LedgerLogSplitter s = new LedgerLogSplitter(factory, conf, outputFS, ledgerLogSystem, null,
          null, ZooKeeperProtos.SplitLogTask.RecoveryMode.LOG_SPLITTING);
        // There may be someone still writing to the log. Close it.
        ledgerLogSystem.closeLog(log);
        if (s.splitLog(log, null)) {
          finishSplitLogs(log, conf);
          if (s.outputSink.getSplits() != null) {
            splits.addAll(s.outputSink.getSplits().stream().map(Path::toString)
              .collect(Collectors.toList()));
          }
        }
      }
    }

    ledgerLogSystem.deleteRecursively(logPath);
    return splits;
  }
}
