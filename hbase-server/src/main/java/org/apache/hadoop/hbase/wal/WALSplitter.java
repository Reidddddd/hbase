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
import java.io.EOFException;
import java.io.FileNotFoundException;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoordinatedStateException;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableStateManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.master.SplitLogManager;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos.RegionStoreSequenceIds;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos.StoreSequenceId;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.regionserver.LastSequenceId;
// imports for things that haven't moved from regionserver.wal yet.
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;

/**
 * This class is responsible for splitting up a bunch of regionserver commit log
 * files that are no longer being written to, into new files, one per region for
 * region to replay on startup. Delete the old log files when finished.
 */
@InterfaceAudience.Private
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="JLM_JSR166_UTILCONCURRENT_MONITORENTER",
  justification="Synchronization on concurrent map is intended")
public class WALSplitter extends AbstractWALSplitter {
  private static final Log LOG = LogFactory.getLog(WALSplitter.class);

  // Parameters for split process
  protected final Path walDir;
  protected final FileSystem walFS;

  @VisibleForTesting
  WALSplitter(final WALFactory factory, Configuration conf, Path walDir,
      FileSystem walFS, LastSequenceId idChecker,
      CoordinatedStateManager csm, RecoveryMode mode) {
    super(conf, factory, idChecker, csm, mode);
    this.walDir = walDir;
    this.walFS = walFS;
    initOutputSink();
  }

  @Override
  protected void initOutputSink() {
    if (csm != null && this.distributedLogReplay) {
      outputSink = new LogReplayOutputSink(controller, entryBuffers, numWriterThreads, conf,
        disablingOrDisabledTables, lastFlushedSequenceIds, csm, regionMaxSeqIdInStores,
        failedServerName, walFS, this);
    } else {
      if (this.distributedLogReplay) {
        LOG.info("ZooKeeperWatcher is passed in as NULL so disable distrubitedLogRepaly.");
      }
      this.distributedLogReplay = false;
      if(splitWriterCreationBounded){
        outputSink = new BoundedLogWriterCreationOutputSink(controller, entryBuffers,
          numWriterThreads, walFS, conf, regionMaxSeqIdInStores, this);
      }else {
        outputSink = new LogRecoveredEditsOutputSink(controller, entryBuffers, numWriterThreads,
          walFS, conf, this, regionMaxSeqIdInStores);
      }
    }
  }

  /**
   * Splits a WAL file into region's recovered-edits directory.
   * This is the main entry point for distributed log splitting from SplitLogWorker.
   * <p>
   * If the log file has N regions then N recovered.edits files will be produced.
   * <p>
   * @param rootDir
   * @param logfile
   * @param walFS FileSystem to use for WAL reading and splitting
   * @param conf
   * @param reporter
   * @param idChecker
   * @param cp coordination state manager
   * @return false if it is interrupted by the progress-able.
   * @throws IOException
   */
  public static boolean splitLogFile(Path walDir, FileStatus logfile, FileSystem walFS,
      Configuration conf, CancelableProgressable reporter, LastSequenceId idChecker,
      CoordinatedStateManager cp, RecoveryMode mode, final WALFactory factory) throws IOException {
    WALSplitter s = new WALSplitter(factory, conf, walDir, walFS, idChecker, cp, mode);
    return s.splitLogFile(logfile, reporter);
  }

  // A wrapper to split one log folder using the method used by distributed
  // log splitting. Used by tools and unit tests. It should be package private.
  // It is public only because UpgradeTo96 and TestWALObserver are in different packages,
  // which uses this method to do log splitting.
  public static List<Path> split(Path walRootDir, Path logDir, Path oldLogDir,
      FileSystem walFs, Configuration conf, final WALFactory factory) throws IOException {
    final FileStatus[] logfiles = SplitLogManager.getFileList(conf,
        Collections.singletonList(logDir), null);
    List<Path> splits = new ArrayList<Path>();
    if (logfiles != null && logfiles.length > 0) {
      for (FileStatus logfile: logfiles) {
        WALSplitter s = new WALSplitter(factory, conf, walRootDir, walFs, null, null,
            RecoveryMode.LOG_SPLITTING);
        if (s.splitLogFile(logfile, null)) {
          finishSplitLogFile(walRootDir, oldLogDir, logfile.getPath(), conf);
          if (s.outputSink.splits != null) {
            splits.addAll(s.outputSink.splits);
          }
        }
      }
    }
    if (!walFs.delete(logDir, true)) {
      throw new IOException("Unable to delete src dir: " + logDir);
    }
    return splits;
  }

  /**
   * log splitting implementation, splits one log file.
   * @param logfile should be an actual log file.
   */
  @VisibleForTesting
  boolean splitLogFile(FileStatus logfile, CancelableProgressable reporter) throws IOException {
    Preconditions.checkState(status == null);
    Preconditions.checkArgument(logfile.isFile(),
        "passed in file status is for something other than a regular file.");
    boolean isCorrupted = false;
    boolean skipErrors = conf.getBoolean("hbase.hlog.split.skip.errors",
      SPLIT_SKIP_ERRORS_DEFAULT);
    int interval = conf.getInt("hbase.splitlog.report.interval.loglines", 1024);
    Path logPath = logfile.getPath();
    boolean outputSinkStarted = false;
    boolean progress_failed = false;
    int editsCount = 0;
    int editsSkipped = 0;

    status = TaskMonitor.get().createStatus(
          "Splitting log file " + logfile.getPath() + "into a temporary staging area.");
    Reader in = null;
    // set the file being split currently
    if (this.outputSink instanceof LogRecoveredEditsOutputSink) {
      ((LogRecoveredEditsOutputSink) this.outputSink).setFileBeingSplit(logfile);
    }
    try {
      long logLength = logfile.getLen();
      LOG.info("Splitting wal: " + logPath + ", length=" + logLength);
      LOG.info("DistributedLogReplay = " + this.distributedLogReplay);
      status.setStatus("Opening log file");
      if (reporter != null && !reporter.progress()) {
        progress_failed = true;
        return false;
      }
      try {
        in = getReader(logfile, skipErrors, reporter);
      } catch (CorruptedLogFileException e) {
        LOG.warn("Could not get reader, corrupted log file " + logPath, e);
        ZKSplitLog.markCorrupted(walDir, logfile.getPath().getName(), walFS);
        isCorrupted = true;
      }
      if (in == null) {
        LOG.warn("Nothing to split in log file " + logPath);
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
      ServerName serverName = WALUtils.getServerNameFromWALDirectoryName(logPath);
      failedServerName = (serverName == null) ? "" : serverName.getServerName();
      while ((entry = getNextLogLine(in, logPath, skipErrors)) != null) {
        if (this.sanityCheck) {
          if (!checkWALEntrySanity(entry)) {
            // We have broken data here. Mark this as corrupted.
            ZKSplitLog.markCorrupted(walDir, logfile.getPath().getName(), walFS);
            progress_failed = true;
            isCorrupted = true;
            throw new CorruptedLogFileException("Read broken cell, corrupted log file " + logPath);
          }
        }
        byte[] region = entry.getKey().getEncodedRegionName();
        String encodedRegionNameAsStr = Bytes.toString(region);
        lastFlushedSequenceId = lastFlushedSequenceIds.get(encodedRegionNameAsStr);
        if (lastFlushedSequenceId == null) {
          if (this.distributedLogReplay) {
            RegionStoreSequenceIds ids =
                csm.getSplitLogWorkerCoordination().getRegionFlushedSequenceId(failedServerName,
                  encodedRegionNameAsStr);
            if (ids != null) {
              lastFlushedSequenceId = ids.getLastFlushedSequenceId();
              if (LOG.isDebugEnabled()) {
                LOG.debug("DLR Last flushed sequenceid for " + encodedRegionNameAsStr + ": " +
                  TextFormat.shortDebugString(ids));
              }
            }
          } else if (sequenceIdChecker != null) {
            RegionStoreSequenceIds ids = sequenceIdChecker.getLastSequenceId(region);
            Map<byte[], Long> maxSeqIdInStores = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
            for (StoreSequenceId storeSeqId : ids.getStoreSequenceIdList()) {
              maxSeqIdInStores.put(storeSeqId.getFamilyName().toByteArray(),
                storeSeqId.getSequenceId());
            }
            regionMaxSeqIdInStores.put(encodedRegionNameAsStr, maxSeqIdInStores);
            lastFlushedSequenceId = ids.getLastFlushedSequenceId();
            if (LOG.isDebugEnabled()) {
              LOG.debug("DLS Last flushed sequenceid for " + encodedRegionNameAsStr + ": " +
                  TextFormat.shortDebugString(ids));
            }
          }
          if (lastFlushedSequenceId == null) {
            lastFlushedSequenceId = -1L;
          }
          lastFlushedSequenceIds.put(encodedRegionNameAsStr, lastFlushedSequenceId);
        }
        if (lastFlushedSequenceId >= entry.getKey().getLogSeqNum()) {
          editsSkipped++;
          continue;
        }
        // Don't send Compaction/Close/Open region events to recovered edit type sinks.
        if (entry.getEdit().isMetaEdit() && !outputSink.keepRegionEvent(entry)) {
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
            progress_failed = true;
            return false;
          }
        }
      }
    } catch (InterruptedException ie) {
      IOException iie = new InterruptedIOException();
      iie.initCause(ie);
      throw iie;
    } catch (CorruptedLogFileException e) {
      LOG.warn("Could not parse, corrupted log file " + logPath, e);
      if (csm != null) {
        csm.getSplitLogWorkerCoordination().markCorrupted(walDir, logfile.getPath().getName(),
          walFS);
      }
      isCorrupted = true;
    } catch (IOException e) {
      e = RemoteExceptionHandler.checkIOException(e);
      throw e;
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
          // Set progress_failed to true as the immediate following statement will reset its value
          // when finishWritingAndClose() throws exception, progress_failed has the right value
          progress_failed = true;
          progress_failed = outputSink.finishWritingAndClose() == null;
        }
      } finally {
        String msg =
            "Processed " + editsCount + " edits across " + outputSink.getNumberOfRecoveredRegions()
                + " regions; edits skipped=" + editsSkipped + "; log file=" + logPath +
                ", length=" + logfile.getLen() + // See if length got updated post lease recovery
                ", corrupted=" + isCorrupted + ", progress failed=" + progress_failed;
        LOG.info(msg);
        status.markComplete(msg);
      }
    }
    return !progress_failed;
  }

  /**
   * Completes the work done by splitLogFile by archiving logs
   * <p>
   * It is invoked by SplitLogManager once it knows that one of the
   * SplitLogWorkers have completed the splitLogFile() part. If the master
   * crashes then this function might get called multiple times.
   * <p>
   * @param logfile
   * @param conf
   * @throws IOException
   */
  public static void finishSplitLogFile(String logfile,
      Configuration conf)  throws IOException {
    Path walDir = FSUtils.getWALRootDir(conf);
    Path oldLogDir = new Path(walDir, HConstants.HREGION_OLDLOGDIR_NAME);
    Path logPath;
    if (FSUtils.isStartingWithPath(walDir, logfile)) {
      logPath = new Path(logfile);
    } else {
      logPath = new Path(walDir, logfile);
    }
    finishSplitLogFile(walDir, oldLogDir, logPath, conf);
  }

  private static void finishSplitLogFile(Path walDir, Path oldLogDir,
      Path logPath, Configuration conf) throws IOException {
    List<Path> processedLogs = new ArrayList<Path>();
    List<Path> corruptedLogs = new ArrayList<Path>();
    FileSystem walFS = walDir.getFileSystem(conf);
    if (ZKSplitLog.isCorrupted(walDir, logPath.getName(), walFS)) {
      corruptedLogs.add(logPath);
    } else {
      processedLogs.add(logPath);
    }
    archiveLogs(corruptedLogs, processedLogs, oldLogDir, walFS, conf);
    Path stagingDir = ZKSplitLog.getSplitLogDir(walDir, logPath.getName());
    walFS.delete(stagingDir, true);
  }

  /**
   * Moves processed logs to a oldLogDir after successful processing Moves
   * corrupted logs (any log that couldn't be successfully parsed to corruptDir
   * (.corrupt) for later investigation
   *
   * @param corruptedLogs
   * @param processedLogs
   * @param oldLogDir
   * @param walFS FileSystem to use for WAL archival
   * @param conf
   * @throws IOException
   */
  private static void archiveLogs(
      final List<Path> corruptedLogs,
      final List<Path> processedLogs, final Path oldLogDir,
      final FileSystem walFS, final Configuration conf) throws IOException {
    final Path corruptDir = new Path(FSUtils.getWALRootDir(conf), conf.get(
        "hbase.regionserver.hlog.splitlog.corrupt.dir",  HConstants.CORRUPT_DIR_NAME));

    if (!walFS.mkdirs(corruptDir)) {
      LOG.info("Unable to mkdir " + corruptDir);
    }
    walFS.mkdirs(oldLogDir);

    // this method can get restarted or called multiple times for archiving
    // the same log files.
    for (Path corrupted : corruptedLogs) {
      Path p = new Path(corruptDir, corrupted.getName());
      if (walFS.exists(corrupted)) {
        if (!walFS.rename(corrupted, p)) {
          LOG.warn("Unable to move corrupted log " + corrupted + " to " + p);
        } else {
          LOG.warn("Moved corrupted log " + corrupted + " to " + p);
        }
      }
    }

    for (Path p : processedLogs) {
      Path newPath = WALUtils.getWALArchivePath(oldLogDir, p);
      if (walFS.exists(p)) {
        if (!FSUtils.renameAndSetModifyTime(walFS, p, newPath)) {
          LOG.warn("Unable to move  " + p + " to " + newPath);
        } else {
          LOG.info("Archived processed log " + p + " to " + newPath);
        }
      }
    }
  }

  /**
   * Create a new {@link Reader} for reading logs to split.
   *
   * @param file
   * @return A new Reader instance, caller should close
   * @throws IOException
   * @throws CorruptedLogFileException
   */
  protected Reader getReader(FileStatus file, boolean skipErrors, CancelableProgressable reporter)
      throws IOException, CorruptedLogFileException {
    Path path = file.getPath();
    long length = file.getLen();
    Reader in;

    // Check for possibly empty file. With appends, currently Hadoop reports a
    // zero length even if the file has been sync'd. Revisit if HDFS-376 or
    // HDFS-878 is committed.
    if (length <= 0) {
      LOG.warn("File " + path + " might be still open, length is 0");
    }

    try {
      FSUtils.getInstance(walFS, conf).recoverFileLease(walFS, path, conf, reporter);
      try {
        in = getReader(path, reporter);
      } catch (EOFException e) {
        if (length <= 0) {
          // TODO should we ignore an empty, not-last log file if skip.errors
          // is false? Either way, the caller should decide what to do. E.g.
          // ignore if this is the last log in sequence.
          // TODO is this scenario still possible if the log has been
          // recovered (i.e. closed)
          LOG.warn("Could not open " + path + " for reading. File is empty", e);
          return null;
        } else {
          // EOFException being ignored
          return null;
        }
      }
    } catch (IOException e) {
      if (e instanceof FileNotFoundException) {
        // A wal file may not exist anymore. Nothing can be recovered so move on
        LOG.warn("File " + path + " doesn't exist anymore.", e);
        return null;
      }
      if (!skipErrors || e instanceof InterruptedIOException) {
        throw e; // Don't mark the file corrupted if interrupted, or not skipErrors
      }
      CorruptedLogFileException t =
        new CorruptedLogFileException("skipErrors=true Could not open wal " +
            path + " ignoring");
      t.initCause(e);
      throw t;
    }
    return in;
  }

  /**
   * Create a new {@link Writer} for writing log splits.
   * @return a new Writer instance, caller should close
   */
  @Override
  protected Writer createWriter(Path logfile) throws IOException {
    return WALSplitterUtil.createWriter(logfile, walFS, conf);
  }

  @Override
  protected Reader getReader(Path curLogFile, CancelableProgressable reporter) throws IOException {
    return WALSplitterUtil.getReader(curLogFile, reporter, walFS, conf);
  }

  public static void main(String[] args) {
    if (args.length < 2) {
      usage();
      System.exit(-1);
    }
    if (args[0].compareTo("--split") == 0) {
      Configuration conf = HBaseConfiguration.create();
      for (int i = 1; i < args.length; i++) {
        try {
          Path logPath = new Path(args[i]);
          FSUtils.setFsDefault(conf, logPath);
          split(conf, logPath);
        } catch (IOException t) {
          t.printStackTrace(System.err);
          System.exit(-1);
        }
      }
    } else {
      usage();
      System.exit(-1);
    }
  }

  private static void usage() {
    System.err.println("Usage: WALSplitter <ARGS>");
    System.err.println("Arguments:");
    System.err.println(" --split Split the passed directory of WAL logs");
    System.err.println("         For example: " +
        "FSHLog --split hdfs://example.com:9000/hbase/.logs/DIR");
  }

  private static void split(final Configuration conf, final Path p) throws IOException {
    FileSystem fs = FSUtils.getWALFileSystem(conf);
    if (!fs.exists(p)) {
      throw new FileNotFoundException(p.toString());
    }
    if (!fs.getFileStatus(p).isDirectory()) {
      throw new IOException(p + " is not a directory");
    }

    final Path baseDir = FSUtils.getWALRootDir(conf);
    final Path archiveDir = new Path(baseDir, HConstants.HREGION_OLDLOGDIR_NAME);
    split(baseDir, p, archiveDir, fs, conf, WALFactory.getInstance(conf));
  }
}
