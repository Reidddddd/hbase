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
 * limitations under the License
 */
package org.apache.hadoop.hbase.regionserver.wal;

import static org.apache.hadoop.hbase.wal.WALUtils.DISTRIBUTED_LOG_ARCHIVE_PREFIX;
import dlshade.org.apache.distributedlog.api.DistributedLogManager;
import dlshade.org.apache.distributedlog.api.namespace.Namespace;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.wal.WALUtils;
import org.apache.hadoop.hbase.wal.Writer;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A DistributedLog based implementation of {@link AbstractLog}.
 * Due to the coprocessor compatibility reason, we use the hadoop.Path class.
 * TODO: Remove the dependency of fs.Path.
 */
@InterfaceAudience.Private
public class DistributedLog extends AbstractLog {
  private static final Log LOG = LogFactory.getLog(DistributedLog.class);
  private static final String DISTRIBUTED_LOG_ROLL_SIZE =
    "hbase.regionserver.wal.distributedlog.rollsize";
  private static final long DEFAULT_DISTRIBUTED_LOG_ROLL_SIZE = 134217728; // 128 MB
  private static final String DISTRIBUTED_LOG_SYNC_PERIOD =
    "hbase.regionserver.wal.distributedlog.sync.period";
  private static final int DEFAULT_DISTRIBUTED_LOG_FORCE_PERIOD = 100;
  
  private final ScheduledExecutorService forceScheduler = Executors.newScheduledThreadPool(1);
  private final Namespace distributedLogNamespace;
  private final String serverName;

  public DistributedLog(Configuration conf, List<WALActionsListener> listeners, final String prefix,
      final String suffix, String serverName) throws IOException {
    super(conf, listeners, prefix, suffix);
    try {
      distributedLogNamespace = DistributedLogAccessor.getInstance(conf).getNamespace();
    } catch (Exception e) {
      LOG.error("Failed accessing distributedlog with exception: \n", e);
      throw new IOException(e);
    }
    this.serverName = serverName;
    init();
    ourLogs = new LogNameFilter(prefixLogStr, logNameSuffix);
    rollWriter();
    initDisruptor();

    forceScheduler.scheduleAtFixedRate(
      () -> {
        Exception lastException = null;
        try {
          if (writer != null) {
            ((DistributedLogWriter) writer).forceWriter();
          }
        } catch (IOException e) {
          LOG.error("Error syncing, request close of WAL", e);
          lastException = e;
        } catch (Exception e) {
          LOG.warn("UNEXPECTED", e);
          lastException = e;
        } finally {
          if (lastException != null) {
            requestLogRoll();
          } else {
            checkLogRoll();
          }
        }
      }, 100, conf.getInt(DISTRIBUTED_LOG_SYNC_PERIOD, DEFAULT_DISTRIBUTED_LOG_FORCE_PERIOD),
      TimeUnit.MILLISECONDS);
  }

  @Override
  protected void archiveLogUnities(List<String> logsToArchive) throws IOException {
    if (logsToArchive != null) {
      for (String p : logsToArchive) {
        archiveLogEntity(p);
        this.byWalRegionSequenceIds.remove(p);
      }
    }
  }


  // The log path is ServerName/logName
  // The archived path is oldWALs/logName
  private void archiveLogEntity(String log) throws IOException {
    Path oldPath = new Path(log);
    Path newPath = new Path(DISTRIBUTED_LOG_ARCHIVE_PREFIX, oldPath.getName());
    // Tell our listeners that a log is going to be archived.
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.preLogArchive(oldPath, newPath);
      }
    }
    LOG.info("Archiving " + oldPath + " to " + newPath);
    DistributedLogManager distributedLogManager = distributedLogNamespace.openLog(log);
    long size = distributedLogManager.getLogRecordCount() == 0 ? 0 :
      distributedLogManager.getLastTxId();
    this.totalLogSize.addAndGet(-size);

    // Make sure there will be no more write
    WALUtils.checkEndOfStream(distributedLogManager);
    distributedLogManager.close();
    try {
      distributedLogNamespace.renameLog(log, WALUtils.pathToDistributedLogName(newPath)).get();
    } catch (ExecutionException | InterruptedException | CancellationException e) {
      throw new IOException("Unable to rename " + log + " to " + newPath, e);
    }

    // Tell our listeners that a log has been archived.
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.postLogArchive(oldPath, newPath);
      }
    }
  }

  @Override
  protected Writer createWriterInstance(String newWriterName) throws IOException {
    if (newWriterName == null) {
      throw new IOException("Cannot create writer with null name.");
    }
    Writer writer = WALUtils.createWriter(conf, null, new Path(newWriterName), false);
    if (! (writer instanceof DistributedLogWriter)) {
      throw new IllegalArgumentIOException("DistributedLog must create " +
        DistributedLogWriter.class.getName() + " but get: " + writer.getClass().getName());
    }
    return writer;
  }

  @Override
  protected void afterReplaceWriter(String newPathStr, String oldPathStr, Writer nextWriter)
    throws IOException {
    int oldNumEntries = this.numEntries.get();
    this.numEntries.set(0);
    try {
      if (oldPathStr != null) {
        this.byWalRegionSequenceIds.put(oldPathStr, this.sequenceIdAccounting.resetHighest());
        DistributedLogManager distributedLogManager = distributedLogNamespace.openLog(oldPathStr);
        long oldFileLen = distributedLogManager.getLogRecordCount() == 0 ? 0 :
          distributedLogManager.getLastTxId();
        distributedLogManager.close();

        this.totalLogSize.addAndGet(oldFileLen);
        LOG.info("Rolled WAL " + oldPathStr + " with entries=" + oldNumEntries + ", filesize="
          + StringUtils.byteDesc(oldFileLen) + "; new WAL " + newPathStr);
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

  @Override
  protected void checkLogRoll() {
    try {
      if ((writer != null && writer.getLength() > logrollsize)) {
        requestLogRoll();
      }
    } catch (IOException e) {
      LOG.warn("Writer.getLength() failed; continuing", e);
    }
  }

  @Override
  public void requestLogRoll() {
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i: this.listeners) {
        // We do not care the number of replicas when we use DistributedLog to store WAL.
        i.logRollRequested(false);
      }
    }
  }

  @Override
  protected void init() {
    logrollsize = this.conf.getLong(DISTRIBUTED_LOG_ROLL_SIZE, DEFAULT_DISTRIBUTED_LOG_ROLL_SIZE);
    this.prefixLogStr = WALUtils.getFullPathStringForDistributedLog(this.serverName,
      this.logNamePrefix + WALUtils.WAL_FILE_NAME_DELIMITER);
  }

  @Override
  protected String getLogFullName() {
    // In UT, the writer could be not an instance of DistributedLogWriter.
    // So we check the type here.
    if (writer instanceof DistributedLogWriter) {
      return ((DistributedLogWriter) writer).getURL();
    }
    return writer.toString();
  }

  @Override
  protected boolean checkExists(String logName) throws IOException {
    try {
      return distributedLogNamespace.logExists(logName);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  protected long updateHighestSyncedSequence(long sequence) {
    long sequenceId = super.updateHighestSyncedSequence(sequence);
    ((DistributedLogWriter) writer).setHighestUnsyncedSequenceId(sequenceId);
    return sequenceId;
  }

  @Override
  protected void nextWriterInit(Writer nextWriter) {
    // we currently have nothing to do with the next writer.
  }

  /**
   * TODO:WALCoprocessors are currently deep coupled with hdfs.Path so that we have to use Path here
   * TODO:Supplant Path variables after make WALCoprocessors & Listeners compatible with String.
   */
  @Override
  public void close() throws IOException {
    shutdown();
    try {
      forceScheduler.shutdown();
      // We only archive the logs under this server.
      Iterator<String> logNames = distributedLogNamespace.getLogs(serverName);
      String logBaseURL = distributedLogNamespace.getNamespaceDriver().getUri().getPath();
      int numOfLogs = 0;
      while (logNames.hasNext()) {
        String logName = logNames.next();
        if (!ourLogs.accept(logName)) {
          continue;
        }
        String archiveLogName =
          WALUtils.getWALArchivePathStr(DISTRIBUTED_LOG_ARCHIVE_PREFIX, logName);
        String logNameWithParent = WALUtils.getFullPathStringForDistributedLog(serverName, logName);
        // Tell our listeners that a log is going to be archived.
        if (!this.listeners.isEmpty()) {
          for (WALActionsListener i : this.listeners) {
            i.preLogArchive(new Path(logBaseURL, logName), new Path(logBaseURL, archiveLogName));
          }
        }

        WALUtils.checkEndOfStream(distributedLogNamespace, logNameWithParent);
        distributedLogNamespace.renameLog(logNameWithParent, archiveLogName).get();

        // Tell our listeners that a log was archived.
        if (!this.listeners.isEmpty()) {
          for (WALActionsListener i : this.listeners) {
            i.postLogArchive(new Path(logBaseURL, logName), new Path(logBaseURL, archiveLogName));
          }
        }
        numOfLogs += 1;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Renamed " + numOfLogs + " distributedlogs to old wals.");
      }
      LOG.info("Closed WAL: " + toString());
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
