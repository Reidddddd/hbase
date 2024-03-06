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
package org.apache.hadoop.hbase.regionserver.wal.distributedlog;

import static org.apache.hadoop.hbase.wal.WALUtils.DISTRIBUTED_LOG_ARCHIVE_PREFIX;
import dlshade.org.apache.bookkeeper.bookie.BookieException;
import dlshade.org.apache.distributedlog.api.DistributedLogManager;
import dlshade.org.apache.distributedlog.api.namespace.Namespace;
import dlshade.org.apache.distributedlog.exceptions.BKTransmitException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.regionserver.wal.AbstractLog;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.util.LogNameFilter;
import org.apache.hadoop.hbase.wal.WALUtils;
import org.apache.hadoop.hbase.wal.Writer;
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
    logNameFilter = new LogNameFilter(prefixLogStr, logNameSuffix);
    rollWriter();
    initDisruptor();
  }

  @Override
  public void archiveLogUnities(List<String> logsToArchive) throws IOException {
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
  public Writer createWriterInstance(String newWriterName) throws IOException {
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
      // We only archive the logs under this server.
      Iterator<String> logNames = distributedLogNamespace.getLogs(serverName);
      String logBaseURL = distributedLogNamespace.getNamespaceDriver().getUri().getPath();
      int numOfLogs = 0;
      while (logNames.hasNext()) {
        String logName = logNames.next();
        if (!logNameFilter.accept(logName)) {
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

  @Override
  public CompletableFuture<Void> asyncCloseWriter(Writer writer, String oldPath, String newPath,
      Writer nextWriter) {
    CompletableFuture<Void> res = super.asyncCloseWriter(writer, oldPath, newPath, nextWriter);
    CompletableFuture<Void> newFuture = new CompletableFuture<>();
    res.whenComplete((s, t) -> {
      if (t == null) {
        newFuture.complete(null);
      } else if (t instanceof BKTransmitException) {
        int errorCode = ((BKTransmitException) t).getBKResultCode();
        if (errorCode == BookieException.Code.LedgerFencedException) {
          // We will get this exception when the basic bk ledger is under recovery.
          // It is still readable and we have already finished writer switching.
          // Just log it and continue.
          LOG.warn("Met exception when close writer of: " + oldPath, t);
          newFuture.complete(null);
        } else {
          newFuture.completeExceptionally(t);
        }
      } else {
        newFuture.completeExceptionally(t);
      }
    });

    return newFuture;
  }
}
