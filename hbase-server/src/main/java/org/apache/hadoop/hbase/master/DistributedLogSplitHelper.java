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
package org.apache.hadoop.hbase.master;

import dlshade.org.apache.distributedlog.api.namespace.Namespace;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.regionserver.wal.distributedlog.DistributedLogAccessor;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.apache.hadoop.hbase.wal.WALUtils;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class DistributedLogSplitHelper extends AbstractSplitLogHelper {
  private static final Log LOG = LogFactory.getLog(DistributedLogSplitHelper.class);

  private final Namespace walNamespace;

  public DistributedLogSplitHelper(MasterFileSystem masterFileSystem) throws IOException {
    super(masterFileSystem);
    try {
      walNamespace =
        DistributedLogAccessor.getInstance(masterFileSystem.getConfiguration()).getNamespace();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  Path createInitialFileSystemLayout() throws IOException {
    // DistributedLog does not need to pre-create directory.
    // Just return the path of 'oldWALs'.
    return new Path(WALUtils.DISTRIBUTED_LOG_ARCHIVE_PREFIX);
  }

  @Override
  Set<ServerName> getFailedServersFromLogFolders() {
    boolean retrySplitting = !conf.getBoolean("hbase.hlog.split.skip.errors",
      WALSplitter.SPLIT_SKIP_ERRORS_DEFAULT);

    Set<ServerName> serverNames = new HashSet<>();

    do {
      if (this.masterFileSystem.master.isStopped()) {
        LOG.warn("Master stopped while trying to get failed servers.");
        break;
      }
      try {
        Iterator<String> iterator = walNamespace.getLogs();
        List<String> logRoots = new ArrayList<>();
        while (iterator.hasNext()) {
          logRoots.add(iterator.next());
        }

        // Get online servers after getting log folders to avoid log folder deletion of newly
        // checked in region servers . see HBASE-5916
        Set<ServerName> onlineServers = ((HMaster) masterFileSystem.master).getServerManager()
          .getOnlineServers().keySet();

        if (logRoots.size() == 0) {
          LOG.debug("No log files to split, proceeding...");
          return serverNames;
        }
        for (String logRoot : logRoots) {
          if (logRoot.startsWith(WALUtils.DISTRIBUTED_LOG_ARCHIVE_PREFIX)) {
            // Filtered the logs under oldWALs path.
            continue;
          }
          Iterator<String> subIterator = walNamespace.getLogs(logRoot);
          List<String> currentLogs = new ArrayList<>();
          while (subIterator.hasNext()) {
            currentLogs.add(subIterator.next());
          }

          if (currentLogs.size() == 0) {
            // Empty log folder. No recovery needed
            continue;
          }

          Path logRootPath = new Path(logRoot);
          final ServerName serverName = WALUtils.getServerNameFromWALDirectoryName(logRootPath,
            logRootPath.depth() > 1);
          if (null == serverName) {
            LOG.warn("Log path " + logRoot + " doesn't look like its name includes a " +
              "region server name; leaving in place. If you see later errors about missing " +
              "write ahead logs they may be saved in this location.");
          } else if (!onlineServers.contains(serverName)) {
            LOG.info("Log path " + logRoot + " doesn't belong to a known region server, " +
              "splitting");
            serverNames.add(serverName);
          } else {
            LOG.info("Log path " + logRoot + " belongs to an existing region server");
          }
        }
        retrySplitting = false;
      } catch (IOException ioe) {
        LOG.warn("Failed getting failed servers to be recovered.", ioe);
        try {
          if (retrySplitting) {
            Thread.sleep(conf.getInt("hbase.hlog.split.failure.retry.interval", 30 * 1000));
          }
        } catch (InterruptedException e) {
          LOG.warn("Interrupted, aborting since cannot return w/o splitting");
          Thread.currentThread().interrupt();
          retrySplitting = false;
          Runtime.getRuntime().halt(1);
        }
      }
    } while (retrySplitting);

    return serverNames;
  }

  @Override
  List<Path> getLogDirs(Set<ServerName> serverNames) throws IOException {
    List<Path> logDirs = new ArrayList<Path>();
    boolean needReleaseLock = false;
    if (!this.masterFileSystem.services.isInitialized()) {
      // during master initialization, we could have multiple places splitting a same wal
      this.splitLogLock.lock();
      needReleaseLock = true;
    }
    try {
      for (ServerName serverName : serverNames) {
        Path logDir = new Path(serverName.toString());
        Path splitDir = logDir.suffix(WALUtils.SPLITTING_EXT);

        try {
          WALUtils.renameDistributedLogsPath(walNamespace, logDir, splitDir);
        } catch (Exception e) {
          throw new IOException("Failed fs.rename for log split: " + logDir, e.getCause());
        }

        LOG.debug("Renamed region directory: " + splitDir);
        logDirs.add(splitDir);
      }
    } finally {
      if (needReleaseLock) {
        this.splitLogLock.unlock();
      }
    }
    return logDirs;
  }

  @Override
  void archiveMetaLog(ServerName serverName) {
    try {
      Path logDir = new Path(serverName.toString());
      Path splitDir = logDir.suffix(WALUtils.SPLITTING_EXT);
      List<String> logEntries = WALUtils.listLogs(walNamespace, splitDir,
        MasterFileSystem.META_FILTER);

      if (logEntries.size() > 0) {
        for (String entry : logEntries) {
          Path newPath = WALUtils.getWALArchivePath(this.masterFileSystem.getOldLogDir(),
            new Path(entry));
          WALUtils.checkEndOfStream(walNamespace, entry);
          CompletableFuture<Void> renameFuture =
            walNamespace.renameLog(entry, WALUtils.pathToDistributedLogName(newPath));
          try {
            renameFuture.join();
            if (LOG.isDebugEnabled()) {
              LOG.debug("Archived meta log " + entry + " to " + newPath);
            }
          } catch (Exception e) {
            LOG.warn("Unable to move  " + entry + " to " + newPath);
          }
        }
      }
      try {
        List<String> splittingLogs = WALUtils.listLogsUnderPath(splitDir, walNamespace);
        if (splittingLogs.isEmpty()) {
          WALUtils.deleteLogsUnderPath(walNamespace, WALUtils.pathToDistributedLogName(splitDir),
            DistributedLogAccessor.getDistributedLogStreamName(conf), true);
        } else {
          LOG.warn("The log path: " + splitDir + " is not empty. Not delete it");
        }
      } catch (IOException ioe) {
        LOG.warn("Unable to delete log dir. Ignoring. " + splitDir, ioe);
      }
    } catch (IOException ie) {
      LOG.warn("Failed archiving meta log for server " + serverName, ie);
    }
  }
}
