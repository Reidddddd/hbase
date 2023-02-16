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
package org.apache.hadoop.hbase.master;

import static org.apache.hadoop.hbase.master.MasterFileSystem.HBASE_WAL_DIR_PERMS;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.apache.hadoop.hbase.wal.WALUtils;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class SplitLogHelper extends AbstractSplitLogHelper {
  private static final Log LOG = LogFactory.getLog(SplitLogHelper.class);

  private final FileSystem walFS;


  // Is the fileystem ok?
  private volatile boolean walFsOk = true;

  public SplitLogHelper(MasterFileSystem masterFileSystem) throws IOException {
    super(masterFileSystem);
    this.walFS = FSUtils.getWALFileSystem(conf);
    this.walFS.setConf(conf);
    FSUtils.setFsDefault(conf, new Path(this.walFS.getUri()));
  }

  @Override
  Path createInitialFileSystemLayout() throws IOException {
    this.masterFileSystem.checkRootDir(this.walRootDir, conf, this.walFS,
      HFileSystem.HBASE_WAL_DIR, HBASE_WAL_DIR_PERMS);
    Path oldLogDir = new Path(this.walRootDir, HConstants.HREGION_OLDLOGDIR_NAME);

    // Make sure the region servers can archive their old logs
    if(!this.walFS.exists(oldLogDir)) {
      this.walFS.mkdirs(oldLogDir);
    }
    return oldLogDir;
  }

  @Override
  Set<ServerName> getFailedServersFromLogFolders() {
    boolean retrySplitting = !conf.getBoolean("hbase.hlog.split.skip.errors",
      WALSplitter.SPLIT_SKIP_ERRORS_DEFAULT);

    Set<ServerName> serverNames = new HashSet<ServerName>();
    Path logsDirPath = new Path(this.walRootDir, HConstants.HREGION_LOGDIR_NAME);

    do {
      if (this.masterFileSystem.master.isStopped()) {
        LOG.warn("Master stopped while trying to get failed servers.");
        break;
      }
      try {
        if (!this.walFS.exists(logsDirPath)) {
          return serverNames;
        }
        FileStatus[] logFolders = FSUtils.listStatus(this.walFS, logsDirPath, null);
        // Get online servers after getting log folders to avoid log folder deletion of newly
        // checked in region servers . see HBASE-5916
        Set<ServerName> onlineServers = ((HMaster) masterFileSystem.master).getServerManager()
          .getOnlineServers().keySet();

        if (logFolders == null || logFolders.length == 0) {
          LOG.debug("No log files to split, proceeding...");
          return serverNames;
        }
        for (FileStatus status : logFolders) {
          FileStatus[] curLogFiles = FSUtils.listStatus(this.walFS, status.getPath(), null);
          if (curLogFiles == null || curLogFiles.length == 0) {
            // Empty log folder. No recovery needed
            continue;
          }
          final ServerName serverName = WALUtils.getServerNameFromWALDirectoryName(
            status.getPath());
          if (null == serverName) {
            LOG.warn("Log folder " + status.getPath() + " doesn't look like its name includes a " +
              "region server name; leaving in place. If you see later errors about missing " +
              "write ahead logs they may be saved in this location.");
          } else if (!onlineServers.contains(serverName)) {
            LOG.info("Log folder " + status.getPath() + " doesn't belong "
              + "to a known region server, splitting");
            serverNames.add(serverName);
          } else {
            LOG.info("Log folder " + status.getPath() + " belongs to an existing region server");
          }
        }
        retrySplitting = false;
      } catch (IOException ioe) {
        LOG.warn("Failed getting failed servers to be recovered.", ioe);
        if (!checkFileSystem()) {
          LOG.warn("Bad Filesystem, exiting");
          Runtime.getRuntime().halt(1);
        }
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
        Path logDir = new Path(this.walRootDir,
          WALUtils.getWALDirectoryName(serverName.toString()));
        Path splitDir = logDir.suffix(WALUtils.SPLITTING_EXT);
        // Rename the directory so a rogue RS doesn't create more WALs
        if (walFS.exists(logDir)) {
          if (!this.walFS.rename(logDir, splitDir)) {
            throw new IOException("Failed fs.rename for log split: " + logDir);
          }
          logDir = splitDir;
          LOG.debug("Renamed region directory: " + splitDir);
        } else if (!walFS.exists(splitDir)) {
          LOG.info("Log dir for server " + serverName + " does not exist");
          continue;
        }
        logDirs.add(splitDir);
      }
    } catch (IOException ioe) {
      if (!checkFileSystem()) {
        this.masterFileSystem.services.abort("Aborting due to filesystem unavailable", ioe);
        throw ioe;
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
      Path logDir = new Path(this.walRootDir,
        WALUtils.getWALDirectoryName(serverName.toString()));
      Path splitDir = logDir.suffix(WALUtils.SPLITTING_EXT);
      if (walFS.exists(splitDir)) {
        FileStatus[] logfiles = FSUtils.listStatus(walFS, splitDir, MasterFileSystem.META_FILTER);
        if (logfiles != null) {
          for (FileStatus status : logfiles) {
            if (!status.isDir()) {
              Path newPath = WALUtils.getWALArchivePath(this.masterFileSystem.getOldLogDir(),
                status.getPath());
              if (!FSUtils.renameAndSetModifyTime(walFS, status.getPath(), newPath)) {
                LOG.warn("Unable to move  " + status.getPath() + " to " + newPath);
              } else {
                LOG.debug("Archived meta log " + status.getPath() + " to " + newPath);
              }
            }
          }
        }
        if (!walFS.delete(splitDir, false)) {
          LOG.warn("Unable to delete log dir. Ignoring. " + splitDir);
        }
      }
    } catch (IOException ie) {
      LOG.warn("Failed archiving meta log for server " + serverName, ie);
    }
  }

  /**
   * Checks to see if the file system is still accessible.
   * If not, sets closed
   * @return false if file system is not available
   */
  boolean checkFileSystem() {
    if (this.walFsOk) {
      try {
        FSUtils.checkFileSystemAvailable(this.walFS);
        FSUtils.checkDfsSafeMode(this.conf);
      } catch (IOException e) {
        masterFileSystem.master.abort("Shutting down HBase cluster: file system not available", e);
        this.walFsOk = false;
      }
    }
    return this.walFsOk;
  }
}
