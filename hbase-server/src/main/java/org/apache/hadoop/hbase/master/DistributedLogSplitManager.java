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

import dlshade.org.apache.distributedlog.api.DistributedLogManager;
import dlshade.org.apache.distributedlog.api.namespace.Namespace;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SplitLogCounters;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.wal.DistributedLogAccessor;
import org.apache.hadoop.hbase.wal.WALUtils;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class DistributedLogSplitManager extends SplitLogManager {
  private static final Log LOG = LogFactory.getLog(DistributedLogSplitManager.class);

  private final Namespace walNamespace;

  /**
   * Its OK to construct this object even when region-servers are not online. It does lookup the
   * orphan tasks in coordination engine but it doesn't block waiting for them to be done.
   *
   * @param server     the server instance
   * @param conf       the HBase configuration
   * @param stopper    the stoppable in case anything is wrong
   * @param master     the master services
   * @param serverName the master server name
   */
  public DistributedLogSplitManager(Server server, Configuration conf, Stoppable stopper,
      MasterServices master, ServerName serverName) throws IOException {
    super(server, conf, stopper, master, serverName);
    try {
      walNamespace = DistributedLogAccessor.getInstance(conf).getNamespace();
    } catch (Exception e) {
      LOG.error("Failed accessing DistributedLog \n", e);
      throw new IOException(e);
    }
  }

  @Override
  protected long preQueueSplitTask(final Set<ServerName> serverNames, final List<Path> logDirs,
      PathFilter filter, TaskBatch batch) throws IOException {
    long totalSize = 0;
    Path[] logs = getLogList(conf, logDirs, filter, walNamespace);
    LOG.info("Started splitting " + logs.length + " logs in " + logDirs +
      " for " + serverNames);
    DistributedLogManager dlm = null;
    for (Path log : logs) {
      // TODO If the log file is still being written to - which is most likely
      // the case for the last log file - then its length will show up here
      // as zero. The size of such a file can only be retrieved after
      // recover-lease is done. totalSize will be under in most cases and the
      // metrics that it drives will also be under-reported.
      String logStr = WALUtils.pathToDistributedLogName(log);
      if (!walNamespace.logExists(logStr)) {
        // Ignore non-exist logs.
        continue;
      }
      // Check empty log.
      dlm = walNamespace.openLog(logStr);
      WALUtils.checkEndOfStream(dlm);
      if (dlm.getLogRecordCount() < 1) {
        // Remove the empty log.
        dlm.delete();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Remove empty log: " + logStr);
        }
        continue;
      }
      totalSize += dlm.getLastTxId();
      dlm.close();
      LOG.info("Start enqueue split task for log: " + logStr);
      if (!enqueueSplitTask(logStr, batch)) {
        throw new IOException("duplicate log split scheduled for " + logStr);
      }
    }
    return totalSize;
  }

  @Override
  protected void cleanupLogs(final List<Path> logDirs, MonitoredTask status) {
    for (Path logDir : logDirs) {
      status.setStatus("Cleaning up log directory...");
      try {
        String logDirStr = WALUtils.pathToDistributedLogName(logDir);
        List<String> logs = WALUtils.listLogs(walNamespace, logDir, null);
        if (logs.isEmpty()) {
          WALUtils.deleteLogsUnderPath(walNamespace, logDirStr,
            DistributedLogAccessor.getDistributedLogStreamName(conf), true);
        } else {
          LOG.warn("The cleaned up path is not empty. Ignoring delete it");
        }
      } catch (IOException ioe) {
        LOG.warn("Unable to delete log src dir. Ignoring. " + logDir, ioe);
      }
      SplitLogCounters.tot_mgr_log_split_batch_success.incrementAndGet();
    }
  }

  /**
   * The caller will block until all the log files of the given region server have been processed -
   * successfully split or an error is encountered - by an available worker region server. This
   * method must only be called after the region servers have been brought online.
   * @param logDirs List of log dirs to split
   * @throws IOException If there was an error while splitting any log file
   * @return cumulative size of the logfiles split
   */
  @Override
  public long splitLogDistributed(final List<Path> logDirs) throws IOException {
    if (logDirs.isEmpty()) {
      return 0;
    }
    Set<ServerName> serverNames = new HashSet<ServerName>();
    for (Path logDir : logDirs) {
      try {
        ServerName serverName = WALUtils.getServerNameFromWALDirectoryName(logDir);
        if (serverName != null) {
          serverNames.add(serverName);
        }
      } catch (IllegalArgumentException e) {
        // ignore invalid format error.
        LOG.warn("Cannot parse server name from " + logDir);
      }
    }
    return splitLogDistributed(serverNames, logDirs, null);
  }
}
