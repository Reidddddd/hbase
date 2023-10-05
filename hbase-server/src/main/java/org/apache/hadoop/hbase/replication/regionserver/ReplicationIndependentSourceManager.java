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
package org.apache.hadoop.hbase.replication.regionserver;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueues;
import org.apache.hadoop.hbase.replication.ReplicationTracker;

/**
 *The main difference from ReplicationSourceManager is this class uses
 * ReplicationIndependentConsumerQueueChecker to get new wal files and enqueue them,
 * similar to ReplicationSourceManager#postLogRoll.
 */
public class ReplicationIndependentSourceManager extends ReplicationSourceManager {
  private static final Log LOG =
    LogFactory.getLog(ReplicationIndependentSourceManager.class);

  ReplicationIndependentConsumerQueueChecker queueChecker;
  private static ThreadPoolExecutor QUEUE_CHECKER_EXECUTOR = null;
  private Runnable onRegionServerMoved;

  /**
   * Creates a replication manager and sets the watch on all the other registered region servers
   *
   * @param replicationQueues  the interface for manipulating replication queues
   * @param replicationPeers   the interface for manipulating replication peers
   * @param replicationTracker the interface for tracking replication events
   * @param conf               the configuration to use
   * @param server             the server for this region server
   * @param fs                 the file system to use
   * @param logDir             the directory that contains all wal directories of live RSs
   * @param oldLogDir          the directory where old logs are archived
   * @param clusterId          unique UUID for the cluster
   */
  public ReplicationIndependentSourceManager(
    ReplicationQueues replicationQueues,
    ReplicationPeers replicationPeers,
    ReplicationTracker replicationTracker,
    Configuration conf, Server server,
    FileSystem fs, Path logDir,
    Path oldLogDir, UUID clusterId) {
    super(replicationQueues, replicationPeers, replicationTracker, conf, server, fs, logDir,
      oldLogDir, clusterId, false);
    if (QUEUE_CHECKER_EXECUTOR == null) {
      QUEUE_CHECKER_EXECUTOR = (ThreadPoolExecutor) Executors.newFixedThreadPool(1,
        new ThreadFactoryBuilder()
          .setNameFormat("ReplicationIndependentConsumerQueueCheckerExecutor-%d")
          .setDaemon(true)
          .build()
      );
    }
  }

  @Override
  protected void init() throws IOException, ReplicationException {
    for (String id : this.replicationPeers.getPeerIds()) {
      MetricsSource metrics = new MetricsSource(id);
      this.sourcesMetrics.put(id, metrics);
      this.sourcesPeerRunningStatus.put(id, PeerRunningStatus.RUNNING);
      metrics.setPeerRunning();
      addPeerSource(id, metrics);
    }

    queueChecker = new ReplicationIndependentConsumerQueueChecker(
      replicationQueues, getLatestPaths(), getSources(), walsById);
    QUEUE_CHECKER_EXECUTOR.execute(queueChecker);

    if (enableFailover) {
      tryHandleFailoverWork();
    }
  }

  @Override
  public void join() {
    CompletableFuture<Boolean> stopFuture = queueChecker.terminate();
    queueChecker.interrupt();
    try {
      LOG.info("try to terminate queueChecker...");
      if(!stopFuture.get(30 * 1000, TimeUnit.MILLISECONDS)) {
        RuntimeException e = new RuntimeException();
        LOG.fatal("Cannot terminate queueChecker, crash myself.", e);
        throw e;
      }
    } catch (Exception e) {
      LOG.fatal("Cannot terminate queueChecker, crash myself.", e);
      throw new RuntimeException(e);
    }
    super.join();
    LOG.info("Independent source manager stopped.");
  }

  @Override
  public void regionServerRemoved(String regionserver) {
    if (replicationQueues.isThisOurZnode(regionserver)) {
      LOG.info("My znode moved, stop consuming: " + regionserver);
      onRegionServerMoved.run();
    } else {
      super.regionServerRemoved(regionserver);
    }
  }

  public void onRegionServerMoved(Runnable runnable) {
    this.onRegionServerMoved = runnable;
  }
}
