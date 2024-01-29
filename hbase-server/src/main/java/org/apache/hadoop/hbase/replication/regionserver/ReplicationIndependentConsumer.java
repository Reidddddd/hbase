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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationFactory;
import org.apache.hadoop.hbase.replication.ReplicationQueues;
import org.apache.hadoop.hbase.replication.ReplicationType;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.CreateMode;

/**
 * An replication process. It is used to replicate data from master cluster to sinker,
 * and could be deployed independently outside regionserver.
 *
 * Currently, each process can replicate the data of one regionserver, so it is necessary to
 * ensure that the number of nodes of this service is greater than regionservers that need to
 * be replicated.
 *
 * Use zookeeper to implement distributed locks to ensure that a regionserver will only be
 * processed by one consumer.
 */
@InterfaceAudience.Private
public class ReplicationIndependentConsumer extends Configured implements Tool, Abortable,
  Stoppable {

  private static final Log LOG = LogFactory.getLog(ReplicationIndependentConsumer.class);

  private static Configuration conf;

  private Replication replication;
  private ReplicationQueues replicationQueues;
  private ReplicationIndependentSourceManager manager;
  private FileSystem fs;
  private Path oldLogDir, logDir, walRootDir;
  private ZooKeeperWatcher zkw;
  private String consumeRS;

  // although the tool is designed to be run on command line
  // this api is provided for executing the tool through another app
  public static void setConfigure(Configuration config) {
    conf = config;
  }

  /**
   * Main program
   * @param args args
   * @throws Exception Exception
   */
  public static void main(String[] args) throws Exception {
    try {
      if (conf == null) {
        conf = HBaseConfiguration.create();
      }
      int ret = ToolRunner.run(conf, new ReplicationIndependentConsumer(), args);
      if (ret != 0) {
        System.exit(ret);
      }
    } catch (Exception e) {
      LOG.error("Failed to run", e);
      System.exit(-1);
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Abortable abortable = new Abortable() {
      @Override
      public void abort(String why, Throwable e) {
      }

      @Override
      public boolean isAborted() {
        return false;
      }
    };

    zkw = new ZooKeeperWatcher(conf,
      "ReplicationIndependentConsumer" + System.currentTimeMillis(), abortable, true);

    walRootDir = FSUtils.getWALRootDir(conf);
    fs = FSUtils.getWALFileSystem(conf);
    oldLogDir = new Path(walRootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    logDir = new Path(walRootDir, HConstants.HREGION_LOGDIR_NAME);

    LOG.info("Start to init replication consumer.");
    init();
    LOG.info("Init replication consumer done, current consuming regionserver is " + consumeRS);
    return 0;
  }

  private void init() throws InterruptedException {
    boolean initSuccessful = false;
    int sleepTime = 10 * 1000;
    while (!initSuccessful) {
      try {
        tryGetRSReplicationQueue();
        replication = new Replication(new DummyServer(zkw, consumeRS), fs, logDir, oldLogDir,
          ReplicationType.INDEPENDENT);
        manager = (ReplicationIndependentSourceManager) replication.getReplicationManager();
        manager.onRegionServerMoved(() -> {
          manager.join();
          releaseRSReplicationQueueLock();
          try {
            init();
          } catch (InterruptedException e) {
            LOG.info("Consumer interrupted after RS moved.", e);
            System.exit(0);
          }
        });
        manager.init();
      } catch (ReplicationException | IOException e) {
        LOG.error("init replication failed, sleep " + sleepTime + " and retry.", e);
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException ex) {
          throw new RuntimeException(ex);
        }
        sleepTime = sleepTime >= 60 * 10 * 1000 ? 600000 : sleepTime * 2;
        continue;
      }
      initSuccessful = true;
    }
  }

  private void releaseRSReplicationQueueLock() {
    if (consumeRS != null && consumeRS.length() > 0) {
      replicationQueues =
        ReplicationFactory.getReplicationQueues(
          zkw, this.conf, this, fs, oldLogDir, ReplicationType.INDEPENDENT);
      replicationQueues.unlockOtherRS(consumeRS);
      LOG.info("Released lock on " + consumeRS);
      consumeRS = null;
    }
  }

  private void tryGetRSReplicationQueue() throws ReplicationException, InterruptedException {
    LOG.info("Start to lock and get replication queue.");
    replicationQueues =
      ReplicationFactory.getReplicationQueues(
        zkw, this.conf, this, fs, oldLogDir, ReplicationType.INDEPENDENT);
    List<String> currentReplicators = replicationQueues.getListOfReplicators();
    loop :
    while (consumeRS == null || consumeRS.length() == 0) {
      if (currentReplicators != null && !currentReplicators.isEmpty()) {
        LOG.info("Found " + currentReplicators.size() + " regionservers.");
        for (String serverName : currentReplicators) {
          List<String> queuesOfReplicator =
            replicationQueues.getAllQueuesOfReplicator(serverName);
          if (queuesOfReplicator == null || queuesOfReplicator.size() == 0) {
            LOG.info("Queue of " + serverName + " is empty, try to find another one");
            continue;
          }
          if (replicationQueues.lockOtherRS(serverName, CreateMode.EPHEMERAL)) {
            consumeRS = serverName;
            LOG.info("Locked " + consumeRS);
            break loop;
          }
        }
      } else {
        LOG.info("Not found any regionservers, will retry after 10s...");
      }
      try {
        Thread.sleep(10 * 1000);
      } catch (InterruptedException e) {
        throw e;
      }
      currentReplicators = replicationQueues.getListOfReplicators();
    }
  }

  @VisibleForTesting
  public boolean isLockedRS() {
    return consumeRS != null && consumeRS.length() > 0;
  }

  @Override
  public void abort(String why, Throwable e) {

  }

  @Override
  public boolean isAborted() {
    return false;
  }

  @Override
  public void stop(String why) {
    if (manager != null) {
      LOG.info("Stop replication independent consumer: " + why + ", " + consumeRS + ", " +
        manager);
      manager.join();
      releaseRSReplicationQueueLock();
    }
  }

  @Override
  public boolean isStopped() {
    return false;
  }

  static class DummyServer implements Server {
    ServerName serverName;
    ZooKeeperWatcher zkw;

    DummyServer(ZooKeeperWatcher zkw, String serverName) {
      this.serverName = ServerName.valueOf(serverName);
      this.zkw = zkw;
    }

    @Override
    public Configuration getConfiguration() {
      return conf;
    }

    @Override
    public ZooKeeperWatcher getZooKeeper() {
      return zkw;
    }

    @Override
    public CoordinatedStateManager getCoordinatedStateManager() {
      return null;
    }

    @Override
    public MetaTableLocator getMetaTableLocator() {
      return null;
    }

    @Override
    public ServerName getServerName() {
      return serverName;
    }

    @Override
    public void abort(String why, Throwable e) {
    }

    @Override
    public boolean isAborted() {
      return false;
    }

    @Override
    public void stop(String why) {
    }

    @Override
    public boolean isStopped() {
      return false;
    }

    @Override
    public ClusterConnection getConnection() {
      return null;
    }

    @Override
    public ChoreService getChoreService() {
      return null;
    }
  }
}
