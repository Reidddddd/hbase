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

import com.google.protobuf.Service;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
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
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.quotas.RegionServerQuotaManager;
import org.apache.hadoop.hbase.regionserver.CompactionRequestor;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HeapMemoryManager;
import org.apache.hadoop.hbase.regionserver.Leases;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerAccounting;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.ServerNonceManager;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.KeeperException;

/**
 * In a scenario of Replication based Disaster/Recovery, when hbase
 * Master-Cluster crashes, this tool is used to sync-up the delta from Master to
 * Slave using the info from Zookeeper. The tool will run on Master-Cluser, and
 * assume ZK, Filesystem and NetWork still available after hbase crashes
 *
 * hbase org.apache.hadoop.hbase.replication.regionserver.ReplicationSyncUp
 */

public class ReplicationSyncUp extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(ReplicationSyncUp.class.getName());

  private static Configuration conf;

  private static final long SLEEP_TIME = 10000;

  // although the tool is designed to be run on command line
  // this api is provided for executing the tool through another app
  public static void setConfigure(Configuration config) {
    conf = config;
  }

  /**
   * Main program
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    if (conf == null) {
      conf = HBaseConfiguration.create();
    }
    int ret = ToolRunner.run(conf, new ReplicationSyncUp(), args);
    System.exit(ret);
  }

  @Override
  public int run(String[] args) throws Exception {
    Replication replication;
    ReplicationSourceManager manager;
    FileSystem fs;
    Path oldLogDir, logDir, walRootDir;
    ZooKeeperWatcher zkw;

    Abortable abortable = new Abortable() {
      @Override
      public void abort(String why, Throwable e) {
      }

      @Override
      public boolean isAborted() {
        return false;
      }
    };

    zkw =
        new ZooKeeperWatcher(conf, "syncupReplication" + System.currentTimeMillis(), abortable,
            true);

    walRootDir = FSUtils.getWALRootDir(conf);
    fs = FSUtils.getWALFileSystem(conf);
    oldLogDir = new Path(walRootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    logDir = new Path(walRootDir, HConstants.HREGION_LOGDIR_NAME);

    System.out.println("Start Replication Server start");
    replication = new Replication(new DummyRegionServerServices(zkw), fs, logDir, oldLogDir);
    manager = replication.getReplicationManager();
    manager.init();

    try {
      int numberOfOldSource = 1; // default wait once
      while (numberOfOldSource > 0) {
        Thread.sleep(SLEEP_TIME);
        numberOfOldSource = manager.getOldSources().size();
      }
      manager.join();
    } catch (InterruptedException e) {
      System.err.println("didn't wait long enough:" + e);
      return (-1);
    } finally {
      zkw.close();
    }

    return (0);
  }
  
  static class DummyRegionServerServices implements RegionServerServices {
    String hostname;
    ZooKeeperWatcher zkw;
    
    DummyRegionServerServices(ZooKeeperWatcher zkw) {
      // an unique name in case the first run fails
      hostname = System.currentTimeMillis() + ".SyncUpTool.replication.org";
      this.zkw = zkw;
    }
    
    DummyRegionServerServices(String hostname) {
      this.hostname = hostname;
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
      return ServerName.valueOf(hostname, 1234, 1L);
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
    
    @Override
    public void updateRegionFavoredNodesMapping(
        String encodedRegionName, List<HBaseProtos.ServerName> favoredNodes) {
    
    }
    
    @Override
    public InetSocketAddress[] getFavoredNodesForRegion(String encodedRegionName) {
      return new InetSocketAddress[0];
    }
    
    @Override
    public void addToOnlineRegions(Region r) {
    
    }
    
    @Override
    public boolean removeFromOnlineRegions(Region r, ServerName destination) {
      return false;
    }
    
    @Override
    public Region getFromOnlineRegions(String encodedRegionName) {
      return null;
    }
    
    @Override
    public List<Region> getOnlineRegions(TableName tableName) throws IOException {
      return null;
    }
    
    @Override
    public List<Region> getOnlineRegions() {
      return null;
    }
    
    @Override
    public boolean isStopping() {
      return false;
    }
    
    @Override
    public WAL getWAL(HRegionInfo regionInfo) throws IOException {
      return null;
    }
  
    @Override
    public List<WAL> getWALs() throws IOException {
      return Collections.emptyList();
    }
  
    @Override
    public CompactionRequestor getCompactionRequester() {
      return null;
    }
    
    @Override
    public FlushRequester getFlushRequester() {
      return null;
    }
    
    @Override
    public RegionServerAccounting getRegionServerAccounting() {
      return null;
    }
    
    @Override
    public TableLockManager getTableLockManager() {
      return null;
    }
    
    @Override
    public RegionServerQuotaManager getRegionServerQuotaManager() {
      return null;
    }
    
    @Override
    public void postOpenDeployTasks(
        PostOpenDeployContext context) throws KeeperException, IOException {
    
    }
    
    @Override
    public void postOpenDeployTasks(Region r) throws KeeperException, IOException {
    
    }
    
    @Override
    public boolean reportRegionStateTransition(RegionStateTransitionContext context) {
      return false;
    }
    
    @Override
    public boolean reportRegionStateTransition(
        TransitionCode code, long openSeqNum, HRegionInfo... hris) {
      return false;
    }
    
    @Override
    public boolean reportRegionStateTransition(TransitionCode code, HRegionInfo... hris) {
      return false;
    }
    
    @Override
    public RpcServerInterface getRpcServer() {
      return null;
    }
    
    @Override
    public ConcurrentMap<byte[], Boolean> getRegionsInTransitionInRS() {
      return null;
    }
    
    @Override
    public FileSystem getFileSystem() {
      return null;
    }
    
    @Override
    public Leases getLeases() {
      return null;
    }
    
    @Override
    public ExecutorService getExecutorService() {
      return null;
    }
    
    @Override
    public Map<String, Region> getRecoveringRegions() {
      return null;
    }
    
    @Override
    public ServerNonceManager getNonceManager() {
      return null;
    }
    
    @Override
    public boolean registerService(Service service) {
      return false;
    }
    
    @Override
    public HeapMemoryManager getHeapMemoryManager() {
      return null;
    }
    
    @Override
    public double getCompactionPressure() {
      return 0;
    }
    
    @Override
    public Set<TableName> getOnlineTables() {
      return null;
    }
    
    @Override
    public ThroughputController getFlushThroughputController() {
      return null;
    }
    
    @Override
    public double getFlushPressure() {
      return 0;
    }
    
    @Override
    public MetricsRegionServer getMetrics() {
      return null;
    }
    
    @Override
    public void unassign(byte[] regionName) throws IOException {
    
    }
  }
}
