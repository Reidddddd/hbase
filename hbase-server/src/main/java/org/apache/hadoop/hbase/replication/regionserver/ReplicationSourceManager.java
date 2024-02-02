/*
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

package org.apache.hadoop.hbase.replication.regionserver;

import static org.apache.hadoop.hbase.HConstants.REPLICATION_SOURCE_ONLY_PRODUCE_DEFAULT;
import static org.apache.hadoop.hbase.HConstants.REPLICATION_SOURCE_ONLY_PRODUCE_KEY;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerCoprocessorHost;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationListener;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationPeer.PeerState;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.replication.ReplicationQueues;
import org.apache.hadoop.hbase.replication.ReplicationTracker;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This class is responsible to manage all the replication
 * sources. There are two classes of sources:
 * <ul>
 * <li> Normal sources are persistent and one per peer cluster</li>
 * <li> Old sources are recovered from a failed region server and our
 * only goal is to finish replicating the WAL queue it had up in ZK</li>
 * </ul>
 *
 * When a region server dies, this class uses a watcher to get notified and it
 * tries to grab a lock in order to transfer all the queues in a local
 * old source.
 *
 * This class implements the ReplicationListener interface so that it can track changes in
 * replication state.
 */
@InterfaceAudience.Private
public class ReplicationSourceManager implements ReplicationListener, ConfigurationObserver {
  private static final Log LOG =
      LogFactory.getLog(ReplicationSourceManager.class);
  private static final String REPLICATION_ENABLE_FAILOVER_KEY =
    "replication.source.rs.enable.failover";
  private static final boolean REPLICATION_ENABLE_FAILOVER_DEFAULT_VALUE = true;
  // Map of all the sources that read this RS's logs
  private final Map<String, ReplicationSourceInterface> sources;
  // List of all the sources we got from died RSs
  private final List<ReplicationSourceInterface> oldsources;
  protected final ReplicationQueues replicationQueues;
  private final ReplicationTracker replicationTracker;
  protected final ReplicationPeers replicationPeers;
  // UUID for this cluster
  private final UUID clusterId;
  // All about stopping
  protected final Server server;
  // All logs we are currently tracking
  // Index structure of the map is: peer_id->logPrefix/logGroup->logs
  protected final Map<String, Map<String, SortedSet<String>>> walsById;
  // Logs for recovered sources we are currently tracking
  private final Map<String, Map<String, SortedSet<String>>> walsByIdRecoveredQueues;
  private final Configuration conf;
  private final FileSystem fs;
  // The paths to the latest log of each wal group, for new coming peers
  private final Set<Path> latestPaths;
  // Path to the wals directories
  private final Path logDir;
  // Path to the wal archive
  private final Path oldLogDir;
  // The number of ms that we wait before moving znodes, HBASE-3596
  private final long sleepBeforeFailover;
  // Homemade executor service for replication failover
  private final ThreadPoolExecutor failoverExecutor;
  // Homemade executor service for replication clean source
  private final ThreadPoolExecutor cleanSourceExecutor;

  private final Random rand;
  private final boolean replicationForBulkLoadDataEnabled;
  private final boolean isSyncUp;
  // Map to record the online region count in this server for the tables in this
  private final Map<TableName, AtomicInteger> tableOnlineRegionCount;
  // The paths that need to be consumed for the source
  private final Map<String, Set<Path>> sourcesWaitingDrainPaths;
  // Map to record all the peer replication table
  private final Set<TableName> peerReplicationTables;
  // Map to record all the peer consume status for all the sources
  protected final Map<String, PeerRunningStatus> sourcesPeerRunningStatus;
  // Map to record all the metrics for all the sources
  protected final Map<String, MetricsSource> sourcesMetrics;
  protected volatile boolean enableFailover;

  @Override
  public void onConfigurationChange(Configuration conf) {
    boolean currentEnableFailover = this.enableFailover;
    this.enableFailover = conf.getBoolean(REPLICATION_ENABLE_FAILOVER_KEY,
      REPLICATION_ENABLE_FAILOVER_DEFAULT_VALUE);
    LOG.info("enableFailover changed from " + currentEnableFailover + " to " + enableFailover);
    if (enableFailover && !currentEnableFailover) {
      try {
        tryHandleFailoverWork();
      } catch (ReplicationException e) {
        LOG.warn("Cannot get list of replicators, ", e);
      }
    }
  }

  enum PeerRunningStatus {
    // When all the related table is offline and the waiting for drain log is consumed
    // then remove the source and related information mark the peer as not running
    NOT_RUNNING,
    // Opposite of NOT_RUNNING state
    RUNNING
  }


  /**
   * Creates a replication manager and sets the watch on all the other registered region servers
   * @param replicationQueues the interface for manipulating replication queues
   * @param replicationPeers the interface for manipulating replication peers
   * @param replicationTracker the interface for tracking replication events
   * @param conf the configuration to use
   * @param server the server for this region server
   * @param fs the file system to use
   * @param logDir the directory that contains all wal directories of live RSs
   * @param oldLogDir the directory where old logs are archived
   * @param clusterId unique UUID for the cluster
   */
  public ReplicationSourceManager(final ReplicationQueues replicationQueues,
      final ReplicationPeers replicationPeers, final ReplicationTracker replicationTracker,
      final Configuration conf, final Server server, final FileSystem fs, final Path logDir,
      final Path oldLogDir, final UUID clusterId, final boolean isSyncUp) {
    // ConcurrentHashMap is thread-safe.
    // Generally, reading is more than modifying.
    this.sources = new ConcurrentHashMap<String, ReplicationSourceInterface>();
    this.replicationQueues = replicationQueues;
    this.replicationPeers = replicationPeers;
    this.replicationTracker = replicationTracker;
    this.server = server;
    this.walsById = new ConcurrentHashMap<String, Map<String, SortedSet<String>>>();
    this.walsByIdRecoveredQueues = new ConcurrentHashMap<String, Map<String, SortedSet<String>>>();
    this.oldsources = new CopyOnWriteArrayList<ReplicationSourceInterface>();
    this.conf = conf;
    this.fs = fs;
    this.logDir = logDir;
    this.oldLogDir = oldLogDir;
    this.enableFailover = conf.getBoolean(REPLICATION_ENABLE_FAILOVER_KEY,
      REPLICATION_ENABLE_FAILOVER_DEFAULT_VALUE);
    this.sleepBeforeFailover =
        conf.getLong("replication.sleep.before.failover", 30000); // 30 seconds
    this.clusterId = clusterId;
    this.isSyncUp = isSyncUp;
    this.replicationTracker.registerListener(this);
    this.replicationPeers.getAllPeerIds();
    // It's preferable to failover 1 RS at a time, but with good zk servers
    // more could be processed at the same time.
    int nbWorkers = conf.getInt("replication.executor.workers", 1);
    // use a short 100ms sleep since this could be done inline with a RS startup
    // even if we fail, other region servers can take care of it
    this.failoverExecutor = new ThreadPoolExecutor(nbWorkers, nbWorkers,
        100, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(),
        new ThreadFactoryBuilder()
            .setNameFormat("ReplicationFailoverExecutor-%d")
            .setDaemon(true)
            .build()
    );
    // CleanSourceExecutor is newFixedThreadPool with only 1 thread
    // used to execute the cleanWaitingDrainSource tasks.
    // Since there aren't many cleanWaitingDrainSource tasks,
    // the corePoolSize and maximumPoolSize can be 1.
    this.cleanSourceExecutor =  (ThreadPoolExecutor) Executors.newFixedThreadPool(1,
        new ThreadFactoryBuilder()
            .setNameFormat("ReplicationCleanSourceExecutor-%d")
            .setDaemon(true)
            .build()
    );
    this.rand = new Random();
    this.latestPaths = Collections.synchronizedSet(new HashSet<Path>());
    replicationForBulkLoadDataEnabled =
        conf.getBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY,
          HConstants.REPLICATION_BULKLOAD_ENABLE_DEFAULT);
    this.tableOnlineRegionCount = new ConcurrentHashMap<>();
    this.sourcesWaitingDrainPaths = new ConcurrentHashMap<String, Set<Path>>();
    this.peerReplicationTables = Collections.synchronizedSet(new HashSet<TableName>());
    this.sourcesPeerRunningStatus = new ConcurrentHashMap<String, PeerRunningStatus>();
    this.sourcesMetrics = new ConcurrentHashMap<String, MetricsSource>();
    if (this.server instanceof HRegionServer) {
      this.server.getChoreService()
          .scheduleChore(new ReplicationSourceCleanerChore(this.server, this));
    }
  }

  /**
   * Provide the id of the peer and a log key and this method will figure which
   * wal it belongs to and will log, for this region server, the current
   * position. It will also clean old logs from the queue.
   * @param log Path to the log currently being replicated from
   *   replication status in zookeeper. It will also delete older entries.
   * @param id id of the replication queue
   * @param position current location in the log
   * @param queueRecovered indicates if this queue comes from another region server
   * @param holdLogInZK if true then the log is retained in ZK
   */
  public synchronized void logPositionAndCleanOldLogs(Path log, String id, long position,
    boolean queueRecovered, boolean holdLogInZK) {
    String fileName = log.getName();
    this.replicationQueues.setLogPosition(id, fileName, position);
    if (holdLogInZK) {
      return;
    }
    cleanOldLogs(fileName, id, queueRecovered);
  }

  /**
   * Cleans a log file and all older files from ZK. Called when we are sure that a
   * log file is closed and has no more entries.
   * @param key Path to the log
   * @param id id of the peer cluster
   * @param queueRecovered Whether this is a recovered queue
   */
  public void cleanOldLogs(String key, String id, boolean queueRecovered) {
    String logPrefix = DefaultWALProvider.getWALPrefixFromWALName(key);
    if (queueRecovered) {
      Map<String, SortedSet<String>> walsForPeer = walsByIdRecoveredQueues.get(id);
      if(walsForPeer != null) {
        SortedSet<String> wals = walsForPeer.get(logPrefix);
        if (wals != null && !wals.first().equals(key)) {
          cleanOldLogs(wals, key, id);
        }
      }
    } else {
      synchronized (this.walsById) {
        SortedSet<String> wals = walsById.get(id).get(logPrefix);
        if (wals != null && !wals.first().equals(key)) {
          cleanOldLogs(wals, key, id);
        }
      }
    }
  }

  private void cleanOldLogs(SortedSet<String> wals, String key, String id) {
    SortedSet<String> walSet = wals.headSet(key);
    LOG.debug("Removing " + walSet.size() + " logs in the list: " + walSet);
    for (String wal : walSet) {
      LOG.debug("Removing " + wal + " for peer: " + id);
      this.replicationQueues.removeLog(id, wal);
    }
    walSet.clear();
  }

  /**
   * Adds a normal source per registered peer cluster and tries to process all
   * old region server wal queues
   */
  protected void init() throws IOException, ReplicationException {
    for (String id : this.replicationPeers.getPeerIds()) {
      MetricsSource metrics = new MetricsSource(id);
      this.sourcesMetrics.put(id ,metrics);
      // When init source manager only add source that the tableCFs is empty
      Map<TableName, List<String>> tableCFs = this.replicationPeers.getTableCFs(id);
      if (tableCFs == null || tableCFs.isEmpty()) {
        this.sourcesPeerRunningStatus.put(id, PeerRunningStatus.RUNNING);
        metrics.setPeerRunning();
        addPeerSource(id, metrics);
      } else {
        this.sourcesPeerRunningStatus.put(id, PeerRunningStatus.NOT_RUNNING);
        metrics.setPeerNotRunning();
        this.peerReplicationTables.addAll(tableCFs.keySet());
      }
    }
    if (enableFailover) {
      tryHandleFailoverWork();
    }
  }

  protected void tryHandleFailoverWork() throws ReplicationException {
    List<String> currentReplicators = this.replicationQueues.getListOfReplicators();
    if (currentReplicators == null || currentReplicators.isEmpty()) {
      return;
    }
    List<String> otherRegionServers = replicationTracker.getListOfRegionServers();
    LOG.info("Current list of replicators: " + currentReplicators + " other RSs: "
      + otherRegionServers);

    // Look if there's anything to process after a restart
    for (String rs : currentReplicators) {
      if (!otherRegionServers.contains(rs)) {
        transferQueues(rs);
      }
    }
  }

  /**
   * Add sources for the given peer cluster on this region server. For the newly added peer, we only
   * need to enqueue the latest log of each wal group and do replication
   * @param id the id of the peer cluster
   * @param metrics the metricsSource of the peer cluster
   * @return the source that was created
   * @throws IOException If unable to add source
   */
  protected ReplicationSourceInterface addSource(String id, MetricsSource metrics)
      throws IOException, ReplicationException {
    ReplicationPeerConfig peerConfig = replicationPeers.getReplicationPeerConfig(id);
    ReplicationPeer peer = replicationPeers.getPeer(id);
    ReplicationSourceInterface src =
        getReplicationSource(this.conf, this.fs, this, this.replicationQueues,
          this.replicationPeers, server, id, this.clusterId, peerConfig, peer, metrics);
    synchronized (latestPaths) {
      this.sources.put(id, src);
      Map<String, SortedSet<String>> walsByGroup = new HashMap<String, SortedSet<String>>();
      this.walsById.put(id, walsByGroup);
      // Add the latest wal to that source's queue
      if (this.latestPaths.size() > 0) {
        for (Path logPath : latestPaths) {
          String name = logPath.getName();
          String walPrefix = DefaultWALProvider.getWALPrefixFromWALName(name);
          SortedSet<String> logs = new TreeSet<String>();
          logs.add(name);
          walsByGroup.put(walPrefix, logs);
          try {
            this.replicationQueues.initLog(id, name, walPrefix);
          } catch (ReplicationException e) {
            String message =
                "Cannot add log to queue when creating a new source, queueId=" + id
                    + ", filename=" + name;
            server.stop(message);
            throw e;
          }
          src.enqueueLog(logPath);
        }
      }
    }
    src.startup();
    return src;
  }

  /**
   * Delete a complete queue of wals associated with a peer cluster
   * @param peerId Id of the peer cluster queue of wals to delete
   */
  public void deleteSource(String peerId, boolean closeConnection) {
    this.replicationQueues.removeQueue(peerId);
    if (closeConnection) {
      this.replicationPeers.peerRemoved(peerId);
    }
  }

  /**
   * Terminate the replication on this region server
   */
  public void join() {
    this.failoverExecutor.shutdown();
    this.cleanSourceExecutor.shutdown();
    if (this.sources.size() == 0) {
      this.replicationQueues.removeAllQueues();
    }
    for (ReplicationSourceInterface source : this.sources.values()) {
      source.terminate("Region server is closing");
    }
  }

  /**
   * Get a copy of the wals of the first source on this rs
   * @return a sorted set of wal names
   */
  protected Map<String, Map<String, SortedSet<String>>> getWALs() {
    return Collections.unmodifiableMap(walsById);
  }

  /**
   * Get a copy of the wals of the recovered sources on this rs
   * @return a sorted set of wal names
   */
  protected Map<String, Map<String, SortedSet<String>>> getWalsByIdRecoveredQueues() {
    return Collections.unmodifiableMap(walsByIdRecoveredQueues);
  }
  
  /**
   * Get a list of all the replication source metrics of this rs
   * @return list off all metrics of sources
   */
  public List<MetricsSource> getAllMetrics() {
    return new ArrayList<>(this.sourcesMetrics.values());
  }

  /**
   * Get a list of all the normal sources of this rs
   * @return lis of all sources
   */
  public List<ReplicationSourceInterface> getSources() {
    return new ArrayList<>(this.sources.values());
  }

  /**
   * Get a list of all the old sources of this rs
   * @return list of all old sources
   */
  public List<ReplicationSourceInterface> getOldSources() {
    return this.oldsources;
  }
  
  /**
   * Get the normal source for a given peer
   * @param peerId a short that identifies the cluster
   * @return the normal source for the give peer if it exists, otherwise null.
   */
  public ReplicationSourceInterface getSource(String peerId) {
    return this.sources.get(peerId);
  }

  @VisibleForTesting
  List<String> getAllQueues() {
    return replicationQueues.getAllQueues();
  }
  
  void preLogRoll(Path newLog) throws IOException {

  }
  
  /**
   * 1. Check and update the latestPaths and the walsById
   * 2. Check and enqueue the given log to the correct source. If there's still no source for the
   * group to which the given log belongs, create one
   * @param logPath the log path to check and enqueue
   * @throws IOException if unable to add log into replicationQueues
   */
  public void postLogRoll(Path logPath) throws IOException {
    String logName = logPath.getName();
    String logPrefix = DefaultWALProvider.getWALPrefixFromWALName(logName);
    // update replication queues on ZK
    synchronized (latestPaths) {
      // update the latestPaths
      Iterator<Path> iterator = latestPaths.iterator();
      while (iterator.hasNext()) {
        Path path = iterator.next();
        if (path.getName().contains(logPrefix)) {
          iterator.remove();
          break;
        }
      }
      this.latestPaths.add(logPath);
      // update the replicationQueues and enqueue the new log
      for (ReplicationSourceInterface source : this.sources.values()) {
        String peerId = source.getPeerClusterZnode();
        try {
          this.replicationQueues.initLog(peerId, logName, logPrefix);
        } catch (ReplicationException e) {
          throw new IOException("Cannot add log to replication queue"
              + " when creating a new source, queueId=" + peerId + ", filename=" + logName, e);
        }
        if (!conf.getBoolean(REPLICATION_SOURCE_ONLY_PRODUCE_KEY,
          REPLICATION_SOURCE_ONLY_PRODUCE_DEFAULT)) {
          source.enqueueLog(logPath);
        }
      }
      if (!conf.getBoolean(REPLICATION_SOURCE_ONLY_PRODUCE_KEY,
        REPLICATION_SOURCE_ONLY_PRODUCE_DEFAULT)) {
        // update walsById map
        synchronized (walsById) {
          for (Map.Entry<String, Map<String, SortedSet<String>>> entry : this.walsById.entrySet()) {
            String peerId = entry.getKey();
            Map<String, SortedSet<String>> walsByPrefix = entry.getValue();
            boolean existingPrefix = false;
            for (Map.Entry<String, SortedSet<String>> walsEntry : walsByPrefix.entrySet()) {
              SortedSet<String> wals = walsEntry.getValue();
              if (this.sources.isEmpty()) {
                // If there's no slaves, don't need to keep the old wals since
                // we only consider the last one when a new slave comes in
                wals.clear();
              }
              if (logPrefix.equals(walsEntry.getKey())) {
                wals.add(logName);
                existingPrefix = true;
              }
            }
            if (!existingPrefix) {
              // The new log belongs to a new group, add it into this peer
              LOG.debug("Start tracking logs for wal group " + logPrefix + " for peer " + peerId);
              SortedSet<String> wals = new TreeSet<String>();
              wals.add(logName);
              walsByPrefix.put(logPrefix, wals);
            }
          }
        }
      }
    }
  }
  
  /**
   * Factory method to create a replication source
   * @param conf the configuration to use
   * @param fs the file system to use
   * @param manager the manager to use
   * @param server the server object for this region server
   * @param peerId the id of the peer cluster
   * @return the created source
   * @throws IOException if unable to pass replication endpoint implementation
   */
  protected ReplicationSourceInterface getReplicationSource(final Configuration conf,
      final FileSystem fs, final ReplicationSourceManager manager,
      final ReplicationQueues replicationQueues, final ReplicationPeers replicationPeers,
      final Server server, final String peerId, final UUID clusterId,
      final ReplicationPeerConfig peerConfig, final ReplicationPeer replicationPeer,
      final MetricsSource metrics) throws IOException {
    RegionServerCoprocessorHost rsServerHost = null;
    TableDescriptors tableDescriptors = null;
    if (server instanceof HRegionServer) {
      rsServerHost = ((HRegionServer) server).getRegionServerCoprocessorHost();
      tableDescriptors = ((HRegionServer) server).getTableDescriptors();
    }
    ReplicationSourceInterface src;
    try {
      @SuppressWarnings("rawtypes")
      Class c = Class.forName(conf.get("replication.replicationsource.implementation",
          ReplicationSource.class.getCanonicalName()));
      src = (ReplicationSourceInterface) c.newInstance();
    } catch (Exception e) {
      LOG.warn("Passed replication source implementation throws errors, " +
          "defaulting to ReplicationSource", e);
      src = new ReplicationSource();
    }

    ReplicationEndpoint replicationEndpoint = null;
    try {
      String replicationEndpointImpl = peerConfig.getReplicationEndpointImpl();
      if (replicationEndpointImpl == null) {
        // Default to HBase inter-cluster replication endpoint
        replicationEndpointImpl = HBaseInterClusterReplicationEndpoint.class.getName();
      }
      @SuppressWarnings("rawtypes")
      Class c = Class.forName(replicationEndpointImpl);
      replicationEndpoint = (ReplicationEndpoint) c.newInstance();
      if(rsServerHost != null) {
        ReplicationEndpoint newReplicationEndPoint = rsServerHost
            .postCreateReplicationEndPoint(replicationEndpoint);
        if(newReplicationEndPoint != null) {
          // Override the newly created endpoint from the hook with configured end point
          replicationEndpoint = newReplicationEndPoint;
        }
      }
    } catch (Exception e) {
      LOG.warn("Passed replication endpoint implementation throws errors"
          + " while initializing ReplicationSource for peer: " + peerId, e);
      throw new IOException(e);
    }

    // init replication source
    src.init(conf, fs, manager, replicationQueues, replicationPeers, server, peerId,
      clusterId, replicationEndpoint, metrics);

    // init replication endpoint
    replicationEndpoint.init(new ReplicationEndpoint.Context(
        conf, replicationPeer.getConfiguration(), fs, peerId,
        clusterId, replicationPeer, metrics, tableDescriptors, server));

    return src;
  }
  
  /**
   * Transfer all the queues of the specified to this region server.
   * First it tries to grab a lock and if it works it will move the
   * znodes and finally will delete the old znodes.
   *
   * It creates one old source for any type of source of the old rs.
   * @param rsZnode region server znode
   */
  private void transferQueues(String rsZnode) {
    NodeFailoverWorker transfer =
        new NodeFailoverWorker(rsZnode, this.replicationQueues, this.replicationPeers,
            this.clusterId, this.isSyncUp);
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Start up replication queues transfer of " + rsZnode);
      }
      this.failoverExecutor.execute(transfer);
    } catch (RejectedExecutionException ex) {
      CompatibilitySingletonFactory.getInstance(MetricsReplicationSourceFactory.class)
        .getGlobalSource().incrFailedRecoveryQueue();
      LOG.info("Cancelling the transfer of " + rsZnode + " because of " + ex.getMessage());
    }
  }

  /**
   * Clear the references to the specified old source
   * @param src source to clear
   */
  public void closeRecoveredQueue(ReplicationSourceInterface src) {
    LOG.info("Done with the recovered queue " + src.getPeerClusterZnode());
    src.getSourceMetrics().clear();
    this.oldsources.remove(src);
    deleteSource(src.getPeerClusterZnode(), false);
    this.walsByIdRecoveredQueues.remove(src.getPeerClusterZnode());
  }

  /**
   * Clear the references to the specified old source
   * @param src source to clear
   */
  public void closeQueue(ReplicationSourceInterface src) {
    String id = src.getPeerClusterZnode();
    this.sourcesPeerRunningStatus.computeIfPresent(id , (peerId, status) -> {
      this.sources.remove(peerId);
      clearPeerInformation(peerId);
      this.replicationQueues.removePeerFromHFileRefs(peerId);
      return null;
    });
  }
  
  private void clearPeerInformation(String peerId) {
    this.sourcesMetrics.remove(peerId).clear();
    this.sourcesWaitingDrainPaths.remove(peerId);
    this.walsById.remove(peerId);
    Map<TableName, List<String>> tableCFs = this.replicationPeers.getTableCFs(peerId);
    deleteSource(peerId, true);
    if (tableCFs != null && !tableCFs.isEmpty()) {
      Set<TableName> currentPeerReplicationTables =
          this.replicationPeers.getAllPeerReplicationTables();
      tableCFs.keySet().forEach(tableName -> {
        if (!currentPeerReplicationTables.contains(tableName)) {
          this.peerReplicationTables.remove(tableName);
        }
      });
    }
  }

  /**
   * Thie method first deletes all the recovered sources for the specified
   * id, then deletes the normal source (deleting all related data in ZK).
   * @param id The id of the peer cluster
   */
  public void removePeer(String id) {
    LOG.info("Closing the following queue " + id + ", currently have "
        + sources.size() + " and another "
        + oldsources.size() + " that were recovered");
    String terminateMessage = "Replication stream was removed by a user";
    List<ReplicationSourceInterface> oldSourcesToDelete =
        new ArrayList<ReplicationSourceInterface>();
    // synchronized on oldsources to avoid adding recovered source for the to-be-removed peer
    // see NodeFailoverWorker.run
    synchronized (oldsources) {
      // First close all the recovered sources for this peer
      for (ReplicationSourceInterface src : oldsources) {
        if (id.equals(src.getPeerClusterId())) {
          oldSourcesToDelete.add(src);
        }
      }
      for (ReplicationSourceInterface src : oldSourcesToDelete) {
        src.terminate(terminateMessage);
        closeRecoveredQueue(src);
      }
    }
    LOG.info("Number of deleted recovered sources for " + id + ": "
        + oldSourcesToDelete.size());
    this.sourcesPeerRunningStatus.computeIfPresent(id, (peerId, status) -> {
      ReplicationSourceInterface srcToRemove = getSource(peerId);
      this.sources.remove(peerId);
      if (srcToRemove != null) {
        srcToRemove.terminate(terminateMessage);
      }
      clearPeerInformation(peerId);
      return null;
    });
  }

  @Override
  public void regionServerRemoved(String regionserver) {
    if (enableFailover) {
      transferQueues(regionserver);
    } else {
      LOG.info("RegionServer " + regionserver + " offline, " +
        "because of we set enableFailover to false at " +
        server.getServerName().toString() +
        ", so it will not handle the failover task.");
    }
  }

  @Override
  public void peerRemoved(String peerId) {
    removePeer(peerId);
    this.replicationQueues.removePeerFromHFileRefs(peerId);
  }

  @Override
  public void peerListChanged(List<String> peerIds) {
    for (String id : peerIds) {
      this.sourcesPeerRunningStatus.computeIfAbsent(id, peerId -> {
        MetricsSource metrics = new MetricsSource(peerId);
        metrics.setPeerRunning();
        this.sourcesMetrics.put(peerId ,metrics);
        try {
          this.replicationPeers.peerAdded(peerId);
          addPeerSource(peerId, metrics);
          Map<TableName, List<String>> tableCFs = this.replicationPeers.getTableCFs(peerId);
          if (tableCFs != null && !tableCFs.isEmpty()) {
            this.peerReplicationTables.addAll(tableCFs.keySet());
            // Check if new added peer's tables are online or not
            if (tableCFs.keySet()
                .stream()
                .noneMatch(tableName -> {
                  AtomicInteger onlineRegionCount = this.tableOnlineRegionCount.get(tableName);
                  return onlineRegionCount != null && onlineRegionCount.get() > 0;
                })
            ) {
              synchronized (this.latestPaths) {
                this.sourcesWaitingDrainPaths.put(peerId, new HashSet<>(this.latestPaths));
              }
            }
          }
        } catch (Exception e) {
          LOG.error("Error while adding a new peer", e);
        }
        return PeerRunningStatus.RUNNING;
      });
    }
  }

  /**
   * Class responsible to setup new ReplicationSources to take care of the
   * queues from dead region servers.
   */
  class NodeFailoverWorker extends Thread {

    private final boolean isSyncUp;
    private String rsZnode;
    private final ReplicationQueues rq;
    private final ReplicationPeers rp;
    private final UUID clusterId;
  
    /**
     * @param rsZnode region server znode
     */
    public NodeFailoverWorker(String rsZnode) {
      this(rsZnode, replicationQueues, replicationPeers, ReplicationSourceManager.this.clusterId,
        false);
    }

    public NodeFailoverWorker(String rsZnode, final ReplicationQueues replicationQueues,
        final ReplicationPeers replicationPeers, final UUID clusterId, final boolean isSyncUp) {
      super("Failover-for-"+rsZnode);
      this.rsZnode = rsZnode;
      this.rq = replicationQueues;
      this.rp = replicationPeers;
      this.clusterId = clusterId;
      this.isSyncUp = isSyncUp;
    }

    @Override
    public void run() {
      if (this.rq.isThisOurZnode(rsZnode)) {
        return;
      }
      // Wait a bit before transferring the queues, we may be shutting down.
      // This sleep may not be enough in some cases.
      try {
        Thread.sleep(sleepBeforeFailover + (long) (rand.nextFloat() * sleepBeforeFailover));
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting before transferring a queue.");
        Thread.currentThread().interrupt();
      }
      // We try to lock that rs' queue directory
      if (server.isStopped()) {
        LOG.info("Not transferring queue since we are shutting down");
        return;
      }

      Map<String, SortedSet<String>> newQueues = new HashMap<>();
      List<String> peers = rq.getUnClaimedQueueIds(rsZnode);
      if (LOG.isDebugEnabled()) {
        if (peers == null) {
          LOG.debug("getUnClaimedQueueIds peers = null, rsZnode = " + rsZnode);
        } else {
          LOG.debug("getUnClaimedQueueIds peers.size = " + peers.size() + ", rsZnode = " + rsZnode);
        }
      }
      while (peers != null && !peers.isEmpty()) {
        Pair<String, SortedSet<String>> peer = this.rq.claimQueue(rsZnode,
            peers.get(rand.nextInt(peers.size())), isSyncUp);
        long sleep = sleepBeforeFailover/2;
        if (peer != null) {
          newQueues.put(peer.getFirst(), peer.getSecond());
          sleep = sleepBeforeFailover;
        }
        try {
          Thread.sleep(sleep);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while waiting before transferring a queue.");
          Thread.currentThread().interrupt();
        }
        peers = rq.getUnClaimedQueueIds(rsZnode);
      }
      if (peers != null) {
        rq.removeReplicatorIfQueueIsEmpty(rsZnode);
      }

      // Copying over the failed queue is completed.
      if (newQueues.isEmpty()) {
        // We either didn't get the lock or the failed region server didn't have any outstanding
        // WALs to replicate, so we are done.
        return;
      }

      for (Map.Entry<String, SortedSet<String>> entry : newQueues.entrySet()) {
        String peerId = entry.getKey();
        SortedSet<String> walsSet = entry.getValue();
        try {
          // there is not an actual peer defined corresponding to peerId for the failover.
          ReplicationQueueInfo replicationQueueInfo = new ReplicationQueueInfo(peerId);
          String actualPeerId = replicationQueueInfo.getPeerId();
          ReplicationPeer peer = replicationPeers.getPeer(actualPeerId);
          ReplicationPeerConfig peerConfig = null;
          try {
            peerConfig = replicationPeers.getReplicationPeerConfig(actualPeerId);
          } catch (ReplicationException ex) {
            LOG.warn("Received exception while getting replication peer config, skipping replay"
                + ex);
          }
          if (peer == null || peerConfig == null) {
            LOG.warn("Skipping failover for peer:" + actualPeerId + " of node" + rsZnode);
            replicationQueues.removeQueue(peerId);
            continue;
          }
          if (server instanceof ReplicationSyncUp.DummyServer
              && peer.getPeerState().equals(PeerState.DISABLED)) {
            LOG.warn("Peer " + actualPeerId + " is disbaled. ReplicationSyncUp tool will skip "
                + "replicating data to this peer.");
            continue;
          }
          // track sources in walsByIdRecoveredQueues
          Map<String, SortedSet<String>> walsByGroup = new HashMap<String, SortedSet<String>>();
          walsByIdRecoveredQueues.put(peerId, walsByGroup);
          for (String wal : walsSet) {
            String walPrefix = DefaultWALProvider.getWALPrefixFromWALName(wal);
            SortedSet<String> wals = walsByGroup.get(walPrefix);
            if (wals == null) {
              wals = new TreeSet<String>();
              walsByGroup.put(walPrefix, wals);
            }
            wals.add(wal);
          }

          // enqueue sources
          MetricsSource metrics = new MetricsSource(peerId);
          ReplicationSourceInterface src =
              getReplicationSource(conf, fs, ReplicationSourceManager.this, this.rq, this.rp,
                server, peerId, this.clusterId, peerConfig, peer, metrics);
          // synchronized on oldsources to avoid adding recovered source for the to-be-removed peer
          // see removePeer
          synchronized (oldsources) {
            if (!this.rp.getPeerIds().contains(src.getPeerClusterId())) {
              src.terminate("Recovered queue doesn't belong to any current peer");
              closeRecoveredQueue(src);
              continue;
            }
            oldsources.add(src);
            for (String wal : walsSet) {
              src.enqueueLog(new Path(oldLogDir, wal));
            }
            src.startup();
          }
        } catch (IOException e) {
          // TODO manage it
          LOG.error("Failed creating a source", e);
        }
      }
    }
  }

  /**
   * Get the directory where wals are archived
   * @return the directory where wals are archived
   */
  public Path getOldLogDir() {
    return this.oldLogDir;
  }

  /**
   * Get the directory where wals are stored by their RSs
   * @return the directory where wals are stored by their RSs
   */
  public Path getLogDir() {
    return this.logDir;
  }

  /**
   * Get the handle on the local file system
   * @return Handle on the local file system
   */
  public FileSystem getFs() {
    return this.fs;
  }

  /**
   * Get the ReplicationPeers used by this ReplicationSourceManager
   * @return the ReplicationPeers used by this ReplicationSourceManager
   */
  public ReplicationPeers getReplicationPeers() {
    return this.replicationPeers;
  }

  /**
   * Get a string representation of all the sources' metrics
   */
  public String getStats() {
    StringBuffer stats = new StringBuffer();
    for (ReplicationSourceInterface source : this.sources.values()) {
      stats.append("Normal source for cluster " + source.getPeerClusterId() + ": ");
      stats.append(source.getStats() + "\n");
    }
    for (ReplicationSourceInterface oldSource : oldsources) {
      stats.append("Recovered source for cluster/machine(s) " + oldSource.getPeerClusterId()+": ");
      stats.append(oldSource.getStats()+ "\n");
    }
    return stats.toString();
  }

  public void addHFileRefs(TableName tableName, byte[] family, List<Pair<Path, Path>> pairs)
      throws ReplicationException {
    for (ReplicationSourceInterface source : this.sources.values()) {
      source.addHFileRefs(tableName, family, pairs);
    }
  }

  public void cleanUpHFileRefs(String peerId, List<String> files) {
    this.replicationQueues.removeHFileRefs(peerId, files);
  }
  
  @VisibleForTesting
  public Set<Path> getLatestPaths() {
    return this.latestPaths;
  }
  
  protected void addPeerSource(String id, MetricsSource metrics) throws ReplicationException,
      IOException {
    addSource(id, metrics);
    if (replicationForBulkLoadDataEnabled) {
      // Check if peer exists in hfile-refs queue, if not add it. This can happen in the case
      // when a peer was added before replication for bulk loaded data was enabled.
      this.replicationQueues.addPeerToHFileRefs(id);
    }
  }
  
  public void increaseOnlineRegionCount(TableName tableName) {
    this.tableOnlineRegionCount.compute(tableName, (table, onlineRegionCount) -> {
      if (onlineRegionCount == null) {
        if (isPeerReplicationTable(table)) {
          activateReplicationSource(table);
        }
        return new AtomicInteger(1);
      } else {
        onlineRegionCount.incrementAndGet();
        return onlineRegionCount;
      }
    });
  }
  
  /**
   * When some region is new online if the table is peer replication table,
   * activate the related sources if needed.
   * @param tableName the tableName of new online region
   */
  private void activateReplicationSource(TableName tableName) {
    for (String id : this.replicationPeers.getTablePeers(tableName)) {
      this.sourcesPeerRunningStatus.computeIfPresent(id, (peerId, status) -> {
        MetricsSource metrics = this.sourcesMetrics.get(peerId);
        if (status.equals(PeerRunningStatus.NOT_RUNNING)) {
          metrics.setPeerRunning();
          try {
            addPeerSource(peerId, metrics);
          } catch (ReplicationException|IOException e) {
            String message = "Error while activating the peer:" + peerId;
            LOG.fatal(message, e);
            this.server.abort(message, e);
          }
        }
        this.sourcesWaitingDrainPaths.remove(peerId);
        return PeerRunningStatus.RUNNING;
      });
    }
  }
  
  public void decreaseOnlineRegionCount(TableName tableName) {
    this.tableOnlineRegionCount.computeIfPresent(tableName, (table, onlineRegionCount) -> {
      if (onlineRegionCount.decrementAndGet() == 0) {
        if (isPeerReplicationTable(table)) {
          markReplicationSourceWaitingDrain(table);
        }
        return null;
      }
      return onlineRegionCount;
    });
  }
  
  /**
   * Check and mark the replication source with specific table as waiting for logQueue drain
   * when all the related region is offline.
   * @param tableName tableName
   */
  private void markReplicationSourceWaitingDrain(TableName tableName) {
    for (String id : this.replicationPeers.getTablePeers(tableName)) {
      this.sourcesPeerRunningStatus.computeIfPresent(id, (peerId, status) -> {
        if (!this.sourcesWaitingDrainPaths.containsKey(peerId) &&
            this.replicationPeers.getTableCFs(peerId)
                .keySet()
                .stream()
                .noneMatch(table -> {
                  AtomicInteger onlineRegionCount = this.tableOnlineRegionCount.get(table);
                  return onlineRegionCount != null && onlineRegionCount.get() > 0;
                })
        ) {
          synchronized (this.latestPaths) {
            this.sourcesWaitingDrainPaths.put(peerId, new HashSet<>(this.latestPaths));
          }
        }
        return status;
      });
    }
  }
  
  /**
   * Check given tableName is peer replication table
   * @param tableName tableName
   * @return true if the tableName is existing in the replcationTables in the sourceManager
   */
  private boolean isPeerReplicationTable(TableName tableName) {
    return this.peerReplicationTables.contains(tableName);
  }
  
  /**
   * Check all the waiting drain sources and remove it when all the recorded paths is consumed
   */
  public void checkAndCleanWaitingDrainSource() {
    this.sourcesWaitingDrainPaths.keySet()
        .forEach(id -> this.sourcesPeerRunningStatus.computeIfPresent(id, (peerId, status) -> {
          Set<Path> waitingConsumePaths = this.sourcesWaitingDrainPaths.get(peerId);
          // check if the source is waiting drain source
          if (waitingConsumePaths != null) {
            // check if the source is drained
            Map<String, SortedSet<String>> peerWals = this.walsById.get(peerId);
            Set<String> peerWalPrefixes = peerWals.keySet();
            boolean drained = waitingConsumePaths.stream()
                .allMatch(path -> {
                  String name = path.getName();
                  String walPrefix = DefaultWALProvider.getWALPrefixFromWALName(name);
                  return peerWalPrefixes.contains(walPrefix) &&
                      !peerWals.get(walPrefix).contains(name);
                });

            if (drained) {
              String terminateMessage = "Replication source " + id +
                  " has no online regions in this server " +
                  "and all the waiting drain paths is consumed";
              MetricsSource metrics = this.sourcesMetrics.get(peerId);
              ReplicationSourceInterface source = getSource(peerId);
              this.sources.remove(peerId);
              source.terminate(terminateMessage);
              this.walsById.remove(peerId);
              this.sourcesWaitingDrainPaths.remove(peerId);
              this.replicationQueues.removePeerFromHFileRefs(peerId);
              deleteSource(peerId, false);
              metrics.clearSizeOfLogQueue();
              metrics.setPeerNotRunning();
              status = PeerRunningStatus.NOT_RUNNING;
            }
          }
          return status;
        }
        ));
  }

  /**
   * This chore, every time it runs, will try to check and clean all the waiting drain source
   */
  static class ReplicationSourceCleanerChore extends ScheduledChore {

    private static final String REPLICATION_SOURCE_CLEANER_CHORE_NAME = "ReplicationSourceCleaner";
    private static final int REPLICATION_SOURCE_CLEANER_INTERVAL = 600 * 1000; // Always 10 min

    private final ReplicationSourceManager manager;

    public ReplicationSourceCleanerChore(Stoppable stopper, ReplicationSourceManager manager) {
      super(REPLICATION_SOURCE_CLEANER_CHORE_NAME, stopper, REPLICATION_SOURCE_CLEANER_INTERVAL);
      this.manager = manager;
    }

    @Override
    protected void chore() {
      manager.checkAndCleanWaitingDrainSource();
    }

  }
  
  @VisibleForTesting
  Set<TableName> getReplicationTables() {
    return this.peerReplicationTables;
  }
  
  @VisibleForTesting
  Map<String, Set<Path>> getSourcesWaitingDrainPaths() {
    return new HashMap<>(this.sourcesWaitingDrainPaths);
  }
  
  @VisibleForTesting
  Map<TableName, AtomicInteger> getTableOnlineRegionCount() {
    return new HashMap<>(this.tableOnlineRegionCount);
  }
  
  @VisibleForTesting
  PeerRunningStatus getPeerRunningStatus(String peerId) {
    return this.sourcesPeerRunningStatus.get(peerId);
  }
  
  @VisibleForTesting
  MetricsSource getSourceMetrics(String peerId) {
    return this.sourcesMetrics.get(peerId);
  }
}
