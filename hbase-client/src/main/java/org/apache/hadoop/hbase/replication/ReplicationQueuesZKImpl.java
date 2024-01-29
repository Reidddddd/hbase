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
package org.apache.hadoop.hbase.replication;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKUtil.ZKUtilOp;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

/**
 * This class provides an implementation of the ReplicationQueues interface using Zookeeper. The
 * base znode that this class works at is the myQueuesZnode. The myQueuesZnode contains a list of
 * all outstanding WAL files on this region server that need to be replicated. The myQueuesZnode is
 * the regionserver name (a concatenation of the region server’s hostname, client port and start
 * code). For example:
 *
 * /hbase/replication/rs/hostname.example.org,6020,1234
 *
 * Within this znode, the region server maintains a set of WAL replication queues. These queues are
 * represented by child znodes named using there give queue id. For example:
 *
 * /hbase/replication/rs/hostname.example.org,6020,1234/1
 * /hbase/replication/rs/hostname.example.org,6020,1234/2
 *
 * Each queue has one child znode for every WAL that still needs to be replicated. The value of
 * these WAL child znodes is the latest position that has been replicated. This position is updated
 * every time a WAL entry is replicated. For example:
 *
 * /hbase/replication/rs/hostname.example.org,6020,1234/1/23522342.23422 [VALUE: 254]
 */
@InterfaceAudience.Private
public class ReplicationQueuesZKImpl extends ReplicationStateZKBase implements ReplicationQueues {

  private ReplicationType replicationType;

  /** Znode containing all replication queues for this region server. */
  private String myQueuesZnode;
  private String serverName;
  /** Name of znode we use to lock during failover */
  public final static String RS_LOCK_ZNODE = "lock";

  private static final Log LOG = LogFactory.getLog(ReplicationQueuesZKImpl.class);
  private FileSystem fs;
  private Path walsDir;
  private Path oldWALsDir;

  public ReplicationQueuesZKImpl(final ZooKeeperWatcher zk, Configuration conf,
                                 Abortable abortable) {
    super(zk, conf, abortable);
  }

  public ReplicationQueuesZKImpl(final ZooKeeperWatcher zk, Configuration conf,
                                 Abortable abortable, FileSystem fs, Path oldWALsDir) {
    super(zk, conf, abortable);
    this.fs = fs;
    this.oldWALsDir = oldWALsDir;
  }

  public ReplicationQueuesZKImpl(final ZooKeeperWatcher zk, Configuration conf,
                                 Abortable abortable, FileSystem fs, Path oldWALsDir,
                                 ReplicationType replicationType) {
    super(zk, conf, abortable);
    this.fs = fs;
    this.oldWALsDir = oldWALsDir;
    this.replicationType = replicationType;
  }

  @Override
  public void init(String serverName, Path walsDir) throws ReplicationException {
    this.myQueuesZnode = ZKUtil.joinZNode(this.queuesZNode, serverName);
    this.serverName = serverName;
    this.walsDir = walsDir;
    try {
      if (ZKUtil.checkExists(this.zookeeper, this.myQueuesZnode) < 0) {
        ZKUtil.createWithParents(this.zookeeper, this.myQueuesZnode);
      }
    } catch (KeeperException e) {
      throw new ReplicationException("Could not initialize replication queues.", e);
    }
    if (conf.getBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY,
      HConstants.REPLICATION_BULKLOAD_ENABLE_DEFAULT)) {
      try {
        if (ZKUtil.checkExists(this.zookeeper, this.hfileRefsZNode) < 0) {
          ZKUtil.createWithParents(this.zookeeper, this.hfileRefsZNode);
        }
      } catch (KeeperException e) {
        throw new ReplicationException("Could not initialize hfile references replication queue.",
            e);
      }
    }
  }

  @Override
  public List<String> getListOfReplicators() throws ReplicationException {
    try {
      return super.getListOfReplicatorsZK();
    } catch (KeeperException e) {
      LOG.warn("getListOfReplicators() from ZK failed", e);
      throw new ReplicationException("getListOfReplicators() from ZK failed", e);
    }
  }

  @Override
  public void removeQueue(String queueId) {
    try {
      ZKUtil.deleteNodeRecursively(this.zookeeper, ZKUtil.joinZNode(this.myQueuesZnode, queueId));
    } catch (KeeperException e) {
      this.abortable.abort("Failed to delete queue (queueId=" + queueId + ")", e);
    }
  }

  @Override
  public void initLog(String queueId, String filename, String walPrefix)
    throws ReplicationException {
    try {
      String znode = ZKUtil.joinZNode(this.myQueuesZnode, queueId);
      if (walPrefix != null && walPrefix.length() > 0) {
        if (ZKUtil.nodeHasChildrenWithPrefix(this.zookeeper, znode, walPrefix)) {
          return;
        }
      }
      addLog(queueId, filename);
    } catch (KeeperException e) {
      throw new ReplicationException(
        "Could not add log because znode could not be created. queueId=" + queueId
          + ", filename=" + filename);
    }
  }

  @Override
  public void addLog(String queueId, String filename) throws ReplicationException {
    String znode = ZKUtil.joinZNode(this.myQueuesZnode, queueId);
    znode = ZKUtil.joinZNode(znode, filename);
    if (LOG.isDebugEnabled()) {
      LOG.debug("addLog znode = " + znode);
    }
    try {
      ZKUtil.createWithParents(this.zookeeper, znode);
    } catch (KeeperException e) {
      throw new ReplicationException(
          "Could not add log because znode could not be created. queueId=" + queueId
              + ", filename=" + filename);
    }
  }

  @Override
  public void removeLog(String queueId, String filename) {
    try {
      String znode = ZKUtil.joinZNode(this.myQueuesZnode, queueId);
      znode = ZKUtil.joinZNode(znode, filename);
      if (-1 != ZKUtil.checkExists(this.zookeeper, znode)) {
        ZKUtil.deleteNode(this.zookeeper, znode);
      }
    } catch (KeeperException e) {
      this.abortable.abort("Failed to remove wal from queue (queueId=" + queueId + ", filename="
          + filename + ")", e);
    }
  }

  @Override
  public void setLogPosition(String queueId, String filename, long position) {
    try {
      String znode = ZKUtil.joinZNode(this.myQueuesZnode, queueId);
      znode = ZKUtil.joinZNode(znode, filename);
      if (LOG.isDebugEnabled()) {
        LOG.debug("setLogPosition znode = " + znode + ", pos =" + position);
      }
      // Why serialize String of Long and not Long as bytes?
      ZKUtil.setData(this.zookeeper, znode, ZKUtil.positionToByteArray(position));
    } catch (KeeperException.NoNodeException e) {
      try {
        addLog(queueId, filename);
      } catch (ReplicationException ex) {
        this.abortable.abort("Failed to add log when writing replication wal position " +
          "because znode could not be created (filename=" + filename
          + ", position=" + position + ")", e);
      }
      setLogPosition(queueId, filename, position);
    } catch (KeeperException e) {
      this.abortable.abort("Failed to write replication wal position (filename=" + filename
          + ", position=" + position + ")", e);
    }
  }

  @Override
  public long getLogPosition(String queueId, String filename) throws ReplicationException {
    String clusterZnode = ZKUtil.joinZNode(this.myQueuesZnode, queueId);
    String znode = ZKUtil.joinZNode(clusterZnode, filename);
    byte[] bytes = null;
    try {
      bytes = ZKUtil.getData(this.zookeeper, znode);
    } catch (KeeperException e) {
      throw new ReplicationException("Internal Error: could not get position in log for queueId="
          + queueId + ", filename=" + filename, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return 0;
    }
    try {
      return ZKUtil.parseWALPositionFrom(bytes);
    } catch (DeserializationException de) {
      LOG.warn("Failed to parse WALPosition for queueId=" + queueId + " and wal=" + filename
          + " znode content, continuing.");
    }
    // if we can not parse the position, start at the beginning of the wal file
    // again
    return 0;
  }

  @Override
  public boolean isThisOurZnode(String znode) {
    return ZKUtil.joinZNode(this.queuesZNode, znode).equals(this.myQueuesZnode);
  }

  @Override
  public void removeAllQueues() {
    try {
      ZKUtil.deleteNodeRecursively(this.zookeeper, this.myQueuesZnode);
    } catch (KeeperException e) {
      // if the znode is already expired, don't bother going further
      if (e instanceof KeeperException.SessionExpiredException) {
        return;
      }
      this.abortable.abort("Failed to delete replication queues for region server: "
          + this.myQueuesZnode, e);
    }
  }

  @Override
  @VisibleForTesting
  public List<String> getLogsInQueue(String queueId) {
    String znode = ZKUtil.joinZNode(this.myQueuesZnode, queueId);
    List<String> result = null;
    try {
      result = getLogsInQueue(ZKUtil.listChildrenNoWatch(this.zookeeper, znode), false);
    } catch (KeeperException | IOException e) {
      this.abortable.abort("Failed to get list of wals for queueId=" + queueId, e);
    }
    return result;
  }

  @Override
  public List<String> getCurrentConsumingLogsInQueue(String queueId)
    throws KeeperException {
    String znode = ZKUtil.joinZNode(this.myQueuesZnode, queueId);
    List<String> result = null;
    try {
      result = ZKUtil.listChildrenNoWatch(this.zookeeper, znode);
    } catch (KeeperException e) {
      this.abortable.abort("Failed to get list of wals for queueId=" + queueId
        + " and serverName=" + serverName, e);
      throw e;
    }
    return result;
  }

  /**
   * Now we only store current consuming wal on zk, and only during the queue transfer process
   * after RS crash, we need to use this method to recover the queue. Notice there is differences
   * between ReplicationSyncUp and normal NodeFailoverWorker: For normal NodeFailoverWorker we only
   * find wals in oldWALs, but for ReplicationSyncUp we need to find all related wal paths.
   * @param currentNodes current nodes
   * @param isSyncUp is ReplicationSyncUp
   * @return queue list
   * @throws IOException IOException
   */
  private List<String> getLogsInQueue(List<String> currentNodes, boolean isSyncUp)
    throws IOException {
    if (currentNodes == null || currentNodes.isEmpty()) {
      return currentNodes;
    }

    // Try to find out every current consuming wal of each wal group on zk.
    Map<String, String> currentConsumingWALMap = new HashMap<>();
    currentNodes.forEach(node -> {
      String serverNameAndGroupName = getWALServerNameAndGroupNameFromWALName(node);
      String timestamp = getWALTimestampFromWALName(node);
      if (!currentConsumingWALMap.containsKey(serverNameAndGroupName)) {
        currentConsumingWALMap.put(serverNameAndGroupName, timestamp);
      } else {
        if (currentConsumingWALMap.get(serverNameAndGroupName).compareTo(timestamp) < 0) {
          currentConsumingWALMap.put(serverNameAndGroupName, timestamp);
        }
      }
    });

    // Search wal path to find out all wals.
    List<String> results = new ArrayList<>();
    FileStatus[] allLogs = null;
    if (!isSyncUp) {
      allLogs = fs.listStatus(oldWALsDir);
    } else {
      Path walDir = new Path(this.walsDir, serverName);
      if (fs.exists(walDir)) {
        allLogs = fs.listStatus(walDir);
      }
      if (fs.exists(walDir.suffix("-splitting"))) {
        allLogs = ArrayUtils.addAll(allLogs, fs.listStatus(walDir.suffix("-splitting")));
      }
      allLogs = ArrayUtils.addAll(allLogs, fs.listStatus(oldWALsDir));
    }

    // Filter out wals which timestamp bigger than current consuming wal,
    // that means they are still in queue.
    Arrays.stream(allLogs).forEach(file -> {
      String fileName = file.getPath().getName();
      if (!fileName.contains("meta")) {
        String serverNameAndGroupName = getWALServerNameAndGroupNameFromWALName(fileName);
        String timestamp = getWALTimestampFromWALName(fileName);
        if (currentConsumingWALMap.containsKey(serverNameAndGroupName)) {
          if (currentConsumingWALMap.get(serverNameAndGroupName).compareTo(timestamp) <= 0) {
            results.add(fileName);
          }
        }
      }
    });
    return results;
  }

  @Override
  public List<String> getAllQueues() {
    List<String> listOfQueues = null;
    try {
      listOfQueues = ZKUtil.listChildrenNoWatch(this.zookeeper, this.myQueuesZnode);
    } catch (KeeperException e) {
      this.abortable.abort("Failed to get a list of queues for region server: "
          + this.myQueuesZnode, e);
    }
    return listOfQueues;
  }

  @Override
  public List<String> getAllQueuesOfReplicator(String replicator) throws ReplicationException {
    try {
      return super.getListOfReplicatorZK(replicator);
    } catch (KeeperException e) {
      this.abortable.abort("Failed to get a list of queues for region server: "
        + this.myQueuesZnode, e);
      throw new ReplicationException("getListOfReplicators() from ZK failed", e);
    }
  }

  @Override
  public boolean isThisOurRegionServer(String regionserver) {
    return ZKUtil.joinZNode(this.queuesZNode, regionserver).equals(this.myQueuesZnode);
  }

  @Override
  public List<String> getUnClaimedQueueIds(String regionserver) {
    if (isThisOurRegionServer(regionserver)) {
      return null;
    }
    String rsZnodePath = ZKUtil.joinZNode(this.queuesZNode, regionserver);
    List<String> queues = null;
    try {
      queues = ZKUtil.listChildrenNoWatch(this.zookeeper, rsZnodePath);
    } catch (KeeperException e) {
      this.abortable.abort("Failed to getUnClaimedQueueIds for " + regionserver, e);
    }
    if (queues != null) {
      queues.remove(RS_LOCK_ZNODE);
    }
    return queues;
  }

  @Override
  public Pair<String, SortedSet<String>> claimQueue(
    String regionserver, String queueId, boolean isSyncUp) {
    // As we cannot get all accurate wals through zk now,
    // need to wait SCP done at first so we can get all wals in `oldWALs`.
    if (!isSyncUp) {
      waitingServerCrashProcedureDone(regionserver);
    }
    if (conf.getBoolean(HConstants.ZOOKEEPER_USEMULTI, true)) {
      LOG.info("Atomically moving " + regionserver + "/" + queueId + "'s WALs to my queue");
      return moveQueueUsingMulti(regionserver, queueId, isSyncUp);
    } else {
      LOG.info("Moving " + regionserver + "/" + queueId + "'s wals to my queue");
      if (!lockOtherRS(regionserver)) {
        LOG.info("Can not take the lock now");
        return null;
      }
      Pair<String, SortedSet<String>> newQueues;
      try {
        newQueues = copyQueueFromLockedRS(regionserver, queueId, isSyncUp);
        removeQueueFromLockedRS(regionserver, queueId);
      } finally {
        unlockOtherRS(regionserver);
      }
      return newQueues;
    }
  }

  private void waitingServerCrashProcedureDone(String deadServerName) {
    final Path deadRsDir = new Path(walsDir.getParent(), deadServerName);
    final Path deadRsSplitDir = deadRsDir.suffix("-splitting");
    try {
      while (true) {
        if (!fs.exists(deadRsDir) && !fs.exists(deadRsSplitDir)) {
          break;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Waiting for SCP of " + deadServerName + "done...");
        }
        Thread.sleep(10 * 1000);
      }
    } catch (Exception e) {
      LOG.error("Got an error when waiting " + deadServerName + " SCP done ", e);
      throw new RuntimeException(e);
    }
  }

  private void removeQueueFromLockedRS(String znode, String peerId) {
    String nodePath = ZKUtil.joinZNode(this.queuesZNode, znode);
    String peerPath = ZKUtil.joinZNode(nodePath, peerId);
    try {
      ZKUtil.deleteNodeRecursively(this.zookeeper, peerPath);
    } catch (KeeperException e) {
      LOG.warn("Remove copied queue failed", e);
    }
  }

  @Override
  public void removeReplicatorIfQueueIsEmpty(String regionserver) {
    String rsPath = ZKUtil.joinZNode(this.queuesZNode, regionserver);
    try {
      List<String> list = ZKUtil.listChildrenNoWatch(this.zookeeper, rsPath);
      if (list != null && list.size() == 0){
        ZKUtil.deleteNode(this.zookeeper, rsPath);
      }
    } catch (KeeperException e) {
      LOG.warn("Got error while removing replicator", e);
    }
  }

  /**
   * Try to set a lock in another region server's znode.
   * @param znode the server names of the other server
   * @return true if the lock was acquired, false in every other cases
   */
  @VisibleForTesting
  @Override
  public boolean lockOtherRS(String znode) {
    return lockOtherRS(znode, CreateMode.PERSISTENT);
  }

  /**
   * Try to set a lock in another region server's znode.
   * @param znode the server names of the other server
   * @return true if the lock was acquired, false in every other cases
   */
  @VisibleForTesting
  @Override
  public boolean lockOtherRS(String znode, CreateMode createMode) {
    try {
      String parent = ZKUtil.joinZNode(this.queuesZNode, znode);
      if (parent.equals(this.myQueuesZnode)) {
        LOG.warn("Won't lock because this is us, we're dead!");
        return false;
      }
      String p = ZKUtil.joinZNode(parent, RS_LOCK_ZNODE);
      if (runningIndependentReplicationConsumer()) {
        // Under normal circumstances, myQueuesZnode is assigned during init,
        // but for ReplicationIndependentConsumer, we need to use this method to obtain the lock
        // before init.
        String hostname = InetAddress.getLocalHost().getHostAddress();
        ZKUtil.createAndWatch(this.zookeeper, p, lockToByteArray(hostname), createMode);
      } else {
        ZKUtil.createAndWatch(this.zookeeper, p, lockToByteArray(this.myQueuesZnode), createMode);
      }
    } catch (KeeperException | UnknownHostException e) {
      // This exception will pop up if the znode under which we're trying to
      // create the lock is already deleted by another region server, meaning
      // that the transfer already occurred.
      // NoNode => transfer is done and znodes are already deleted
      // NodeExists => lock znode already created by another RS
      if (e instanceof KeeperException.NoNodeException
          || e instanceof KeeperException.NodeExistsException) {
        LOG.info("Won't transfer the queue," + " another RS took care of it because of: "
            + e.getMessage());
      } else {
        LOG.info("Failed lock other rs", e);
      }
      return false;
    }
    return true;
  }

  private boolean runningIndependentReplicationConsumer() {
    return replicationType != null &&
      replicationType == ReplicationType.INDEPENDENT;
  }

  public String getLockZNode(String znode) {
    return this.queuesZNode + "/" + znode + "/" + RS_LOCK_ZNODE;
  }

  @VisibleForTesting
  public boolean checkLockExists(String znode) throws KeeperException {
    return ZKUtil.checkExists(zookeeper, getLockZNode(znode)) >= 0;
  }

  public void unlockOtherRS(String znode){
    String parent = ZKUtil.joinZNode(this.queuesZNode, znode);
    String p = ZKUtil.joinZNode(parent, RS_LOCK_ZNODE);
    try {
      ZKUtil.deleteNode(this.zookeeper, p);
    } catch (KeeperException e) {
      this.abortable.abort("Remove lock failed", e);
    }
  }

  /**
   * Delete all the replication queues for a given region server.
   * @param regionserverZnode The znode of the region server to delete.
   */
  private void deleteAnotherRSQueues(String regionserverZnode) {
    String fullpath = ZKUtil.joinZNode(this.queuesZNode, regionserverZnode);
    try {
      List<String> clusters = ZKUtil.listChildrenNoWatch(this.zookeeper, fullpath);
      for (String cluster : clusters) {
        // No need to delete, it will be deleted later.
        if (cluster.equals(RS_LOCK_ZNODE)) {
          continue;
        }
        String fullClusterPath = ZKUtil.joinZNode(fullpath, cluster);
        ZKUtil.deleteNodeRecursively(this.zookeeper, fullClusterPath);
      }
      // Finish cleaning up
      ZKUtil.deleteNodeRecursively(this.zookeeper, fullpath);
    } catch (KeeperException e) {
      if (e instanceof KeeperException.NoNodeException
          || e instanceof KeeperException.NotEmptyException) {
        // Testing a special case where another region server was able to
        // create a lock just after we deleted it, but then was also able to
        // delete the RS znode before us or its lock znode is still there.
        if (e.getPath().equals(fullpath)) {
          return;
        }
      }
      this.abortable.abort("Failed to delete replication queues for region server: "
          + regionserverZnode, e);
    }
  }

  /**
   * It "atomically" copies one peer's wals queue from another dead region server and returns them
   * all sorted. The new peer id is equal to the old peer id appended with the dead server's znode.
   * @param znode pertaining to the region server to copy the queues from
   * @peerId peerId pertaining to the queue need to be copied
   */
  private Pair<String, SortedSet<String>> moveQueueUsingMulti(
    String znode, String peerId, boolean isSyncUp) {
    try {
      // hbase/replication/rs/deadrs
      String deadRSZnodePath = ZKUtil.joinZNode(this.queuesZNode, znode);
      List<ZKUtilOp> listOfOps = new ArrayList<>();
      ReplicationQueueInfo replicationQueueInfo = new ReplicationQueueInfo(peerId);

      String newPeerId = peerId + "-" + znode;
      String newPeerZnode = ZKUtil.joinZNode(this.myQueuesZnode, newPeerId);
      // check the logs queue for the old peer cluster
      String oldClusterZnode = ZKUtil.joinZNode(deadRSZnodePath, peerId);
      List<String> wals = ZKUtil.listChildrenNoWatch(this.zookeeper, oldClusterZnode);

      if (!peerExists(replicationQueueInfo.getPeerId())) {
        LOG.warn("Peer " + replicationQueueInfo.getPeerId() +
                " didn't exist, will move its queue to avoid the failure of multi op");
        for (String wal : wals) {
          String oldWalZnode = ZKUtil.joinZNode(oldClusterZnode, wal);
          listOfOps.add(ZKUtilOp.deleteNodeFailSilent(oldWalZnode));
        }
        listOfOps.add(ZKUtilOp.deleteNodeFailSilent(oldClusterZnode));
        ZKUtil.multiOrSequential(this.zookeeper, listOfOps, false);
        return null;
      }

      SortedSet<String> logQueue = new TreeSet<>();
      if (wals == null || wals.size() == 0) {
        listOfOps.add(ZKUtilOp.deleteNodeFailSilent(oldClusterZnode));
      } else {
        // create the new cluster znode
        ZKUtilOp op = ZKUtilOp.createAndFailSilent(newPeerZnode, HConstants.EMPTY_BYTE_ARRAY);
        listOfOps.add(op);
        // get the offset of the logs and set it to new znodes
        for (String wal : wals) {
          String oldWalZnode = ZKUtil.joinZNode(oldClusterZnode, wal);
          byte[] logOffset = ZKUtil.getData(this.zookeeper, oldWalZnode);
          LOG.debug("Creating " + wal + " with data " + Bytes.toString(logOffset));
          String newLogZnode = ZKUtil.joinZNode(newPeerZnode, wal);
          listOfOps.add(ZKUtilOp.createAndFailSilent(newLogZnode, logOffset));
          listOfOps.add(ZKUtilOp.deleteNodeFailSilent(oldWalZnode));
        }
        // add delete op for peer
        listOfOps.add(ZKUtilOp.deleteNodeFailSilent(oldClusterZnode));

        if (LOG.isTraceEnabled())
          LOG.trace(" The multi list size is: " + listOfOps.size());
        logQueue = new TreeSet<>(getLogsInQueue(wals, isSyncUp));
      }
      ZKUtil.multiOrSequential(this.zookeeper, listOfOps, false);

      LOG.info("Atomically moved " + znode + "/" + peerId + "'s WALs to my queue");
      return new Pair<>(newPeerId, logQueue);
    } catch (KeeperException | IOException e) {
      // Multi call failed; it looks like some other regionserver took away the logs.
      LOG.warn("Got exception in copyQueuesFromRSUsingMulti: ", e);
    } catch (InterruptedException e) {
      LOG.warn("Got exception in copyQueuesFromRSUsingMulti: ", e);
      Thread.currentThread().interrupt();
    }
    return null;
  }

  /**
   * This methods moves all the wals queues from another region server and returns them all sorted
   * per peer cluster (appended with the dead server's znode)
   * @param znode server names to copy
   * @return all wals for the peer of that cluster, null if an error occurred
   */
  private Pair<String, SortedSet<String>> copyQueueFromLockedRS(
    String znode, String peerId, boolean isSyncUp) {
    // TODO this method isn't atomic enough, we could start copying and then
    // TODO fail for some reason and we would end up with znodes we don't want.
    try {
      String nodePath = ZKUtil.joinZNode(this.queuesZNode, znode);
      ReplicationQueueInfo replicationQueueInfo = new ReplicationQueueInfo(peerId);
      String clusterPath = ZKUtil.joinZNode(nodePath, peerId);
      if (!peerExists(replicationQueueInfo.getPeerId())) {
        LOG.warn("Peer " + peerId + " didn't exist, skipping the replay");
        // Protection against moving orphaned queues
        return null;
      }
      // We add the name of the recovered RS to the new znode, we can even
      // do that for queues that were recovered 10 times giving a znode like
      // number-startcode-number-otherstartcode-number-anotherstartcode-etc
      String newCluster = peerId + "-" + znode;
      String newClusterZnode = ZKUtil.joinZNode(this.myQueuesZnode, newCluster);

      List<String> wals = ZKUtil.listChildrenNoWatch(this.zookeeper, clusterPath);
      // That region server didn't have anything to replicate for this cluster
      if (wals == null || wals.size() == 0) {
        return null;
      }
      ZKUtil.createNodeIfNotExistsAndWatch(this.zookeeper, newClusterZnode,
          HConstants.EMPTY_BYTE_ARRAY);
      for (String wal : wals) {
        String z = ZKUtil.joinZNode(clusterPath, wal);
        byte[] positionBytes = ZKUtil.getData(this.zookeeper, z);
        long position = 0;
        try {
          position = ZKUtil.parseWALPositionFrom(positionBytes);
        } catch (DeserializationException e) {
          LOG.warn("Failed parse of wal position from the following znode: " + z
            + ", Exception: " + e);
        }
        LOG.debug("Creating " + wal + " with data " + position);
        String child = ZKUtil.joinZNode(newClusterZnode, wal);
        // Position doesn't actually change, we are just deserializing it for
        // logging, so just use the already serialized version
        ZKUtil.createNodeIfNotExistsAndWatch(this.zookeeper, child, positionBytes);
      }
      SortedSet<String> logQueue = new TreeSet<>(getLogsInQueue(wals, isSyncUp));
      return new Pair<>(newCluster, logQueue);
    } catch (KeeperException | IOException e) {
      LOG.warn("Got exception in copyQueueFromLockedRS: "+
        " Possible problem: check if znode size exceeds jute.maxBuffer value. "
          + "If so, increase it for both client and server side." ,e);

    } catch (InterruptedException e) {
      LOG.warn(e);
      Thread.currentThread().interrupt();
    }
    return null;
  }

  /**
   * @param lockOwner
   * @return Serialized protobuf of <code>lockOwner</code> with pb magic prefix prepended suitable
   *         for use as content of an replication lock during region server fail over.
   */
  static byte[] lockToByteArray(final String lockOwner) {
    byte[] bytes =
        ZooKeeperProtos.ReplicationLock.newBuilder().setLockOwner(lockOwner).build().toByteArray();
    return ProtobufUtil.prependPBMagic(bytes);
  }

  @Override
  public void addHFileRefs(String peerId, List<Pair<Path, Path>> pairs)
      throws ReplicationException {
    String peerZnode = ZKUtil.joinZNode(this.hfileRefsZNode, peerId);
    boolean debugEnabled = LOG.isDebugEnabled();
    if (debugEnabled) {
      LOG.debug("Adding hfile references " + pairs + " in queue " + peerZnode);
    }
    List<ZKUtilOp> listOfOps = new ArrayList<ZKUtil.ZKUtilOp>();
    int size = pairs.size();
    for (int i = 0; i < size; i++) {
      listOfOps.add(ZKUtilOp.createAndFailSilent(
        ZKUtil.joinZNode(peerZnode, pairs.get(i).getSecond().getName()),
        HConstants.EMPTY_BYTE_ARRAY));
    }
    if (debugEnabled) {
      LOG.debug(" The multi list size for adding hfile references in zk for node " + peerZnode
          + " is " + listOfOps.size());
    }
    try {
      ZKUtil.multiOrSequential(this.zookeeper, listOfOps, true);
    } catch (KeeperException e) {
      throw new ReplicationException("Failed to create hfile reference znode=" + e.getPath(), e);
    }
  }

  @Override
  public void removeHFileRefs(String peerId, List<String> files) {
    String peerZnode = ZKUtil.joinZNode(this.hfileRefsZNode, peerId);
    boolean debugEnabled = LOG.isDebugEnabled();
    if (debugEnabled) {
      LOG.debug("Removing hfile references " + files + " from queue " + peerZnode);
    }
    List<ZKUtilOp> listOfOps = new ArrayList<ZKUtil.ZKUtilOp>();
    int size = files.size();
    for (int i = 0; i < size; i++) {
      listOfOps.add(ZKUtilOp.deleteNodeFailSilent(ZKUtil.joinZNode(peerZnode, files.get(i))));
    }
    if (debugEnabled) {
      LOG.debug(" The multi list size for removing hfile references in zk for node " + peerZnode
          + " is " + listOfOps.size());
    }
    try {
      ZKUtil.multiOrSequential(this.zookeeper, listOfOps, true);
    } catch (KeeperException e) {
      LOG.error("Failed to remove hfile reference znode=" + e.getPath(), e);
    }
  }

  @Override
  public List<Path> getLogsInPath() throws IOException {
    Path walDir = new Path(this.walsDir, serverName);
    FileStatus[] allLogs = null;
    if (fs.exists(walDir)) {
      allLogs = fs.listStatus(walDir);
    }
    if (fs.exists(walDir.suffix("-splitting"))) {
      allLogs = ArrayUtils.addAll(allLogs, fs.listStatus(walDir.suffix("-splitting")));
    }
    allLogs = ArrayUtils.addAll(allLogs, fs.listStatus(oldWALsDir,
      path -> {
        if (!path.getName().contains("meta") &&
          path.getName().contains(serverName.split(",")[0])) {
          return true;
        }
        return false;
      }));
    return Arrays.stream(allLogs).map(fileStatus -> fileStatus.getPath())
      .collect(Collectors.toList());
  }

  @Override
  public void addPeerToHFileRefs(String peerId) throws ReplicationException {
    String peerZnode = ZKUtil.joinZNode(this.hfileRefsZNode, peerId);
    try {
      if (ZKUtil.checkExists(this.zookeeper, peerZnode) == -1) {
        LOG.info("Adding peer " + peerId + " to hfile reference queue.");
        ZKUtil.createWithParents(this.zookeeper, peerZnode);
      }
    } catch (KeeperException e) {
      throw new ReplicationException("Failed to add peer " + peerId + " to hfile reference queue.",
          e);
    }
  }

  @Override
  public void removePeerFromHFileRefs(String peerId) {
    final String peerZnode = ZKUtil.joinZNode(this.hfileRefsZNode, peerId);
    try {
      if (ZKUtil.checkExists(this.zookeeper, peerZnode) == -1) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Peer " + peerZnode + " not found in hfile reference queue.");
        }
        return;
      } else {
        LOG.info("Removing peer " + peerZnode + " from hfile reference queue.");
        ZKUtil.deleteNodeRecursively(this.zookeeper, peerZnode);
      }
    } catch (KeeperException e) {
      LOG.error("Ignoring the exception to remove peer " + peerId + " from hfile reference queue.",
        e);
    }
  }
}
