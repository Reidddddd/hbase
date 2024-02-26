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
package org.apache.hadoop.hbase.regionserver.wal.bookkeeper;

import static org.apache.hadoop.hbase.HConstants.EMPTY_BYTE_ARRAY;
import static org.apache.hadoop.hbase.regionserver.wal.bookkeeper.BKConstants.BK_DIGEST_TYPE_DEFAULT;
import static org.apache.hadoop.hbase.regionserver.wal.bookkeeper.BKConstants.BK_DIGEST_TYPE_KEY;
import static org.apache.hadoop.hbase.regionserver.wal.bookkeeper.BKConstants.BK_NUM_ACK_DEFAULT;
import static org.apache.hadoop.hbase.regionserver.wal.bookkeeper.BKConstants.BK_NUM_ACK_KEY;
import static org.apache.hadoop.hbase.regionserver.wal.bookkeeper.BKConstants.BK_NUM_ENSEMBLE_DEFAULT;
import static org.apache.hadoop.hbase.regionserver.wal.bookkeeper.BKConstants.BK_NUM_ENSEMBLE_KEY;
import static org.apache.hadoop.hbase.regionserver.wal.bookkeeper.BKConstants.BK_NUM_QUORUM_DEFAULT;
import static org.apache.hadoop.hbase.regionserver.wal.bookkeeper.BKConstants.BK_NUM_QUORUM_KEY;
import static org.apache.hadoop.hbase.regionserver.wal.bookkeeper.BKConstants.BK_PASSWORD_DEFAULT;
import static org.apache.hadoop.hbase.regionserver.wal.bookkeeper.BKConstants.BK_PASSWORD_KEY;
import static org.apache.hadoop.hbase.regionserver.wal.bookkeeper.BKConstants.DEFAULT_LEDGER_META_ZK_QUORUMS;
import static org.apache.hadoop.hbase.regionserver.wal.bookkeeper.BKConstants.LEDGER_META_ZK_QUORUMS;
import static org.apache.hadoop.hbase.util.Bytes.SIZEOF_LONG;
import com.google.common.annotations.VisibleForTesting;
import dlshade.org.apache.bookkeeper.client.BKException;
import dlshade.org.apache.bookkeeper.client.BookKeeper;
import dlshade.org.apache.bookkeeper.client.LedgerHandle;
import dlshade.org.apache.bookkeeper.conf.ClientConfiguration;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.LedgerUtil;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;

/**
 * A class abstract the log accessing API.
 */
@InterfaceAudience.Private
public final class LedgerLogSystem {
  private static final Log LOG = LogFactory.getLog(LedgerLogSystem.class);
  private static final AtomicReference<LedgerLogSystem> instance = new AtomicReference<>();
  private static final String LOCK_SUFFIX = "lock";

  private final LogManager logManager;
  private final BookKeeper bkClient;
  private final ClientConfiguration bkConfig;
  private final RecoverableZooKeeper zookeeper;
  private final Configuration conf;
  private final String walRootPath;

  private LedgerLogSystem(Configuration conf) throws IOException, ConfigurationException {
    this(conf, LedgerUtil.getBKClientConf(conf));
  }

  private LedgerLogSystem(Configuration conf, ClientConfiguration bkConfig)
      throws IOException {
    String quorum = conf.get(LEDGER_META_ZK_QUORUMS, DEFAULT_LEDGER_META_ZK_QUORUMS);
    zookeeper = ZKUtil.connect(conf, quorum, null);
    logManager = new LogManagerZKImpl(conf, zookeeper);
    walRootPath = LedgerUtil.getLedgerRootPath(conf);

    this.conf = conf;
    this.bkClient = LedgerUtil.getBKClient(bkConfig);
    this.bkConfig = bkConfig;
    LOG.info("Open log system on zk: " + quorum);
  }

  public synchronized LedgerHandle createLog(String logPath)
      throws BKException, InterruptedException, IOException {
    LedgerHandle result = createNewLedger();

    LogMetadata logMetadata = new LogMetadata(result.getId(),
      conf.getBoolean(BKConstants.LEDGER_COMPRESSED, BKConstants.DEFAULT_LEDGER_COMPRESSED));
    createPathRecursive(logPath, logMetadata.toBytes());
    LOG.info("Created log: " + logPath + " with ledgerId: " + result.getId());
    return result;
  }

  public LedgerHandle createNewLedger() throws BKException, InterruptedException {
    return bkClient.createLedgerAdv(
      this.bkConfig.getInt(BK_NUM_ENSEMBLE_KEY, BK_NUM_ENSEMBLE_DEFAULT),
      this.bkConfig.getInt(BK_NUM_QUORUM_KEY, BK_NUM_QUORUM_DEFAULT),
      this.bkConfig.getInt(BK_NUM_ACK_KEY, BK_NUM_ACK_DEFAULT),
      BookKeeper.DigestType.valueOf(bkConfig.getString(BK_DIGEST_TYPE_KEY,
        BK_DIGEST_TYPE_DEFAULT)),
      Bytes.toBytes(bkConfig.getString(BK_PASSWORD_KEY, BK_PASSWORD_DEFAULT))
    );
  }

  // Used to create node recursively. The created parent nodes will have empty byte[].
  public void createPathRecursive(String logPath, byte[] data) throws IOException {
    String parentPath = ZKUtil.getParent(logPath);
    if (parentPath == null || logPath.equals(walRootPath)) {
      // We must have created the root path.
      return;
    }
    try {
      LOG.info("Creating log node: " + logPath);
      synchronized (this) {
        if (zookeeper.exists(parentPath, false) == null) {
          // Parent node created with empty data.
          createPathRecursive(parentPath, EMPTY_BYTE_ARRAY);
        }
      }
      List<ACL> acls = zookeeper.getAcl(parentPath, null);
      zookeeper.create(logPath, data, acls, CreateMode.PERSISTENT);
    } catch (Exception e) {
      if (e instanceof KeeperException.NodeExistsException) {
        // Other instances have created the path.
        throw new IllegalArgumentIOException("Tried to created an existing path: " + logPath);
      } else {
        throw new IOException(e);
      }
    }
  }

  public LogMetadata getLogMetadata(String logPath) throws IOException {
    return logManager.getLogMeta(logPath);
  }

  public void updateLogMetadata(String logPath, LogMetadata metadata)
      throws IOException {
    logManager.updateLogMeta(logPath, metadata);
  }

  public LedgerHandle openLedgerToRead(long ledgerId) throws IOException {
    int nTimesAttempt = 0;
    while (nTimesAttempt < 100) {
      try {
        return getLedgerFromID(ledgerId);
      } catch (BKException | InterruptedException e) {
        if (e instanceof BKException.BKLedgerRecoveryException) {
          try {
            Thread.sleep(nTimesAttempt < 3 ? 500 : 1000);
          } catch (InterruptedException ie) {
            throw (InterruptedIOException) new InterruptedIOException().initCause(ie);
          }
          nTimesAttempt++;
          continue;
        }
        String errMsg = "Failed to read ledger " + ledgerId + " this is a "
          + " fatal error.";
        throw new IOException(errMsg, e);
      }
    }
    throw new IOException("Can't open ledger id: " + ledgerId + " after "
      + nTimesAttempt + " times retry");
  }

  @VisibleForTesting
  public LedgerHandle getLedgerFromID(long ledgerId) throws BKException, InterruptedException {
    return bkClient.openLedger(ledgerId,
      BookKeeper.DigestType.valueOf(bkConfig.getString(BK_DIGEST_TYPE_KEY,
        BK_DIGEST_TYPE_DEFAULT)),
      Bytes.toBytes(bkConfig.getString(BK_PASSWORD_KEY, BK_PASSWORD_DEFAULT)));
  }

  public void deleteLog(String logPath) throws IOException {
    try {
      byte[] data = zookeeper.getData(logPath, false, null);
      // Valid metadata, pointed to a ledger. Delete the corresponding ledger.
      if (data != null && data.length >= SIZEOF_LONG) {
        LogMetadata logMetadata = new LogMetadata(data);
        for (long ledgerId : logMetadata.getLedgerIds()) {
          bkClient.deleteLedger(ledgerId);
        }
      }
      if (!logManager.removeLog(logPath)) {
        LOG.warn("Failed to remove zNode: " + logPath + " need manual cleanse.");
      }
    } catch (Exception e) {
      if (e instanceof IOException) {
        throw (IOException) e;
      } else {
        throw new IOException(e);
      }
    }
  }

  // Drop a node and all its children, if it or its child contains ledgers, all ledgers will also
  // be dropped.
  public void deleteRecursively(String logPath) throws IOException {
    try {
      if (ZKUtil.getParent(logPath) == null || !logExists(logPath) || logPath.equals(walRootPath)) {
        // Three cases just return:
        // 1. Remove '/' path.
        // 2. Remove an not existing path.
        // 3. Remove the WAL root path.
        return;
      }
      List<String> children = zookeeper.getChildren(logPath, false);
      if (!children.isEmpty()) {
        for (String child : children) {
          deleteRecursively(ZKUtil.joinZNode(logPath, child));
        }
      }
      deleteLog(logPath);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public boolean logExists(String logPath) throws IOException {
    return logManager.logExists(logPath);
  }

  public void renamePath(String oldPath, String newPath) throws IOException {
    try {
      // If the log is locked, there is still someone writing it.
      // Close it first and wait until the writing RS closed the writer.
      closeLog(oldPath);
      while (isLocked(oldPath)) {
        Thread.sleep(100);
      }
      String newPathParent = ZKUtil.getParent(newPath);
      if (zookeeper.exists(newPathParent, false) == null) {
        createPathRecursive(newPathParent, EMPTY_BYTE_ARRAY);
      }
      List<String> children = zookeeper.getChildren(oldPath, false);
      if (children.isEmpty()) {
        // No child, safe to rename.
        logManager.renameLog(oldPath, newPath);
        LOG.info("Renamed logPath : " + oldPath + " to " + newPath);
      } else {
        // Have child, copy children.
        for (String child : children) {
          renamePath(ZKUtil.joinZNode(oldPath, child), ZKUtil.joinZNode(newPath, child));
        }
        // Then delete the old path.
        zookeeper.delete(oldPath, -1);
        LOG.info("Delete oldPath " + oldPath);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public List<String> getLogUnderPath(String logPath) throws IOException {
    return logManager.getLogUnderPath(logPath);
  }

  // For singleton use.
  public static synchronized LedgerLogSystem getInstance(Configuration conf)
      throws IOException {
    try {
      if (instance.get() == null) {
        instance.set(new LedgerLogSystem(conf));
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
    return instance.get();
  }

  @VisibleForTesting
  public static synchronized LedgerLogSystem setInstance(Configuration conf,
      ClientConfiguration bkConfig) throws IOException, BKException, InterruptedException {
    return setInstance(new LedgerLogSystem(conf, bkConfig));
  }

  @VisibleForTesting
  public static LedgerLogSystem setInstance(LedgerLogSystem system) {
    instance.set(system);
    return instance.get();
  }

  // Return the length of the log in bytes.
  public long getLogDataSize(String logPath) throws IOException {
    if (!logExists(logPath)) {
      return 0;
    }
    LogMetadata logMetadata = getLogMetadata(logPath);
    return logMetadata.getDataSize();
  }

  public void watchLogPath(String logPath, Watcher watcher)
      throws InterruptedException, KeeperException {
    zookeeper.exists(logPath, watcher);
  }

  public boolean isLedgerClosed(long ledgerId) throws IOException {
    try {
      return bkClient.isClosed(ledgerId);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public boolean tryLockPath(String logPath) {
    try {
      if (!logExists(logPath)) {
        return false;
      }
      zookeeper.create(ZKUtil.joinZNode(logPath, LOCK_SUFFIX), new byte[0],
        zookeeper.getAcl(logPath, null), CreateMode.EPHEMERAL);
      return true;
    } catch (Exception e) {
      LOG.warn("Failed lock logPath: " + logPath, e);
      return false;
    }
  }

  public boolean unlockPath(String logPath) {
    String lockPath = ZKUtil.joinZNode(logPath, LOCK_SUFFIX);
    try {
      if (isLocked(logPath)) {
        zookeeper.delete(lockPath, -1);
        return true;
      }
    } catch (InterruptedException | KeeperException e) {
      LOG.warn("Failed unlock logPath: " + logPath, e);
    }
    return false;
  }

  public boolean isLocked(String logPath) throws InterruptedException, KeeperException {
    String lockPath = ZKUtil.joinZNode(logPath, LOCK_SUFFIX);
    return zookeeper.exists(lockPath, false) != null;
  }

  public void closeLog(String logPath) throws IOException {
    LogMetadata metadata = getLogMetadata(logPath);
    if (!metadata.isClosed()) {
      metadata.setClosed();
      updateLogMetadata(logPath, metadata);
    }
  }

  public byte[] getDataOfPath(String logPath) throws InterruptedException, KeeperException {
    return zookeeper.getData(logPath, false, null);
  }
}
