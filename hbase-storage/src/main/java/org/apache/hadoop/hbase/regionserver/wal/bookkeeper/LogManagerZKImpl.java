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

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;

/**
 * A class manage the mapping between ledgers and HBase log.
 */
@InterfaceAudience.Private
public class LogManagerZKImpl implements LogManager {
  private static final Log LOG = LogFactory.getLog(LogManagerZKImpl.class);

  private final Configuration conf;

  private RecoverableZooKeeper zookeeper;

  public LogManagerZKImpl(Configuration conf, RecoverableZooKeeper zookeeper) {
    this.conf = conf;
    this.zookeeper = zookeeper;
  }

  @Override
  public boolean logExists(String logName) throws IOException {
    try {
      return zookeeper.exists(logName, false) != null;
    } catch (Exception e) {
      LOG.warn("Failed check existence for log: " + logName);
      throw new IOException(e);
    }
  }

  @Override
  public boolean renameLog(String oldName, String newName) throws IOException {
    try {
      List<ACL> acls = zookeeper.getAcl(ZKUtil.getParent(newName), null);
      byte[] data = zookeeper.getData(oldName, null, null);
      zookeeper.create(newName, data, acls, CreateMode.PERSISTENT);
      zookeeper.delete(oldName, -1);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
    return true;
  }

  @Override
  public void createLog(String logName, byte[] data) throws IOException {
    try {
      zookeeper.create(logName, data, null, CreateMode.PERSISTENT);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean removeLog(String logName) throws IOException {
    try {
      zookeeper.delete(logName, -1);
      return true;
    } catch (Exception e) {
      if (e instanceof KeeperException.NoNodeException) {
        LOG.warn("Try to delete log: " + logName + " but it is already removed.");
      } else {
        throw new IOException(e);
      }
    }
    return false;
  }

  @Override
  public LogMetadata getLogMeta(String logName) throws IOException {
    try {
      byte[] data = zookeeper.getData(logName, false, null);
      return new LogMetadata(data);
    } catch (Exception e) {
      String errMsg = "Failed to get ledger of log: " + logName + " this is a fatal inconsistency.";
      throw new IOException(errMsg, e);
    }
  }

  @Override
  public void updateLogMeta(String logName, LogMetadata metadata) throws IOException {
    byte[] data = metadata.toBytes();
    try {
      zookeeper.setData(logName, data, -1);
    } catch (InterruptedException | KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void addWatchOnLogDelete(String logName, Runnable task) throws IOException {
    try {
      zookeeper.getData(logName, watchedEvent -> {
        if (watchedEvent.getType().equals(Watcher.Event.EventType.NodeDeleted)) {
          task.run();
        }
      }, null);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<String> getLogUnderPath(String logParentPath) throws IOException {
    try {
      return zookeeper.getChildren(logParentPath, false);
    } catch (KeeperException | InterruptedException e) {
      if (e instanceof KeeperException.NoNodeException) {
        return Lists.newArrayListWithExpectedSize(0);
      }
      throw new IOException(e);
    }
  }
}
