/*
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
package org.apache.hadoop.hbase.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthFailingRecoverableZooKeeper extends RecoverableZooKeeper {
  private static final Logger LOG = LoggerFactory.getLogger(AuthFailingRecoverableZooKeeper.class);
  private Watcher watcher;
  private int sessionTimeout;
  private String quorumServers;

  public AuthFailingRecoverableZooKeeper(String quorumServers, int sessionTimeout, Watcher watcher,
      int maxRetries, int retryIntervalMillis, int maxSleepTime, String identifier,
      int authFailedRetries, int authFailedPause, int multiMaxSize) throws IOException {
    super(quorumServers, sessionTimeout, watcher, maxRetries, retryIntervalMillis, maxSleepTime,
        identifier, authFailedRetries, authFailedPause, multiMaxSize);
    this.quorumServers = quorumServers;
    this.sessionTimeout = sessionTimeout;
    this.watcher = watcher;
  }

  @Override
  ZooKeeper createNewZooKeeper() throws KeeperException {
    try {
      // Construct our "special" ZooKeeper instance
      return new AuthFailingZooKeeper(quorumServers, sessionTimeout, watcher);
    } catch (IOException ex) {
      LOG.warn("Unable to create ZooKeeper Connection", ex);
      throw new KeeperException.OperationTimeoutException();
    }
  }
}
