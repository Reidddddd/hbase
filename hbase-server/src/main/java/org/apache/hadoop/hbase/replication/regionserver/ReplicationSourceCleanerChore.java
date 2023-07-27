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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class ReplicationSourceCleanerChore extends ScheduledChore {
  
  private static final Log LOG = LogFactory.getLog(ReplicationSourceCleanerChore.class);
  private static final String REPLICATION_SOURCE_CLEANER_CHORE_NAME = "ReplicationSourceCleaner";
  private static final String REPLICATION_SOURCE_CLEANER_INTERVAL =
      "replication.source.cleaner.interval";
  private static final int REPLICATION_SOURCE_CLEANER_DEFAULT_INTERVAL =
      600 * 1000; // Default 10 min
  private final ReplicationSourceManager manager;
  
  /**
   * Construct Snapshot Cleaner Chore with parameterized constructor
   *
   * @param stopper When {@link Stoppable#isStopped()} is true, this chore will cancel and cleanup
   * @param replicationSourceManager SnapshotManager instance to manage lifecycle of snapshot
   */
  ReplicationSourceCleanerChore(Stoppable stopper, Configuration configuration,
                                ReplicationSourceManager replicationSourceManager) {
    super(REPLICATION_SOURCE_CLEANER_CHORE_NAME, stopper,
        configuration.getInt(
            REPLICATION_SOURCE_CLEANER_INTERVAL, REPLICATION_SOURCE_CLEANER_DEFAULT_INTERVAL));
    this.manager = replicationSourceManager;
  }
  
  @Override
  protected void chore() {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Replication Source Cleaner Chore is starting up...");
    }
    manager.cleanUnusedSource();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Replication Source Cleaner Chore is closing...");
    }
  }
}
