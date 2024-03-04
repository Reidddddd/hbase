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
package org.apache.hadoop.hbase.master.cleaner;

import java.io.IOException;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.regionserver.wal.bookkeeper.LedgerLogSystem;
import org.apache.hadoop.hbase.util.LedgerUtil;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class LedgerLogCleaner extends AbstractCleanerChore {
  private static final Log LOG = LogFactory.getLog(LedgerLogCleaner.class);

  private final LedgerLogSystem logSystem;
  private final Configuration conf;
  private final String rootPath;
  private final String archivePath;

  public LedgerLogCleaner(Configuration conf, String name, Stoppable stopper, int period)
      throws IOException {
    this(conf, name, stopper, period, LedgerLogSystem.getInstance(conf));
  }

  LedgerLogCleaner(Configuration conf, String name, Stoppable stopper, int period,
      LedgerLogSystem ledgerLogSystem) throws IOException {
    super(name, stopper, period);
    logSystem = ledgerLogSystem;
    rootPath = LedgerUtil.getLedgerRootPath(conf);
    archivePath = LedgerUtil.getLedgerArchivePath(rootPath);
    this.conf = conf;
  }

  @Override
  protected void chore() {
    runCleaner();
  }

  @Override
  public void onConfigurationChange(Configuration conf) {

  }

  @Override
  public boolean runCleaner() {
    List<String> logs = null;
    try {
      logs = logSystem.getLogUnderPath(archivePath);
    } catch (IOException e) {
      LOG.warn("Failed to get logs to clean, skip for this time.", e);
      return false;
    }
    for (String log : logs) {
      String logName = ZKUtil.joinZNode(archivePath, log);
      try {
        logSystem.deleteLog(logName);
      } catch (IOException e) {
        LOG.warn("Failed delete log: " + logName + " skip it this time.", e);
      }
    }
    return true;
  }
}
