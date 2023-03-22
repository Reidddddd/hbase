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

import dlshade.org.apache.distributedlog.api.namespace.Namespace;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.DistributedLogAccessor;
import org.apache.hadoop.hbase.wal.WALUtils;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class DistributedLogCleaner extends AbstractCleanerChore {
  private static final Log LOG = LogFactory.getLog(DistributedLogCleaner.class);

  private final Namespace walNamespace;
  private final Configuration conf;
  private final Path oldLogDir;

  /**
   * @param p         the period of time to sleep between each run
   * @param s         the stopper
   * @param conf      configuration to use
   * @param oldLogDir the path to the archived logs
   */
  public DistributedLogCleaner(int p, Stoppable s, Configuration conf, Path oldLogDir)
    throws IOException {
    super("DistributedLogCleaner", s, p);
    this.oldLogDir = oldLogDir;
    this.conf = conf;
    try {
      this.walNamespace = DistributedLogAccessor.getInstance(conf).getNamespace();
    } catch (Exception e) {
      // Wrap to IOE and throw.
      throw new IOException(e);
    }
  }

  public DistributedLogCleaner(int p, Stoppable s, Configuration conf, Path oldLogDir,
      Namespace walNamespace) {
    super("DistributedLogCleaner", s, p);
    this.oldLogDir = oldLogDir;
    this.conf = conf;
    this.walNamespace = walNamespace;
  }

  @Override
  protected void chore() {
    runCleaner();
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    // Leave here for future features.
  }

  @Override
  public boolean runCleaner() {
    try {
      WALUtils.deleteLogsUnderPath(walNamespace, WALUtils.pathToDistributedLogName(this.oldLogDir),
        DistributedLogAccessor.getDistributedLogStreamName(conf), false);
    } catch (IOException e) {
      LOG.warn("Failed to clean the old logs: skip it for now.", e);
      return false;
    }
    return true;
  }

  // This method should be only invoked after a table is already deleted.
  public void cleanDeletedTable(TableName tableName) {
    String tablePath = String.join("/", tableName.getNamespaceAsString(),
      tableName.getNameAsString());
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleting logs under path: " + tablePath);
      }
      WALUtils.deleteLogsUnderPath(walNamespace, tablePath,
        DistributedLogAccessor.getDistributedLogStreamName(conf), true);
    } catch (IOException e) {
      LOG.warn("Failed delete distributedlog of table: " + tableName + " skip for now, "
        + "the table should be already removed the distributedlog need to removed manually.");
    }
  }
}
