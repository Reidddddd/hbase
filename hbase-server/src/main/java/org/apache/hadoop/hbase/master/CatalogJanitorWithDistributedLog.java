/**
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
package org.apache.hadoop.hbase.master;

import dlshade.org.apache.distributedlog.api.namespace.Namespace;
import dlshade.org.apache.distributedlog.exceptions.LogNotFoundException;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.wal.DistributedLogAccessor;
import org.apache.hadoop.hbase.wal.WALUtils;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class CatalogJanitorWithDistributedLog extends CatalogJanitor {
  private static final Log LOG = LogFactory.getLog(CatalogJanitorWithDistributedLog.class);

  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private Namespace walNamespace;

  CatalogJanitorWithDistributedLog(Server server, MasterServices services) {
    super(server, services);
    try {
      walNamespace = DistributedLogAccessor.getInstance(server.getConfiguration()).getNamespace();
    } catch (Exception e) {
      String msg = "Failed initializing CatalogJanitor for DistributedLog, aborting. ";
      server.abort(msg, e);
    }
  }

  @Override
  boolean cleanMergeRegion(final HRegionInfo mergedRegion,
    final HRegionInfo regionA, final HRegionInfo regionB) throws IOException {
    boolean success = super.cleanMergeRegion(mergedRegion, regionA, regionB);
    if (success) {
      deleteRegionPath(regionA);
      deleteRegionPath(regionB);
    }
    return success;
  }

  @Override
  boolean cleanParent(final HRegionInfo parent, Result rowContent) throws IOException {
    boolean success = super.cleanParent(parent, rowContent);
    if (success) {
      deleteRegionPath(parent);
    }
    return success;
  }

  private void deleteRegionPath(HRegionInfo regionInfo) throws IOException {
    executor.submit(() -> {
      TableName tableName = regionInfo.getTable();
      String logPath = WALUtils.getDistributedLogRegionPath(tableName.getNamespaceAsString(),
        tableName.getNameAsString(), regionInfo.getRegionNameAsString());
      try {
        walNamespace.deleteLog(logPath);
      } catch (LogNotFoundException e) {
        // Do nothing
      } catch (IOException ioe) {
        LOG.warn("Failed to delete log " + logPath);
      }
    });
  }
}
