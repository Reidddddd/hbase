/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.coprocessor;

import com.google.common.collect.Sets;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.rsgroup.RSGroupAdminClient;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A coprocessor to move the RS to a backup rsgroup and wait all the regions on it moved away.
 */
@InterfaceAudience.Private
public class RegionServerGracefulStopper extends BaseRegionServerObserver {
  private static final Log LOG = LogFactory.getLog(RegionServerGracefulStopper.class);
  private static final String HBASE_BACKUP_GROUP = "hbase.rs.graceful.stopper.backup.group";
  private static final String DEFAULT_BACKUP_GROUP_NAME = "backup";
  private RSGroupAdminClient rsGroupAdminClient = null;
  private RegionServerServices regionServer = null;
  private String backupGroup = null;

  @Override
  public void preStopRegionServer(ObserverContext<RegionServerCoprocessorEnvironment> env)
    throws IOException {
    if (rsGroupAdminClient != null && backupGroup != null) {
      LOG.info("Start moving regions gracefully.");
      RSGroupInfo origin =
        rsGroupAdminClient.getRSGroupOfServer(regionServer.getServerName().getAddress());
      try {
        rsGroupAdminClient.moveServers(Sets.newHashSet(regionServer.getServerName().getAddress()),
          backupGroup);
      } catch (Exception e) {
        // We do not have time to handle any exception. Just return.
        return;
      }
      int checkTimes = 0;

      // We check online regions every 10 seconds and wait maximum 1200 seconds to wait regions
      // moving away.
      // Do not worry about long time waiting, the pod will be recycled right away when we finish
      // stopping.
      while (!regionServer.getOnlineRegions().isEmpty() && checkTimes < 120) {
        // Regions are still moving. Wait.
        try {
          checkTimes++;
          Thread.sleep(10000);
        } catch (Exception e) {
          // We got exception cannot wait the region move. Stop.
          break;
        }
      }
      try {
        // Balance the original group to eliminate the region skew.
        if (origin != null) {
          rsGroupAdminClient.balanceRSGroup(origin.getName());
        }
      } catch (Exception e) {
        // We do not have time to handle this. Skip.
        LOG.info("Failed balancing group: " + origin, e);
      }
    }
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    RegionServerCoprocessorEnvironment rsEnv = (RegionServerCoprocessorEnvironment) env;
    regionServer = rsEnv.getRegionServerServices();
    if (regionServer instanceof HRegionServer) {
      if (((HRegionServer) regionServer).isK8sModeEnabled()) {
        rsGroupAdminClient = new RSGroupAdminClient(regionServer.getConnection());
        backupGroup = regionServer.getConfiguration().get(HBASE_BACKUP_GROUP,
          DEFAULT_BACKUP_GROUP_NAME);
        LOG.info("Graceful stopper initialized with backup group: " + backupGroup);
      }
    }
  }

}
