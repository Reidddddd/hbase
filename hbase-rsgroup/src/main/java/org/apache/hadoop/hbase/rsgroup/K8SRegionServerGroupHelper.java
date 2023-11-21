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
package org.apache.hadoop.hbase.rsgroup;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A thread continuously move server to target rsgroup.
 */
@InterfaceAudience.Private
public class K8SRegionServerGroupHelper extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(K8SRegionServerGroupHelper.class);
  private final BlockingQueue<RSEntry> entryQueue = new LinkedBlockingQueue<>();

  private final RSGroupInfoManager rsGroupInfoManager;

  static class RSEntry {
    private Address rsAddress;
    private String targetGroup;
    private EntryType type;

    public RSEntry(Address rsAddress, String targetGroup, EntryType type) {
      this.rsAddress = rsAddress;
      this.targetGroup = targetGroup;
      this.type = type;
    }
  }

  enum EntryType {
    STARTUP, REMOVE
  }

  public K8SRegionServerGroupHelper(RSGroupInfoManager rsGroupInfoManager) {
    this.rsGroupInfoManager = rsGroupInfoManager;
    this.setDaemon(true);
  }

  @Override
  public void run() {
    RSEntry entry = null;
    while (true) {
      try {
        entry = entryQueue.take();
        switch(entry.type) {
          case STARTUP:
            if (entry.targetGroup.equals(RSGroupInfo.DEFAULT_GROUP)) {
              continue;
            }

            RSGroupInfo currentGroup = null;
            // The new started rs maybe not be moved to default group.
            // So that we could have null here. Just wait some time for default moving.
            while (currentGroup == null) {
              currentGroup = rsGroupInfoManager.getRSGroupOfServer(entry.rsAddress);
              Thread.sleep(100);
            }

            if (currentGroup.getName().equals(entry.targetGroup) || entry.targetGroup == null) {
              continue;
            }
            RSGroupInfo targetGroupInfo = rsGroupInfoManager.getRSGroup(entry.targetGroup);
            if (targetGroupInfo == null) {
              // The group does not exist. Create it.
              rsGroupInfoManager.addRSGroup(new RSGroupInfo(entry.targetGroup));
            }

            rsGroupInfoManager.moveServers(Sets.newHashSet(entry.rsAddress), currentGroup.getName(),
              entry.targetGroup);
            break;

          case REMOVE:
            rsGroupInfoManager.removeServers(Sets.newHashSet(entry.rsAddress));
            break;

          default:
            break;
        }
      } catch (InterruptedException | IOException e) {
        // No harm to our cluster. So do nothing.
        if (entry != null) {
          LOG.warn("Failed dispatch rs: " + entry.rsAddress + " to target group: "
            + entry.targetGroup, e);
        }
      }
    }
  }

  public void dispatch(Address rsAddress, String targetGroup) {
    try {
      entryQueue.put(new RSEntry(rsAddress, targetGroup, EntryType.STARTUP));
    } catch(InterruptedException ie) {
      // No harm to our cluster. So do nothing but leave a log.
      LOG.warn("Failed dispatch rs: " + rsAddress + " to target group: " + targetGroup);
    }
  }

  public void remove(Address rsAddress) {
    try {
      entryQueue.put(new RSEntry(rsAddress, null, EntryType.REMOVE));
    } catch (InterruptedException ie) {
      // No harm to our cluster. So do nothing but leave a log.
      LOG.warn("Failed remove rs: " + rsAddress + " from rsgorup.");
    }
  }
}
