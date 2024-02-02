/**
 * Copyright The Apache Software Foundation
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

package org.apache.hadoop.hbase.rsgroup;

import java.io.IOException;
import java.util.Map;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.net.Address;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class RSGroupMXBeanImpl implements RSGroupMXBean {
  private static final String INTERNAL_HOSTNAME_DELIMITER = "-";
  private static final String CPU_ATTRIBUTE_KEY = "cpu";
  private static final String RAM_ATTRIBUTE_KEY = "ram";

  private static RSGroupMXBeanImpl instance = null;

  private RSGroupAdmin groupAdmin;
  private MasterServices master;

  public synchronized static RSGroupMXBeanImpl init(
      final RSGroupAdmin groupAdmin,
      MasterServices master) {
    if (instance == null) {
      instance = new RSGroupMXBeanImpl(groupAdmin, master);
    }
    return instance;
  }

  protected RSGroupMXBeanImpl(final RSGroupAdmin groupAdmin,
                              MasterServices master) {
    this.groupAdmin = groupAdmin;
    this.master = master;
  }

  @Override
  public Map<String, String> getServersByGroup() throws IOException {
    Map<String, String> data = new HashedMap();
    for (final ServerName entry :
            master.getServerManager().getOnlineServersList()) {
      RSGroupInfo groupInfo = groupAdmin.getRSGroupOfServer(
              Address.fromParts(entry.getHostname(), entry.getPort()));
      data.put("rs_" + entry.getAddress().getHostname() + "_rsgroup_" + groupInfo.getName(), "0");
    }
    return data;
  }

  // The k8s pod name are in the format rsgorup-cpu-ram--cluster-uid.
  // We just split the name and parse the first three fields.
  @Override
  public Map<String, Map<String, Float>> getK8sResourceUsageByGroup() throws IOException {
    Map<String, Map<String, Float>> data = new HashedMap();
    ServerManager serverManager = master.getServerManager();

    for (final ServerName entry : serverManager.getPodInstances()) {
      String internalName = entry.getInternalHostName();
      String[] attributes = internalName.split(INTERNAL_HOSTNAME_DELIMITER);
      String groupName = attributes[0];
      float cpuCores = Float.parseFloat(attributes[1]);
      attributes[2] = attributes[2].toLowerCase();
      float ramUsage = Float.parseFloat(attributes[2].substring(0, attributes[2].indexOf("gi")));

      if (!data.containsKey(groupName)) {
        data.put(groupName, new HashedMap());
      }

      Map<String, Float> subMap = data.get(groupName);
      subMap.put(CPU_ATTRIBUTE_KEY, subMap.getOrDefault(CPU_ATTRIBUTE_KEY, 0f) + cpuCores);
      subMap.put(RAM_ATTRIBUTE_KEY, subMap.getOrDefault(RAM_ATTRIBUTE_KEY, 0f) + ramUsage);
    }
    return data;
  }

}
