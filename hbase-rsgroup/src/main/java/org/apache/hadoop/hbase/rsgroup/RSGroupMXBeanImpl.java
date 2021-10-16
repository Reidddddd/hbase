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

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.hbase.ServerName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.net.Address;

import java.io.IOException;
import java.util.Map;

@InterfaceAudience.Private
public class RSGroupMXBeanImpl implements RSGroupMXBean {

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

}
