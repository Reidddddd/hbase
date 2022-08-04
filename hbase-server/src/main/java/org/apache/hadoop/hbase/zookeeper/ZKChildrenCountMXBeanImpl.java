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

package org.apache.hadoop.hbase.zookeeper;

import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.zookeeper.KeeperException;
import java.io.IOException;
import java.util.Map;

public class ZKChildrenCountMXBeanImpl implements ZKChildrenCountMXBean {

  private static ZKChildrenCountMXBeanImpl instance = null;
  private MasterServices master;
  private Map<String, Integer> zkCountMap;
  private long lastTime;
  private ZooKeeperWatcher zkw;

  public ZKChildrenCountMXBeanImpl(MasterServices master) {
    this.master = master;
    zkw = master.getZooKeeper();
  }

  public synchronized static ZKChildrenCountMXBeanImpl init(
          MasterServices master) {
    if (instance == null) {
      instance = new ZKChildrenCountMXBeanImpl(master);
    }
    return instance;
  }

  @Override
  public Map<String, Integer> getZKNodeCountInfo() throws IOException {
    long currentTime = System.currentTimeMillis();
    if (null == zkCountMap || (currentTime - lastTime >= 10 * 60 * 1000)) {
      try {
        zkCountMap = ZKUtil.getAllNodeNumber(zkw, zkw.baseZNode);
      } catch (KeeperException e) {
        throw new IOException(e);
      }
      lastTime = currentTime;
    }
    return zkCountMap;
  }
}
