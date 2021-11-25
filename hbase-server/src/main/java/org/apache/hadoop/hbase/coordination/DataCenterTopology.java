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

package org.apache.hadoop.hbase.coordination;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.util.Shell;

import java.io.IOException;

/**
 * For {@link ZkSplitLogWorkerCoordination} to determine whether a worker should
 * grab and execute a {@link org.apache.hadoop.hbase.SplitLogTask} based on data center location.
 */
@InterfaceAudience.Private
public class DataCenterTopology {
  private static final Log LOG = LogFactory.getLog(DataCenterTopology.class);
  protected static final String UNRESOLVABLE = "UNRESOLVABLE";

  private Shell.ShellCommandExecutor topoScript;
  protected String localDataCenter;

  public DataCenterTopology(Configuration conf, String localhostname) {
    // At here, means `hbase.regionserver.splitlog.check.topology` is true
    String script = conf.get("hbase.regionserver.splitlog.topology.resolver.script");
    if (script == null || script.isEmpty()) {
      throw new IllegalArgumentException("hbase.regionserver.splitlog.check.topology set true, " +
          "but hbase.regionserver.splitlog.topology.resolver.script is unset.");
    }

    topoScript = new Shell.ShellCommandExecutor(
        new String[] { script, "" }, null, null,
        conf.getLong("hbase.regionserver.splitlog.topology.resolver.script.timeout", 500) // 0.5 seconds
    );
    localDataCenter = getDataCenter(localhostname);
    if (localDataCenter.equals(UNRESOLVABLE)) {
      throw new IllegalArgumentException("SplitLog check topology is enable, but " + localhostname
          + "'s topology is unresolvable. Please check your script.");
    }
    LOG.info(localhostname + " is in " + localDataCenter);
  }

  /**
   * Data center must be resolvable at the init stage, we could skip many empty or null check here.
   * @param hostname hostname of remote server
   * @return its data center location
   */
  public String getDataCenter(String hostname) {
    if (hostname == null || hostname.isEmpty()) {
      return UNRESOLVABLE;
    }
    String[] exec = topoScript.getExecString();
    exec[1] = hostname;
    try {
      topoScript.execute();
    } catch (IOException e) {
      LOG.error(e.getMessage());
      return UNRESOLVABLE;
    }

    String topo = topoScript.getOutput().trim();
    if (topo.isEmpty()) {
      return UNRESOLVABLE;
    }
    if (!topo.startsWith("/")) {
      LOG.warn("The topology format is wrong");
      return UNRESOLVABLE;
    }
    String[] tokens = topo.split("/");
    // token[0] must be "", since we already check the startWith("/")
    String datacenter = tokens[1];
    return datacenter.isEmpty() ? UNRESOLVABLE : datacenter;
  }

  public boolean isResolvable(String dataCenter) {
    return !UNRESOLVABLE.equals(dataCenter);
  }

  public boolean isInSameDataCenter(String dataCenter) {
    return localDataCenter.equals(dataCenter);
  }

  public String getLocalDataCenter() {
    return localDataCenter;
  }

}
