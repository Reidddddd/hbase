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
package org.apache.hadoop.hbase.util;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServer;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServerWrapperStub;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

public final class TestJvmPauseMonitor {

  private TestJvmPauseMonitor() {

  }

  /**
   * Simple 'main' to facilitate manual testing of the pause monitor.
   *
   * This main function just leaks memory into a list. Running this class
   * with a 1GB heap will very quickly go into "GC hell" and result in
   * log messages about the GC pauses.
   */
  public static void main(String []args) throws Exception {
    MetricsRegionServerWrapperStub wrapper = new MetricsRegionServerWrapperStub();
    MetricsRegionServer rsm = new MetricsRegionServer(
      wrapper, new Configuration(false), null);
    MetricsRegionServerSource serverSource = rsm.getMetricsSource();
    new JvmPauseMonitor(new Configuration(), serverSource).start();
    List<String> list = Lists.newArrayList();
    int i = 0;
    while (true) {
      list.add(String.valueOf(i++));
    }
  }
}
