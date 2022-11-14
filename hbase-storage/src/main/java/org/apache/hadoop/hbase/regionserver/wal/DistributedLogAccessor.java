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

package org.apache.hadoop.hbase.regionserver.wal;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.distributedlog.shaded.DistributedLogConfiguration;
import org.apache.distributedlog.shaded.api.namespace.Namespace;
import org.apache.distributedlog.shaded.api.namespace.NamespaceBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;

/**
 * A singleton to access a distributed log Namespace.
 */
@InterfaceAudience.Private
public class DistributedLogAccessor implements Closeable {
  private static final Log LOG = LogFactory.getLog(DistributedLogAccessor.class);
  private static final Object obj = new Object();

  private static volatile DistributedLogAccessor instance = null;

  private static final String DISTRIBUTED_LOG_ZK_QUORUM = "distributedlog.zk.quorum";
  private static final String DEFAULT_DISTRIBUTED_LOG_ZK_QUORUM = "localhost:2181";
  private static final String DISTRIBUTED_LOG_ZNODE_PARENT = "distributedlog.znode.parent";
  private static final String DEFAULT_DISTRIBUTED_LOG_ZNODE_PARENT = "/messaging/WALs";
  private static final String DISTRIBUTED_LOG_STREAM_NAME = "distributedlog.stream.name";
  private static final String DEFAULT_DISTRIBUTED_LOG_STREAM_NAME = "logs";

  private final Configuration conf;
  private final String zkAddress;
  private final String zkRoot;
  private final DistributedLogConfiguration distributedLogConfiguration;
  private final String streamName;
  private final NamespaceBuilder namespaceBuilder;
  private final Namespace namespace;

  private DistributedLogAccessor(Configuration conf) throws Exception {
    this.conf = conf;
    this.zkAddress = conf.get(DISTRIBUTED_LOG_ZK_QUORUM, DEFAULT_DISTRIBUTED_LOG_ZK_QUORUM);
    this.zkRoot = conf.get(DISTRIBUTED_LOG_ZNODE_PARENT, DEFAULT_DISTRIBUTED_LOG_ZNODE_PARENT);
    this.streamName = conf.get(DISTRIBUTED_LOG_STREAM_NAME, DEFAULT_DISTRIBUTED_LOG_STREAM_NAME);
    // Set distributed log conf properties.
    distributedLogConfiguration = new DistributedLogConfiguration();
    distributedLogConfiguration.setCreateStreamIfNotExists(true);
    distributedLogConfiguration.setUnpartitionedStreamName(streamName);
    namespaceBuilder = NamespaceBuilder.newBuilder();
    namespaceBuilder.conf(distributedLogConfiguration);
    ensureZNodeExists(zkRoot);
    // Set URI
    URI uri = new URI("distributedlog-bk://" + zkAddress.replace(',', ';') + zkRoot);
    namespaceBuilder.uri(uri);
    namespace = namespaceBuilder.build();
  }

  private void ensureZNodeExists(String zNode) throws Exception {
    ZooKeeper zkc = new ZooKeeper(zkAddress, 60000, null);
    if (zkc.exists(zNode, false) == null) {
      zkc.create(zNode, new byte[0], OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
  }

  public static DistributedLogAccessor getInstance(Configuration conf) throws Exception {
    // Singleton Double check
    if (instance == null) {
      synchronized (obj) {
        if (instance == null) {
          instance = new DistributedLogAccessor(conf);
        }
      }
    }
    return instance;
  }

  /**
   * Close the unique namespace. Should be only called when the server is shutdown.
   */
  @Override
  public void close() throws IOException {
    this.namespace.close();
  }

  public Namespace getNamespace() {
    return this.namespace;
  }
}
