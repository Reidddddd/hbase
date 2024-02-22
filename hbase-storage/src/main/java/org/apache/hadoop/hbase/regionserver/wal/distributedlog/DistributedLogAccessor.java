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

package org.apache.hadoop.hbase.regionserver.wal.distributedlog;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;
import dlshade.org.apache.distributedlog.DistributedLogConfiguration;
import dlshade.org.apache.distributedlog.api.namespace.Namespace;
import dlshade.org.apache.distributedlog.api.namespace.NamespaceBuilder;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;

/**
 * A singleton to access a distributed log Namespace.
 */
@InterfaceAudience.Private
public final class DistributedLogAccessor implements Closeable {
  private static final Log LOG = LogFactory.getLog(DistributedLogAccessor.class);
  private static final Object obj = new Object();

  private static volatile DistributedLogAccessor instance = null;

  private static final String DISTRIBUTED_LOG_ZK_QUORUM = "distributedlog.zk.quorum";
  private static final String DEFAULT_DISTRIBUTED_LOG_ZK_QUORUM = "localhost:2181";
  private static final String DISTRIBUTED_LOG_ZNODE_PARENT = "distributedlog.znode.parent";
  private static final String DEFAULT_DISTRIBUTED_LOG_ZNODE_PARENT = "/messaging/WALs";
  private static final String DISTRIBUTED_LOG_CONF_FILE_PATH = "distributelog.conf.path";
  private static final String DEFAULT_DISTRIBUTED_LOG_CONF_FILE_PATH = "./dlconfig";

  private final Configuration conf;
  private final String zkAddress;
  private final String zkRoot;
  private final DistributedLogConfiguration distributedLogConfiguration;
  private final Namespace namespace;

  private DistributedLogAccessor(Configuration conf) throws Exception {
    this.conf = conf;
    this.zkAddress = conf.get(DISTRIBUTED_LOG_ZK_QUORUM, DEFAULT_DISTRIBUTED_LOG_ZK_QUORUM);
    this.zkRoot = conf.get(DISTRIBUTED_LOG_ZNODE_PARENT, DEFAULT_DISTRIBUTED_LOG_ZNODE_PARENT);

    // Set distributed log conf properties.
    distributedLogConfiguration = new DistributedLogConfiguration();

    try {
      distributedLogConfiguration.loadConf(
        new File(conf.get(DISTRIBUTED_LOG_CONF_FILE_PATH, DEFAULT_DISTRIBUTED_LOG_CONF_FILE_PATH))
          .toURI().toURL());
    } catch (Exception e) {
      LOG.warn("Failed load distributedlog configuration from local, use default properties");
    }

    NamespaceBuilder namespaceBuilder = NamespaceBuilder.newBuilder();
    namespaceBuilder.conf(distributedLogConfiguration);
    ensureZNodeExists(zkRoot);
    // Set URI
    URI uri = new URI("distributedlog-bk://" + zkAddress.replace(',', ';') + zkRoot);
    namespaceBuilder.uri(uri);
    namespace = namespaceBuilder.build();
    LOG.info("Finished initializing namespace of distributed log: " + uri);
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

  public static String getDistributedLogStreamName(Configuration conf) {
    if (instance == null) {
      try {
        return getInstance(conf).distributedLogConfiguration.getUnpartitionedStreamName();
      } catch (Exception e) {
        throw new RuntimeException("Failed access distributedlog with exception: ", e);
      }
    }
    return instance.distributedLogConfiguration.getUnpartitionedStreamName();
  }
}
