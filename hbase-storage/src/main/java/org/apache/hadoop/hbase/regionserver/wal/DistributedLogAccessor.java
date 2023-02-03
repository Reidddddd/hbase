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
import dlshade.org.apache.distributedlog.DistributedLogConfiguration;
import dlshade.org.apache.distributedlog.api.namespace.Namespace;
import dlshade.org.apache.distributedlog.api.namespace.NamespaceBuilder;
import java.io.Closeable;
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
  private static final String DISTRIBUTED_LOG_WRITE_BUFFER_SIZE =
    "distributedlog.write.buffer.size";
  private static final int DEFAULT_DISTRIBUTED_LOG_WRITE_BUFFER_SIZE = 1024;
  private static final String DISTRIBUTED_LOG_ZK_REQUEST_RATE = "distributedlog.zk.request.rate";
  private static final int DEFAULT_DISTRIBUTED_ZK_REQUEST_RATE = 0;
  private static final String DISTRIBUTED_LOG_IMMEDIATE_FLUSH = "distributedlog.immediate.flush";
  private static final boolean DEFAULT_DISTRIBUTED_LOG_IMMEDIATE_FLUSH = false;
  private static final String DISTRIBUTED_LOG_COMPRESSION_TYPE = "distributedlog.compression.type";
  private static final String DEFAULT_DISTRIBUTED_LOG_COMPRESSION_TYPE = "none";
  private static final String DISTRIBUTED_LOG_IMMEDIATE_FLUSH_INTERVAL =
    "distributedlog.immediate.flush.interval";
  private static final int DEFAULT_DISTRIBUTED_LOG_IMMEDIATE_FLUSH_INTERVAL = 0;
  private static final String DISTRIBUTED_LOG_ASYNC_THROTTLE = "distributedlog.async.throttle";
  private static final int DEFAULT_DISTRIBUTED_LOG_ASYNC_THROTTLE = 0;

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
    distributedLogConfiguration.addProperty("bkc.allowShadedLedgerManagerFactoryClass", true);
    distributedLogConfiguration.addProperty("bkc.throttle",
      conf.getInt(DISTRIBUTED_LOG_ASYNC_THROTTLE, DEFAULT_DISTRIBUTED_LOG_ASYNC_THROTTLE)
    );
    distributedLogConfiguration.setCompressionType(
      conf.get(DISTRIBUTED_LOG_COMPRESSION_TYPE, DEFAULT_DISTRIBUTED_LOG_COMPRESSION_TYPE)
    );
    distributedLogConfiguration.setOutputBufferSize(
      conf.getInt(DISTRIBUTED_LOG_WRITE_BUFFER_SIZE, DEFAULT_DISTRIBUTED_LOG_WRITE_BUFFER_SIZE)
    );
    distributedLogConfiguration.setZKRequestRateLimit(
      conf.getInt(DISTRIBUTED_LOG_ZK_REQUEST_RATE, DEFAULT_DISTRIBUTED_ZK_REQUEST_RATE)
    );
    distributedLogConfiguration.setImmediateFlushEnabled(
      conf.getBoolean(DISTRIBUTED_LOG_IMMEDIATE_FLUSH, DEFAULT_DISTRIBUTED_LOG_IMMEDIATE_FLUSH)
    );
    distributedLogConfiguration.setMinDelayBetweenImmediateFlushMs(
      conf.getInt(DISTRIBUTED_LOG_IMMEDIATE_FLUSH_INTERVAL,
        DEFAULT_DISTRIBUTED_LOG_IMMEDIATE_FLUSH_INTERVAL)
    );

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
