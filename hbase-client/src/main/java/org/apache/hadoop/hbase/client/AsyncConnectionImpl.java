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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.HConstants.CLUSTER_ID_DEFAULT;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_RPC_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.HBASE_RPC_TIMEOUT_KEY;
import static org.apache.hadoop.hbase.client.ConnectionUtils.getStubKey;

import io.netty.util.HashedWheelTimer;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.apache.hadoop.hbase.util.Threads;

/**
 * The implementation of AsyncConnection.
 */
@InterfaceAudience.Private
class AsyncConnectionImpl implements AsyncConnection {

  private static final Log LOG = LogFactory.getLog(AsyncConnectionImpl.class);

  private static final HashedWheelTimer RETRY_TIMER = new HashedWheelTimer(
          Threads.newDaemonThreadFactory("Async-Client-Retry-Timer"), 10, TimeUnit.MILLISECONDS);

  private static final String RESOLVE_HOSTNAME_ON_FAIL_KEY = "hbase.resolve.hostnames.on.failure";

  private final Configuration conf;

  final AsyncConnectionConfiguration connConf;

  private final User user;

  private final ClusterRegistry registry;

  private final String clusterId;

  private final int rpcTimeout;

  private final RpcClient rpcClient;

  final RpcControllerFactory rpcControllerFactory;

  private final boolean hostnameCanChange;

  private final AsyncRegionLocator locator;

  final AsyncRpcRetryingCallerFactory callerFactory;

  private final ConcurrentMap<String, ClientService.Interface> rsStubs = new ConcurrentHashMap<>();

  public AsyncConnectionImpl(Configuration conf, User user) throws IOException {
    this.conf = conf;
    this.user = user;

    this.connConf = new AsyncConnectionConfiguration(conf);

    this.locator = new AsyncRegionLocator(conf);

    // action below will not throw exception so no need to catch and close.
    this.registry = ClusterRegistryFactory.getRegistry(conf);
    this.clusterId = Optional.ofNullable(registry.getClusterId()).orElseGet(() -> {
      if (LOG.isDebugEnabled()) {
        LOG.debug("cluster id came back null, using default " + CLUSTER_ID_DEFAULT);
      }
      return CLUSTER_ID_DEFAULT;
    });
    this.rpcClient = RpcClientFactory.createClient(conf, clusterId);
    this.rpcControllerFactory = RpcControllerFactory.instantiate(conf);
    this.hostnameCanChange = conf.getBoolean(RESOLVE_HOSTNAME_ON_FAIL_KEY, true);
    this.rpcTimeout = conf.getInt(HBASE_RPC_TIMEOUT_KEY, DEFAULT_HBASE_RPC_TIMEOUT);
    this.callerFactory = new AsyncRpcRetryingCallerFactory(this, RETRY_TIMER);
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(locator);
    IOUtils.closeQuietly(rpcClient);
  }

  @Override
  public AsyncTableRegionLocator getRegionLocator(TableName tableName) {
    return new AsyncTableRegionLocatorImpl(tableName, locator);
  }

  // we will override this method for testing retry caller, so do not remove this method.
  AsyncRegionLocator getLocator() {
    return locator;
  }

  private ClientService.Interface createRegionServerStub(ServerName serverName) throws IOException {
    return ClientService.newStub(rpcClient.createRpcChannel(serverName, user, rpcTimeout));
  }

  ClientService.Interface getRegionServerStub(ServerName serverName) throws IOException {
    return CollectionUtils.computeIfAbsentEx(rsStubs,
            getStubKey(ClientService.Interface.class.getSimpleName(), serverName, hostnameCanChange),
            () -> createRegionServerStub(serverName));
  }

  @Override
  public AsyncTable getTable(TableName tableName) {
    return new AsyncTableImpl(this, tableName);
  }
}