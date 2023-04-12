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

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ConnectionManager.HConnectionImplementation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Connection to an HTable from within a Coprocessor. We can do some nice tricks since we know we
 * are on a regionserver, for instance skipping the full serialization/deserialization of objects
 * when talking to the server.
 * <p>
 * You should not use this class from any client - its an internal class meant for use by the
 * coprocessor framework.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CoprocessorHConnection extends HConnectionImplementation {
  private static final Map<String, WeakReference<ClusterConnection>> connectionMap =
    new HashMap<>();

  /**
   * Create an unmanaged {@link HConnection} based on the environment in which we are running the
   * coprocessor. The {@link HConnection} must be externally cleaned up (we bypass the usual HTable
   * cleanup mechanisms since we own everything).
   * @param env environment hosting the {@link HConnection}
   * @return an unmanaged {@link HConnection}.
   * @throws IOException if we cannot create the connection
   */
  public static ClusterConnection getConnectionForEnvironment(CoprocessorEnvironment env)
      throws IOException {
    // this bit is a little hacky - just trying to get it going for the moment
    if (env instanceof RegionCoprocessorEnvironment) {
      RegionCoprocessorEnvironment e = (RegionCoprocessorEnvironment) env;
      RegionServerServices services = e.getRegionServerServices();
      if (services instanceof HRegionServer) {
        return getConnectionInstance(env.getConfiguration(), (HRegionServer) services);
      }
    }
    return getConnectionInstance(env.getConfiguration(), null);
  }

  // We need to guarantee that at one time, there is only one available coprocessor connection to
  // each cluster. Otherwise, the coprocessor may use up connection resources on the rs.
  public static synchronized ClusterConnection getConnectionInstance(Configuration conf,
      HRegionServer server) throws IOException {
    String zkServers = conf.get(HConstants.ZOOKEEPER_QUORUM);
    String zkRoot = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT,
      HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    if (zkServers == null || zkRoot == null) {
      throw new IOException("Failed get connection to illegal hbase cluster, "
        + "the zk quorum or zk root is null.");
    }

    String connectionKey = String.join(":", zkServers, zkRoot);
    WeakReference<ClusterConnection> weakReference = connectionMap.get(connectionKey);
    ClusterConnection connection;

    if (weakReference == null) {
      connection = server == null ? ConnectionManager.createConnectionInternal(conf) :
        new CoprocessorHConnection(server);
      weakReference = new WeakReference<>(connection);
      connectionMap.put(connectionKey, weakReference);
    } else {
      connection = weakReference.get();
      if (connection == null || connection.isClosed() || connection.isAborted()) {
        connection = server == null ? ConnectionManager.createConnectionInternal(conf) :
          new CoprocessorHConnection(server);
        connectionMap.put(connectionKey, new WeakReference<>(connection));
      }
    }

    return connection;
  }

  private final ServerName serverName;
  private final HRegionServer server;

  /**
   * Legacy constructor
   * @param delegate
   * @param server
   * @throws IOException if we cannot create the connection
   * @deprecated delegate is not used
   */
  @Deprecated
  public CoprocessorHConnection(ClusterConnection delegate, HRegionServer server)
      throws IOException {
    this(server);
  }

  /**
   * Constructor that uses server configuration
   * @param server
   * @throws IOException if we cannot create the connection
   */
  public CoprocessorHConnection(HRegionServer server) throws IOException {
    this(server.getConfiguration(), server);
  }

  /**
   * Constructor that accepts custom configuration
   * @param conf
   * @param server
   * @throws IOException if we cannot create the connection
   */
  public CoprocessorHConnection(Configuration conf, HRegionServer server) throws IOException {
    super(conf, false, null, UserProvider.instantiate(conf).getCurrent(), server.getClusterId());
    this.server = server;
    this.serverName = server.getServerName();
  }

  @Override
  public org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService.BlockingInterface
      getClient(ServerName serverName) throws IOException {
    // client is trying to reach off-server, so we can't do anything special
    if (!this.serverName.equals(serverName)) {
      return super.getClient(serverName);
    }
    // the client is attempting to write to the same regionserver, we can short-circuit to our
    // local regionserver
    return server.getRSRpcServices();
  }

  @Override
  public NonceGenerator getNonceGenerator() {
    return ConnectionUtils.NO_NONCE_GENERATOR; // don't use nonces for coprocessor connection
  }
}
