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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.ClientTokenUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ConnectionCache} which could create {@link org.apache.hadoop.hbase.client.Connection}
 * with authentication token.
 */
@InterfaceAudience.Private
public class ConnectionCacheWithAuthToken extends ConnectionCache {
  private static final Logger LOG = LoggerFactory.getLogger(ConnectionCacheWithAuthToken.class);

  private final ThreadLocal<String> effectiveUserPassword =
    new ThreadLocal<String>() {
      @Override
      protected String initialValue() {
        return "";
      }
    };

  public ConnectionCacheWithAuthToken(Configuration conf, UserProvider userProvider,
    int cleanInterval, int maxIdleTime) throws IOException {
    super(conf, userProvider, cleanInterval, maxIdleTime);
  }

  @Override
  ConnectionInfo getCurrentConnection() throws IOException {
    String userName = getEffectiveUser();
    ConnectionInfoWithPassword connInfo = (ConnectionInfoWithPassword) connections.get(userName);
    if (connInfo == null || !connInfo.updateAccessTime() ||
      !connInfo.getPassword().equals(effectiveUserPassword.get())) {
      Lock lock = locker.acquireLock(userName);
      try {
        connInfo = (ConnectionInfoWithPassword) connections.get(userName);
        if (connInfo == null || !connInfo.getPassword().equals(effectiveUserPassword.get())) {
          User user = getConnectionUser(userName);
          Connection conn = ConnectionFactory.createConnection(conf, user);
          connInfo = new ConnectionInfoWithPassword(conn, userName, effectiveUserPassword.get());
          connections.put(userName, connInfo);
        }
      } finally {
        lock.unlock();
      }
    }
    return connInfo;
  }


  class ConnectionInfoWithPassword extends ConnectionInfo {
    private final String password;

    ConnectionInfoWithPassword(Connection conn, String user, String password) {
      super(conn, user);
      this.password = password;
    }

    private String getPassword() {
      return password;
    }
  }

  @Override
  protected User getConnectionUser(String username) {
    User user = userProvider.create(UserGroupInformation.createRemoteUser(username));
    ClientTokenUtil.setUserPassword(user, effectiveUserPassword.get());
    return user;
  }

  public void setPassword(String password) {
    effectiveUserPassword.set(password);
  }
}
