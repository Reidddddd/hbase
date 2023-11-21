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
package org.apache.hadoop.hbase;

import java.util.HashMap;
import java.util.Map;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A Sub class of {@link ServerName} with customised attributes.
 * The main purpose of this class is to transfer the rsgroup info from ServerManager to RSGroup
 * component without involving hbase-rsgroup dependency.
 */
@InterfaceAudience.Private
public class ServerNameWithAttributes extends ServerName {

  private final Map<Object, Object> attributes = new HashMap<>();

  public ServerNameWithAttributes(final String hostName, final int port, final long startcode,
    final String internalHostName) {
    super(hostName, port, startcode, internalHostName);
  }

  public ServerName setAttribute(String key, Object value) {
    attributes.put(key, value);
    return this;
  }

  public Object getAttribute(String key) {
    return attributes.get(key);
  }

  public void removeAttribute(String key) {
    attributes.remove(key);
  }
}
