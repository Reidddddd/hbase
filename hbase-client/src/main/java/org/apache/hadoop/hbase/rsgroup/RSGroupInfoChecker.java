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
package org.apache.hadoop.hbase.rsgroup;

import com.google.protobuf.ServiceException;
import java.io.Closeable;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RSGroupInfoChecker implements Closeable {
  private static final Log LOG = LogFactory.getLog(RSGroupInfoChecker.class);

  private final Admin admin;
  private final RSGroupAdminProtos.RSGroupAdminService.BlockingInterface stub;

  public RSGroupInfoChecker(Connection conn) throws IOException {
    admin = conn.getAdmin();
    stub = RSGroupAdminProtos.RSGroupAdminService.newBlockingStub(admin.coprocessorService());
  }

  public String getRSGroupInfoOfTable(TableName tableName) throws IOException {
    RSGroupAdminProtos.GetRSGroupInfoOfTableRequest
      request = RSGroupAdminProtos.GetRSGroupInfoOfTableRequest.newBuilder().setTableName(
      ProtobufUtil.toProtoTableName(tableName)).build();
    try {
      RSGroupAdminProtos.GetRSGroupInfoOfTableResponse
        resp = stub.getRSGroupInfoOfTable(null, request);
      if (resp.hasRSGroupInfo()) {
        return RSGroupProtobufUtil.toGroupInfo(resp.getRSGroupInfo()).getName();
      }
      return null;
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public void close() throws IOException {
    admin.close();
  }
}
