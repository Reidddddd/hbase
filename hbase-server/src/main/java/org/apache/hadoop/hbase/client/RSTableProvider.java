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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.coprocessor.TableProvider;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class RSTableProvider implements TableProvider {

  @Override
  public HTableInterface createWrapper(List<HTableInterface> tableList, TableName tableName,
      CoprocessorHost.Environment env, ExecutorService pool) throws IOException {
    return HTableWrapper.createWrapper(tableList, tableName, env, pool);
  }

  @Override
  public void internalClose(HTableInterface target) throws IOException {
    ((HTableWrapper) target).internalClose();
  }
}
