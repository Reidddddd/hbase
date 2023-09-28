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

package org.apache.hadoop.hbase.schema;

import static org.apache.hadoop.hbase.HConstants.EMPTY_BYTE_ARRAY;
import static org.apache.hadoop.hbase.schema.SchemaService.NUM_THREADS_DEFAULT;
import static org.apache.hadoop.hbase.schema.SchemaService.NUM_THREADS_KEY;
import static org.apache.hadoop.hbase.schema.SchemaService.SCHEMA_TABLE_CF;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.FastPathExecutor;
import org.apache.hadoop.hbase.FastPathProcessable;
import org.apache.hadoop.hbase.SchemaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RawAsyncTable;
import org.apache.hadoop.hbase.client.RawScanResultConsumer;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.schema.SchemaService.Operation;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class SchemaProcessor {

  // Thread pool access the schema table.
  private FastPathExecutor updateExecutor;
  // Thread pool to dispatch tasks.
  private FastPathExecutor taskAcceptor;

  private Map<TableName, Schema> schemaCache;

  private AsyncConnection connection;
  private RawAsyncTable schemaTable;
  private boolean inited = false;

  private static class Processor {
    private static final SchemaProcessor SINGLETON = new SchemaProcessor();
  }

  public static SchemaProcessor getInstance() {
    return Processor.SINGLETON;
  }

  public synchronized void init(boolean env, Configuration conf) {
    if (inited) {
      return;
    }

    String environment = env ? "Master-" : "RegionServer-";
    int numHandlers = conf.getInt(NUM_THREADS_KEY, NUM_THREADS_DEFAULT);
    updateExecutor = new FastPathExecutor(numHandlers, environment + "SchemaUpdateHandler");
    updateExecutor.start();
    taskAcceptor = new FastPathExecutor(numHandlers, environment + "SchemaProcessHandler");
    taskAcceptor.start();

    schemaCache = new ConcurrentHashMap<>();

    try {
      connection = ConnectionFactory.createAsyncConnection(conf).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    schemaTable = connection.getRawTable(TableName.SCHEMA_TABLE_NAME);
    inited = true;
  }

  public FastPathProcessable createProcessor(TableName table, Operation operation, Cell cell) {
    return new ActionProcessor(table, operation, cell);
  }

  public void acceptTask(FastPathProcessable task) {
    taskAcceptor.accept(task);
  }

  class ActionProcessor implements FastPathProcessable {
    final TableName table;
    final Operation operation;
    final byte[] family;
    final byte[] qualifier;

    ActionProcessor(TableName table, Operation operation, Cell cell) {
      this.table = table;
      this.operation = operation;
      this.family = cell == null ? null : CellUtil.cloneFamily(cell);
      this.qualifier = cell == null ? null : CellUtil.cloneQualifier(cell);
    }

    @Override
    public void process() {
      switch (operation) {
        case INCREMENT:
        case APPEND:
        case PUT: {
          Schema cacheSchema = schemaCache.putIfAbsent(table, new Schema(table));
          if (cacheSchema == null) {
            cacheSchema = schemaCache.get(table);
          }
          final Schema schema = cacheSchema;
          if (schema.containColumn(family, qualifier)) {
            return;
          }

          if (!schema.containFamily(family) && schema.addFamily(family)) {
            updateExecutor.accept(() -> {
              byte[] row = table.getName();
              Put put = new Put(row);
              put.addColumn(SCHEMA_TABLE_CF, family, EMPTY_BYTE_ARRAY);
              schemaTable.checkAndPut(row, SCHEMA_TABLE_CF, family, null, put);
            });
          }

          if (!schema.containColumn(family, qualifier) && schema.addColumn(family, qualifier)) {
            updateExecutor.accept(() -> {
              byte[] t = table.getName();
              byte[] row = new byte[t.length + qualifier.length];
              System.arraycopy(t, 0, row, 0, t.length);
              System.arraycopy(qualifier, 0, row, t.length, qualifier.length);
              Put put = new Put(row);
              put.addColumn(SCHEMA_TABLE_CF, family, EMPTY_BYTE_ARRAY);
              schemaTable.checkAndPut(row, SCHEMA_TABLE_CF, family, null, put);
            });
          }
          break;
        }
        case DELETE: {
          // Only one case can enter this case:
          // a table is disabled, and related Deletes are sent to hbase:schema
          // so RS's schemaCache should remove the cache of dropped/truncated table
          schemaCache.remove(table);
          break;
        }
        case TRUNCATE:
        case DROP: {
          // drop and truncate happens on master side only,
          // so schemaCache won't have any data about a table schema which is stored in RS side
          // from very beginning
          updateExecutor.accept(() -> {
            schemaTable.scan(
              SchemaTableAccessor.createSchemaScanFor(table),
              new RawScanResultConsumer() {
                @Override
                public void onNext(Result[] results, ScanController controller) {
                  List<Delete> deletes = new ArrayList<>(results.length);
                  for (Result result : results) {
                    deletes.add(new Delete(result.getRow()));
                  }
                  schemaTable.delete(deletes);
                }

                @Override
                public void onError(Throwable error) {}

                @Override
                public void onComplete() {
                  // useless in fact as comments above,
                  // execute it for safe and UT (master and rs share the same instance in UT)
                  // Do nothing here, we will clean cache by watching the table zk nodes.
                }
              });
          });
          break;
        }
        default:
          // Do nothing here.
      }
    }
  }

  @VisibleForTesting
  public boolean isTableCleaned(TableName tableName) {
    return !schemaCache.containsKey(tableName);
  }

}
