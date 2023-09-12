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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.FastPathExecutor;
import org.apache.hadoop.hbase.FastPathProcessable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RawAsyncTable;
import org.apache.hadoop.hbase.client.RawScanResultConsumer;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.schema.SchemaService.Operation;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class SchemaProcessor {
  private static final Log LOG = LogFactory.getLog(SchemaProcessor.class);

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
        case TRUNCATE:
        case DROP: {
          updateExecutor.accept(() -> {
            Scan scan = new Scan();
            byte[] startRow = table.getName();
            byte[] stopRow = new byte[startRow.length];
            System.arraycopy(startRow, 0, stopRow, 0, startRow.length);
            stopRow[stopRow.length - 1]++;
            scan.withStartRow(startRow);
            scan.withStopRow(stopRow);
            scan.setCacheBlocks(false);
            scan.setBatch(100);

            schemaTable.scan(scan, new RawScanResultConsumer() {
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
                schemaCache.remove(table);
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

}
