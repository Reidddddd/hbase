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

import static org.apache.hadoop.hbase.schema.SchemaService.MAX_COLUMNS_PER_TABLE_DEFAULT;
import static org.apache.hadoop.hbase.schema.SchemaService.NUM_THREADS_DEFAULT;
import static org.apache.hadoop.hbase.schema.SchemaService.NUM_THREADS_KEY;
import static org.apache.hadoop.hbase.schema.SchemaService.SCHEMA_TABLE_CF;
import static org.apache.hadoop.hbase.schema.SchemaService.SCHEMA_TABLE_META_CF;
import static org.apache.hadoop.hbase.schema.SchemaService.SCHEMA_TABLE_META_COLUMN_COUNT_QUALIFIER;
import static org.apache.hadoop.hbase.schema.SchemaService.SOFT_MAX_COLUMNS_PER_TABLE;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.FastPathExecutor;
import org.apache.hadoop.hbase.FastPathProcessable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.SchemaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.schema.SchemaService.Operation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.cache.Cache;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;

@InterfaceAudience.Private
public class SchemaProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaProcessor.class);

  // Thread pool access the schema table.
  private FastPathExecutor updateExecutor;
  // Thread pool to dispatch tasks.
  private FastPathExecutor taskAcceptor;

  private Cache<TableName, Schema> schemaCache;
  private Set<TableName> wideTableSet;

  private AsyncConnection connection;
  private AsyncTable<AdvancedScanResultConsumer> schemaTable;
  private boolean inited = false;
  private int maxColumn;

  private final static class Processor {
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
    maxColumn = conf.getInt(SOFT_MAX_COLUMNS_PER_TABLE, MAX_COLUMNS_PER_TABLE_DEFAULT);
    updateExecutor = new FastPathExecutor(numHandlers, environment + "SchemaUpdateHandler");
    updateExecutor.start();
    taskAcceptor = new FastPathExecutor(numHandlers, environment + "SchemaProcessHandler");
    taskAcceptor.start();
    schemaCache = CacheBuilder.newBuilder().expireAfterAccess(2, TimeUnit.HOURS).build();
    wideTableSet = ConcurrentHashMap.newKeySet();


    try {
      connection = ConnectionFactory.createAsyncConnection(conf).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    schemaTable = connection.getTableBuilder(TableName.SCHEMA_TABLE_NAME).build();
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
      if (isWideTable(table)) {
        return;
      }
      switch (operation) {
        case INCREMENT:
        case APPEND:
        case PUT: {
          Schema cachedSchema = null;
          try {
            // Get schema by table name.
            // Create a new schema object, cache it and return it if the given table name does not
            // have a schema.
            cachedSchema = schemaCache.get(table, () -> new Schema(table));
          } catch (ExecutionException e) {
            // Skip this we will never get exception here.
          }
          if (cachedSchema == null) {
            // We should never arrive here.
            // This null check is just for safety and pass ide static check.
            return;
          }

          final Schema schema = cachedSchema;
          if (schema.containColumn(family, qualifier)) {
            return;
          }

          if (!schema.containFamily(family) && schema.addFamily(family)) {
            updateExecutor.accept(() -> {
              byte[] row = table.getName();
              Put put = new Put(row);
              put.addColumn(SCHEMA_TABLE_CF, family, HConstants.EMPTY_BYTE_ARRAY);
              schemaTable.checkAndMutate(row, SCHEMA_TABLE_CF).qualifier(family).ifNotExists()
                .thenPut(put);
            });
          }

          if (!schema.containColumn(family, qualifier) && schema.addColumn(family, qualifier)) {
            updateExecutor.accept(() -> {
              byte[] t = table.getName();
              byte[] row = new byte[t.length + qualifier.length];
              System.arraycopy(t, 0, row, 0, t.length);
              System.arraycopy(qualifier, 0, row, t.length, qualifier.length);
              Put put = new Put(row);
              put.addColumn(SCHEMA_TABLE_CF, family, ColumnType.NONE.getCode());
              schemaTable.checkAndMutate(row, SCHEMA_TABLE_CF).qualifier(family).ifNotExists()
                .thenPut(put).whenComplete((succeeded, exception) -> {
                  String errorMsg = "Failed increment column count for table: " + table;
                  if (exception != null) {
                    LOG.warn(errorMsg, exception);
                    return;
                  }
                  if (isWideTable(table)) {
                    return;
                  }
                  if (succeeded) {
                    schemaTable.incrementColumnValue(t, SCHEMA_TABLE_META_CF,
                        SCHEMA_TABLE_META_COLUMN_COUNT_QUALIFIER, 1)
                      .whenComplete((currentSize, error) -> {
                        if (error != null) {
                          LOG.warn(errorMsg, error);
                          return;
                        }
                        try {
                          if (invalidateCacheIfColumnExceedThreshold(table, currentSize)) {
                            if (LOG.isTraceEnabled()) {
                              LOG.trace("Turned off schema service for table: " + table);
                            }
                            // Revert the column count and delete the new added row.
                            Delete delete = new Delete(row);
                            schemaTable.delete(delete);
                            schemaTable.incrementColumnValue(t, SCHEMA_TABLE_META_CF,
                              SCHEMA_TABLE_META_COLUMN_COUNT_QUALIFIER, -1)
                              .whenComplete((size, e) -> {
                                if (e != null) {
                                  LOG.warn(errorMsg, e);
                                }
                              });
                          }
                        } catch (Exception e) {
                          LOG.warn("Failed check column count for table: " + table, e);
                        }
                      });
                  } else {
                    // We failed to add a new row in schema table.
                    // Need to check the col count here to remove invalid table in time.
                    try {
                      long current = getColumnCount(table);
                      invalidateCacheIfColumnExceedThreshold(table, current);
                    } catch (Exception e) {
                      LOG.warn("Failed turn off schema service for table: " + table, e);
                    }
                  }
                });
            });
          }
          break;
        }
        case DELETE: {
          // Only one case can enter this case:
          // a table is disabled, and related Deletes are sent to hbase:schema
          // so RS's schemaCache should remove the cache of dropped/truncated table
          schemaCache.invalidate(table);
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
              new AdvancedScanResultConsumer() {
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

  public boolean isTableCleaned(TableName tableName) {
    return !schemaCache.asMap().containsKey(tableName);
  }

  /**
   * @return true if we disabled schema service for table
   */
  boolean invalidateCacheIfColumnExceedThreshold(TableName table, long current) {
    if (current > maxColumn) {
      // Turn off marker.
      wideTableSet.add(table);
      schemaCache.invalidate(table);
      return true;
    }
    return false;
  }

  long getColumnCount(TableName table) throws ExecutionException, InterruptedException {
    byte[] t = table.getName();
    Get get = new Get(t);
    get.addColumn(SCHEMA_TABLE_META_CF, SCHEMA_TABLE_META_COLUMN_COUNT_QUALIFIER);
    Result result = schemaTable.get(get).get();
    byte[] value = result.getValue(SCHEMA_TABLE_META_CF, SCHEMA_TABLE_META_COLUMN_COUNT_QUALIFIER);
    return value == null ? 0 : Bytes.toLong(value);
  }

  public boolean isWideTable(TableName tableName) {
    return wideTableSet.contains(tableName);
  }
}
