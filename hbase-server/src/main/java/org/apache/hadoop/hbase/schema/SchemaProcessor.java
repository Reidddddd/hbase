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
import static org.apache.hadoop.hbase.schema.SchemaService.MAX_TASK_NUM;
import static org.apache.hadoop.hbase.schema.SchemaService.MAX_TASK_NUM_DEFAULT;
import static org.apache.hadoop.hbase.schema.SchemaService.NUM_THREADS_DEFAULT;
import static org.apache.hadoop.hbase.schema.SchemaService.NUM_THREADS_KEY;
import static org.apache.hadoop.hbase.schema.SchemaService.SCHEMA_TABLE_CF;
import static org.apache.hadoop.hbase.schema.SchemaService.SCHEMA_TABLE_META_CF;
import static org.apache.hadoop.hbase.schema.SchemaService.SCHEMA_TABLE_META_COLUMN_COUNT_QUALIFIER;
import static org.apache.hadoop.hbase.schema.SchemaService.SOFT_MAX_COLUMNS_PER_TABLE;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.FastPathExecutor;
import org.apache.hadoop.hbase.FastPathProcessable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.SchemaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.schema.SchemaService.Operation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

@SuppressWarnings("UnstableApiUsage")
@InterfaceAudience.Private
public class SchemaProcessor {
  private static final Log LOG = LogFactory.getLog(SchemaProcessor.class);
  private final Set<TableName> wideTableSet = ConcurrentHashMap.newKeySet();

  // Thread pool access the schema table.
  private FastPathExecutor updateExecutor;
  // Thread pool to dispatch tasks.
  private FastPathExecutor taskAcceptor;

  private Cache<TableName, Schema> schemaCache;

  private ClusterConnection connection;
  private volatile boolean inited = false;
  private int maxColumn;
  private int maxTasks;

  private static class Processor {
    private static final SchemaProcessor SINGLETON = new SchemaProcessor();
  }

  public static SchemaProcessor getInstance() {
    return Processor.SINGLETON;
  }

  public synchronized void init(boolean env, Configuration conf, ClusterConnection connection) {
    if (inited) {
      return;
    }

    String environment = env ? "Master-" : "RegionServer-";
    int numHandlers = conf.getInt(NUM_THREADS_KEY, NUM_THREADS_DEFAULT);
    maxColumn = conf.getInt(SOFT_MAX_COLUMNS_PER_TABLE, MAX_COLUMNS_PER_TABLE_DEFAULT);
    maxTasks = conf.getInt(MAX_TASK_NUM, MAX_TASK_NUM_DEFAULT);
    updateExecutor = new FastPathExecutor(numHandlers, environment + "SchemaUpdateHandler");
    updateExecutor.start();
    taskAcceptor = new FastPathExecutor(numHandlers, environment + "SchemaProcessHandler");
    taskAcceptor.start();
    schemaCache = CacheBuilder.newBuilder().expireAfterAccess(2, TimeUnit.HOURS).build();

    this.connection = connection;
    inited = true;
  }

  private Table getSchemaTable() throws IOException {
    return connection.getTable(TableName.SCHEMA_TABLE_NAME);
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
    final Famy family;
    final Qualy qualifier;

    ActionProcessor(TableName table, Operation operation, Cell cell) {
      this.table = table;
      this.operation = operation;
      this.family = cell == null ? null : new Famy(cell.getFamilyArray(), cell.getFamilyOffset(),
        cell.getFamilyLength());
      this.qualifier = cell == null ? null : new Qualy(cell.getQualifierArray(),
        cell.getQualifierOffset(), cell.getQualifierLength());
    }

    @Override
    public void process() {
      if (isWideTable(table)) {
        return;
      }

      try (Table schemaTable = getSchemaTable()) {
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
                put.addColumn(SCHEMA_TABLE_CF, family.extractContent(), HConstants.EMPTY_BYTE_ARRAY);
                try {
                  schemaTable.checkAndPut(row, SCHEMA_TABLE_CF, family.extractContent(), null, put);
                } catch (IOException e) {
                  // If we have IOException, that means the check and put is failed.
                  // In this case, we need to invalidate the schemaCache to let the executor retry.
                  schema.removeFamily(family);
                  handleIOException(e);
                }
              });
            }

            if (!schema.containColumn(family, qualifier) && schema.addColumn(family, qualifier)) {
              updateExecutor.accept(() -> {
                byte[] t = table.getName();
                byte[] row = new byte[t.length + qualifier.getLength()];
                System.arraycopy(t, 0, row, 0, t.length);
                System.arraycopy(qualifier.getBytes(), qualifier.getOffset(), row, t.length,
                  qualifier.getLength());
                Put put = new Put(row);
                put.addColumn(SCHEMA_TABLE_CF, family.extractContent(), ColumnType.NONE.getCode());
                boolean succeeded = false;
                try {
                  succeeded = schemaTable.checkAndPut(row, SCHEMA_TABLE_CF, family.extractContent(),
                    null, put);
                } catch (IOException e) {
                  // The put is exceptionally, we need to invalidate the corresponding cache to
                  // let it try to put it again.
                  schema.removeColumn(family, qualifier);
                  handleIOException(e);
                }
                if (succeeded) {
                  if (isWideTable(table)) {
                    return;
                  }
                  long currentSize = 0;
                  try {
                    currentSize = schemaTable.incrementColumnValue(t, SCHEMA_TABLE_META_CF,
                      SCHEMA_TABLE_META_COLUMN_COUNT_QUALIFIER, 1);
                  } catch (IOException e) {
                    handleIOException(e);
                    return;
                  }

                  if (invalidateCacheIfColumnExceedThreshold(table, currentSize)) {
                    LOG.info("Turned off schema service for table: " + table);
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
                }
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
              try {
                Scan scan = SchemaTableAccessor.createSchemaScanFor(table);
                int batch = scan.getBatch();
                ResultScanner scanner = schemaTable.getScanner(scan);
                Result[] resultsBatch = scanner.next(batch);
                while (resultsBatch.length > 0) {
                  List<Delete> deletes = new ArrayList<>(resultsBatch.length);
                  Arrays.stream(resultsBatch).forEach(
                    result -> deletes.add(new Delete(result.getRow()))
                  );
                  schemaTable.delete(deletes);
                  resultsBatch = scanner.next(batch);
                }
              } catch (IOException e) {
                handleIOException(e);
              }
            });
            break;
          }
          default:
            // Do nothing here.
        }
      } catch (IOException e) {
        handleIOException(e);
      }
    }
  }

  private void handleIOException(IOException ioe) {
    if (ioe instanceof TableNotFoundException) {
      LOG.info("Schema table is not found");
    } else if (ioe instanceof TableNotEnabledException) {
      LOG.info("Schema table is not enabled.");
    } else {
      LOG.warn("Failed accessing schema table: ", ioe);
    }
  }

  @VisibleForTesting
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

  long getColumnCount(TableName table) throws IOException {
    byte[] t = table.getName();
    Get get = new Get(t);
    get.addColumn(SCHEMA_TABLE_META_CF, SCHEMA_TABLE_META_COLUMN_COUNT_QUALIFIER);
    try (Table schemaTable = getSchemaTable()) {
      Result result = schemaTable.get(get);
      byte[] value =
        result.getValue(SCHEMA_TABLE_META_CF, SCHEMA_TABLE_META_COLUMN_COUNT_QUALIFIER);
      return value == null ? 0 : Bytes.toLong(value);
    }
  }

  public boolean isWideTable(TableName tableName) {
    return wideTableSet.contains(tableName);
  }

  // A check to confirm whether we keep accept tasks according to the ongoing tasks.
  // For preventing OOM.
  public boolean reachedMaxTasks() {
    return taskAcceptor.numTasksInQueue() + updateExecutor.numTasksInQueue() < maxTasks;
  }

}
