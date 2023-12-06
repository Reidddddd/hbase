/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.schema;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.SchemaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.HasMasterServices;
import org.apache.hadoop.hbase.coprocessor.HasRegionServerServices;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The hbase:schema architecture is:
 * Row key               |    q      |   m   |
 *                       | a | b | c |   c   |
 * table_name            | _ | _ |   |   3   |
 * table_nameQualifier_1 | 1 |   |   |       |
 * table_nameQualifier_2 |   | 2     |       |
 * table_name1           |   |   | 3 |       |
 * <p>
 * First of all, the family char is q in hbase:schema.
 * Qualifier will be families of each table, value is no need here.
 * Row key will be table name of a table or table_name + qualifier of a table.
 * </p>
 * <p>
 * For example, from above, we can tell:
 * Table 'table_name' has two families, one is 'a', the other is 'b',
 * then 'table_name' has a 'Qualifier_1' which under family 'a',
 * and 'Qualifier_2' under family 'b'.
 * </p>
 * <p>
 * The value of each cell is a byte code which represent its type,
 * except for the table row whose cell values are empty.
 * Byte code's details can be referred from {@link ColumnType}
 * </p>
 * <p>
 * The cf 'm' stores other information of the table. We currently record the number of columns
 * with qualifier 'c' and a marker whether record the table's schema (for infinite growth case).
 * </p>
 * <p>
 * With design, we can:
 * 1. Avoid fat row or fat cell
 * 2. Scalable with more qualifiers, e.g, put the type of a column into the value
 * 3. Easy to achieve a table's information with prefix scan or start/stop row scan
 * 4. The meta column family support further functional extensions.
 * </p>
 * <p>
 * Note, this coprocessor service is bind to a RS at region level.
 * Its lifecycle follows RS's.
 * So even a region moved or offline, it can't be shutdown until RS dead.
 * TODO: find an elegantly way to shutdown this service
 * </p>
 */
@CoreCoprocessor
@InterfaceAudience.Private
public class SchemaService implements MasterCoprocessor, RegionCoprocessor,
  MasterObserver, RegionObserver {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaService.class);
  static final String NUM_THREADS_KEY = "hbase.schema.updater.threads";
  static final String SOFT_MAX_COLUMNS_PER_TABLE = "hbase.schema.soft.max.columns.per.table";
  static final String MAX_TASK_NUM = "hbase.schema.max.tasks.num";
  static final int MAX_TASK_NUM_DEFAULT = 1000;
  static final int NUM_THREADS_DEFAULT = 5;
  static final int MAX_COLUMNS_PER_TABLE_DEFAULT = 1000;

  // q for qolumn
  static final byte[] SCHEMA_TABLE_CF = Bytes.toBytes("q");
  static final byte[] SCHEMA_TABLE_META_CF = Bytes.toBytes("m");
  static final byte[] SCHEMA_TABLE_META_COLUMN_COUNT_QUALIFIER = Bytes.toBytes("c");
  static final AtomicReference<TableStateListener> tableStateListener = new AtomicReference<>();

  // A static map to guarantee we do not duplicate zk node watching.
  static final Set<String> watchedTables = ConcurrentHashMap.newKeySet();
  // An independent thread to do a wide table check to not block normal region opening.
  static final ExecutorService preCheckWorker = Executors.newSingleThreadExecutor();
  static final AtomicBoolean schemaServiceOn = new AtomicBoolean();

  private SchemaProcessor processor;

  enum Operation {
    PUT, INCREMENT, APPEND, DELETE, TRUNCATE, DROP
  }

  @Override
  public Optional<MasterObserver> getMasterObserver() {
    return Optional.of(this);
  }

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    processor = SchemaProcessor.getInstance();
  }

  private synchronized void initListener(ZKWatcher watcher) {
    if (tableStateListener.get() == null) {
      tableStateListener.compareAndSet(null, new TableStateListener(watcher, this));
      watcher.registerListener(tableStateListener.get());
    }
  }

  private void createSchemaTableIfNotExist(MasterServices masterServices) throws IOException {
    try (Admin admin = masterServices.getConnection().getAdmin()) {
      if (admin.tableExists(TableName.SCHEMA_TABLE_NAME)) {
        return;
      }
    }

    HTableDescriptor desc = new HTableDescriptor(TableName.SCHEMA_TABLE_NAME);
    desc.addFamily(new HColumnDescriptor(SCHEMA_TABLE_CF)
        .setMaxVersions(1)
        .setInMemory(true)
        .setBlockCacheEnabled(true)
        .setBlocksize(8 * 1024)
        .setBloomFilterType(BloomType.ROW)
        .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
    );
    desc.addFamily(new HColumnDescriptor(SCHEMA_TABLE_META_CF)
        .setMaxVersions(1)
        .setInMemory(true)
        .setBlockCacheEnabled(true)
        .setBlocksize(8 * 1024)
        .setBloomFilterType(BloomType.ROW)
        .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
    );
    masterServices.createSystemTable(desc);
  }

  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
    if (tableName.isSystemTable()) {
      return;
    }
    sendTask(tableName, Operation.DROP);
  }

  @Override
  public void postTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
    if (tableName.isSystemTable()) {
      return;
    }
    sendTask(tableName, Operation.TRUNCATE);
  }

  @Override
  public Result postIncrement(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Increment increment, final Result result) throws IOException {
    TableName table = e.getEnvironment().getRegionInfo().getTable();
    if (shouldSendTask(table)) {
      sendTask(table, Operation.INCREMENT, increment.getFamilyCellMap());
    }
    return result;
  }

  @Override
  public void postPut(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Put put, final WALEdit edit, final Durability durability) throws IOException {
    TableName table = e.getEnvironment().getRegionInfo().getTable();
    if (shouldSendTask(table)) {
      sendTask(table, Operation.PUT, put.getFamilyCellMap());
    }
  }

  @Override
  public Result postAppend(ObserverContext<RegionCoprocessorEnvironment> e, Append append,
      Result result) throws IOException {
    TableName table = e.getEnvironment().getRegionInfo().getTable();
    if (shouldSendTask(table)) {
      sendTask(table, Operation.APPEND, append.getFamilyCellMap());
    }
    return result;
  }

  @Override
  public void postBatchMutate(final ObserverContext<RegionCoprocessorEnvironment> e,
    final MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    TableName table = e.getEnvironment().getRegionInfo().getTable();
    if (!shouldSendTask(table)) {
      return;
    }

    for (int i = 0; i < miniBatchOp.size(); i++) {
      Mutation mutation = miniBatchOp.getOperation(i);
      if (mutation instanceof Put) {
        sendTask(table, Operation.PUT, mutation.getFamilyCellMap());
      } else if (mutation instanceof Append) {
        sendTask(table, Operation.APPEND, mutation.getFamilyCellMap());
      } else if (mutation instanceof Increment) {
        sendTask(table, Operation.INCREMENT, mutation.getFamilyCellMap());
      } // no need to process Delete
    }
  }

  private void sendTask(TableName table, Operation operation) {
    sendTask(table, operation, null);
  }

  private void sendTask(TableName table, Operation operation,
      NavigableMap<byte [], List<Cell>> cellMap) {
    if (processor.reachedMaxTasks()) {
      return;
    }
    if (cellMap == null) {
      processor.acceptTask(processor.createProcessor(table, operation, null));
      return;
    }

    for (Map.Entry<byte[], List<Cell>> entry : cellMap.entrySet()) {
      for (Cell cell : entry.getValue()) {
        processor.acceptTask(processor.createProcessor(table, operation, cell));
      }
    }
  }

  private boolean isWideTable(TableName table) {
    return processor.isWideTable(table);
  }

  static class TableStateListener extends ZKListener {
    private final SchemaService schemaService;

    /**
     * Construct a ZooKeeper event listener.
     */
    public TableStateListener(ZKWatcher watcher, SchemaService schemaService) {
      super(watcher);
      this.schemaService = schemaService;
    }

    @Override
    public void nodeCreated(String path) {
      // If we get this event, we need to re-watch the path
      String parent = ZKUtil.getParent(path);
      if (parent != null && parent.equals(watcher.getZNodePaths().tableZNode)) {
        try {
          ZKUtil.setWatchIfNodeExists(watcher, path);
        } catch (KeeperException e) {
          LOG.warn("Failed watch node: " + path);
        }
      }
    }

    @Override
    public void nodeDeleted(String path) {
      String parent = ZKUtil.getParent(path);
      if (parent != null && parent.equals(watcher.getZNodePaths().tableZNode)) {
        TableName tableName = TableName.valueOf(ZKUtil.getNodeName(path));
        if (watchedTables.remove(tableName.getNameAsString())) {
          schemaService.sendTask(tableName, Operation.DELETE);
        }
      }
    }

    @Override
    public void nodeDataChanged(String path) {
      // If we get this event, we need to re-watch the path
      String parent = ZKUtil.getParent(path);
      if (parent != null && parent.equals(watcher.getZNodePaths().tableZNode)) {
        try {
          ZKUtil.setWatchIfNodeExists(watcher, path);
        } catch (KeeperException e) {
          LOG.warn("Failed watch node: " + path);
        }
      }
    }
  }

  @Override
  public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx)
    throws IOException {
    MasterCoprocessorEnvironment mEnv = ctx.getEnvironment();
    LOG.info("Starting SchemaService on Master");

    if (mEnv instanceof HasMasterServices) {
      processor.init(true, mEnv.getConfiguration(),
        ((HasMasterServices) mEnv).getMasterServices().getClusterConnection());
      new Thread(() -> {
        LOG.info("Waiting for the cluster connection built");
        while (mEnv.getConnection() == null) {
          try {
            wait(1000);
          } catch (InterruptedException ex) {
            LOG.warn("Failed to create schema table: ", ex);
          }
        }

        try {
          createSchemaTableIfNotExist(((HasMasterServices) mEnv).getMasterServices());
        } catch (IOException ex) {
          LOG.warn("Failed to create schema table: ", ex);
        }
      }).start();
    }
  }

  @Override
  public void postOpen(final ObserverContext<RegionCoprocessorEnvironment> c) {
    RegionCoprocessorEnvironment regionEnv= c.getEnvironment();
    TableName tableName = regionEnv.getRegionInfo().getTable();
    if (tableName.isSystemTable()) {
      return;
    }
    try {
      // Update once when each region opens.
      schemaServiceOn.set(
        SchemaTableAccessor.schemaServiceIsOn(
          ((HasRegionServerServices) regionEnv).getRegionServerServices().getClusterConnection())
      );
    } catch (IOException e) {
      LOG.warn("Failed get the status of schema table: ", e);
    }
    if (!shouldSendTask(tableName)) {
      return;
    }
    processor.init(false, regionEnv.getConfiguration(),
      ((HasRegionServerServices) regionEnv).getRegionServerServices().getClusterConnection());

    preCheckWorker.submit(() -> {
      try {
        if (shouldSendTask(tableName)) {
          long current = processor.getColumnCount(tableName);
          processor.invalidateCacheIfColumnExceedThreshold(tableName, current);
        }
      } catch (Exception ex) {
        // Only make a log here and continue.
        LOG.warn("Failed checking column number of table: " + tableName, ex);
      }

      if (watchedTables.add(tableName.getNameAsString())) {
        ZKWatcher watcher =
          ((HasRegionServerServices) regionEnv).getRegionServerServices().getZooKeeper();

        try {
          if (watcher != null) {
            String tableNode = ZNodePaths.joinZNode(
              watcher.getZNodePaths().tableZNode, tableName.getNameAsString());
            ZKUtil.setWatchIfNodeExists(watcher, tableNode);
            initListener(watcher);
          }
        } catch (Exception e) {
          LOG.warn("Failed add remove hook for table: " + tableName);
        }
      }
    });
    LOG.info("Finished starting SchemaService for region " + regionEnv.getRegionInfo());
  }

  // Used for region hooks to not send task for system, wide tables and for the case where schema
  // table is unavailable.
  private boolean shouldSendTask(TableName table) {
    return schemaServiceOn.get() && !table.isSystemTable() && !isWideTable(table);
  }
}
