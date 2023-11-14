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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
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
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The hbase:schema architecture is:
 * Row key               |    q      |
 *                       | a | b | c |
 * table_name            | _ | _ |   |
 * table_nameQualifier_1 | _ |   |   |
 * table_nameQualifier_2 |   | _     |
 * table_name1           |   |   | _ |
 *
 * First of all, the family char is q in hbase:schema.
 * Qualifier will be families of each table, value is no need here.
 * Row key will be table name of a table or table_name + qualifier of a table.
 *
 * For example, from above, we can tell:
 * Table 'table_name' has two families, one is 'a', the other is 'b',
 * then 'table_name' has a 'Qualifier_1' which under family 'a',
 * and 'Qualifier_2' under family 'b'.
 *
 * The value of each cell will be type of a column in the future. For example:
 * string/int/long, can be put into the value to indicate the type
 *
 * With design, we can:
 * 1. avoid fat row or fat cell
 * 2. scalable with more qualifiers, e.g, put the type of a column into the value
 * 3. Easy to achieve a table's information with prefix scan
 *
 * Note, this coprocessor service is bind to a RS at region level.
 * Its lifecycle follows RS's.
 * So even a region moved or offline, it can't be shutdown until RS dead.
 * TODO: find an elegantly way to shutdown this service
 */
@CoreCoprocessor
@InterfaceAudience.Private
public class SchemaService implements MasterCoprocessor, RegionCoprocessor,
  MasterObserver, RegionObserver {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaService.class);
  static final String NUM_THREADS_KEY = "hbase.schema.updater.threads";
  static final int NUM_THREADS_DEFAULT = 5;

  // q for qolumn
  static final byte[] SCHEMA_TABLE_CF = Bytes.toBytes("q");

  private SchemaProcessor processor;

  enum Operation {
    PUT, INCREMENT, APPEND, TRUNCATE, DROP
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
    LOG.info("Starting Schema Service");
    LOG.info("Is Master enviroment: " + (e instanceof MasterCoprocessorEnvironment));
    Configuration conf = e.getConfiguration();

    boolean masterEnv = e instanceof MasterCoprocessorEnvironment;
    processor = SchemaProcessor.getInstance();
    processor.init(masterEnv, conf);

    if (masterEnv) {
      // if running on HMaster
      MasterCoprocessorEnvironment mEnv = (MasterCoprocessorEnvironment) e;
      if (mEnv instanceof HasMasterServices) {
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
    );
    masterServices.createSystemTable(desc);
  }

  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
    sendTask(tableName, Operation.DROP);
  }

  @Override
  public void postTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
    sendTask(tableName, Operation.TRUNCATE);
  }

  @Override
  public Result postIncrement(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Increment increment, final Result result) throws IOException {
    sendTask(e.getEnvironment().getRegionInfo().getTable(),
             Operation.INCREMENT,
             increment.getFamilyCellMap());
    return result;
  }

  @Override
  public void postPut(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Put put, final WALEdit edit, final Durability durability) throws IOException {
    sendTask(e.getEnvironment().getRegionInfo().getTable(),
             Operation.PUT,
             put.getFamilyCellMap());
  }

  @Override
  public Result postAppend(ObserverContext<RegionCoprocessorEnvironment> e, Append append,
      Result result) throws IOException {
    sendTask(e.getEnvironment().getRegionInfo().getTable(),
             Operation.APPEND,
             append.getFamilyCellMap());
    return result;
  }

  @Override
  public void postBatchMutate(final ObserverContext<RegionCoprocessorEnvironment> e,
    final MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    TableName table = e.getEnvironment().getRegionInfo().getTable();
    if (table.isSystemTable()) {
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
    if (table.isSystemTable()) {
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

}
