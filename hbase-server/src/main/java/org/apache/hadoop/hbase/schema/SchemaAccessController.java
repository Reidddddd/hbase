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
import java.util.Optional;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.SchemaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessController;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ACL check for user accessing hbase:schema table. It is a table coprocessor.
 * <p>
 * hbase:schema is a system table, user has no permission access it directly, so here
 * in the related hook method, we'll do an acl check for the request user.
 *
 * For getSchema, its call stack:
 * --> preExist
 *      --> preGetOp
 * --> postExist
 * --> preScannerOpen
 * --> postScannerClose
 *
 * For publishSchema, its call stack:
 * --> prePut (for each cell)
 *       --> preBatchMutate
 * --> postPut
 */
@InterfaceAudience.Private
public class SchemaAccessController extends AccessController {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaAccessController.class);

  private static final String CHECK_REQUEST = "checkTableSchemaExistence";
  private static final String READ_REQUEST = "getTableSchema";
  private static final String UPDATE_REQUEST = "updateTableSchema";
  private static final String ACL_CHECK_PASSED = "acl_check_passed";
  private static final byte[] TRUE = Bytes.toBytes(true);

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  @Override
  public boolean preExists(ObserverContext<RegionCoprocessorEnvironment> e,
      Get get, boolean exists) throws IOException {
    if (TableName.SCHEMA_TABLE_NAME.equals(e.getEnvironment().getRegionInfo().getTable())) {
      User user = e.getCaller().get();
      if (shouldSkipCheck(user, get)) {
        return exists;
      }
      TableName table = parseAndCheckRowKeyForTableName(get.getRow(), CHECK_REQUEST, user);
      if (table == null) {
        return exists;
      }
      aclCheck(CHECK_REQUEST, table, user, Permission.Action.READ);
      // short circuit, no need to execute AccessController, it will fail otherwise
      get.setAttribute(ACL_CHECK_PASSED, TRUE);
      return exists;
    } else {
      return super.preExists(e, get, exists);
    }
  }

  @Override
  public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results)
    throws IOException {
    if (TableName.SCHEMA_TABLE_NAME.equals(e.getEnvironment().getRegionInfo().getTable())) {
      User user = e.getCaller().get();
      if (shouldSkipCheck(user, get)) {
        return;
      }

      TableName table = parseAndCheckRowKeyForTableName(get.getRow(), CHECK_REQUEST, user);
      if (table == null) {
        return;
      }
      aclCheck(CHECK_REQUEST, table, user, Permission.Action.READ);
      // short circuit, no need to execute AccessController, it will fail otherwise
      get.setAttribute(ACL_CHECK_PASSED, TRUE);
    } else {
      super.preGetOp(e, get, results);
    }
  }

  @Override
  public void preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan)
    throws IOException {
    if (TableName.SCHEMA_TABLE_NAME.equals(e.getEnvironment().getRegionInfo().getTable())) {
      User user = e.getCaller().get();
      if (shouldSkipCheck(user, scan)) {
        return;
      }

      TableName table = parseAndCheckRowKeyForTableName(scan.getStartRow(), READ_REQUEST, user);
      if (table == null) {
        return;
      }

      aclCheck(READ_REQUEST, table, user, Permission.Action.READ);
      // short circuit, no need to execute AccessController, it will fail otherwise
      scan.setAttribute(ACL_CHECK_PASSED, TRUE);
    } else {
      super.preScannerOpen(e, scan);
    }
  }

  /**
   * The server side logic is first call prePut preDelete for each mutation,
   * then call preBatchMutate.
   *
   * So for accessing hbase:schema, first put must be the table name. If it passes, just skip for
   * the following mutation
   */
  @Override
  public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit,
      Durability durability) throws IOException {
    if (TableName.SCHEMA_TABLE_NAME.equals(e.getEnvironment().getRegionInfo().getTable())) {
      User user = e.getCaller().get();
      if (shouldSkipCheck(user, put)) {
        return;
      }

      TableName table = parseAndCheckRowKeyForTableName(put.getRow(), UPDATE_REQUEST, user);
      aclCheck(UPDATE_REQUEST, table, user, Permission.Action.WRITE);
      // short circuit, no need to execute AccessController, it will fail otherwise
      put.setAttribute(ACL_CHECK_PASSED, TRUE);
    } else {
      super.prePut(e, put, edit, durability);
    }
  }

  @Override
  public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> e,
      MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    if (TableName.SCHEMA_TABLE_NAME.equals(e.getEnvironment().getRegionInfo().getTable())) {
      User user = e.getCaller().get();
      if (shouldSkipCheck(user, miniBatchOp.getOperation(0))) {
        return;
      }

      for (int i = 0; i < miniBatchOp.size(); i++) {
        Mutation mutation = miniBatchOp.getOperation(i);
        TableName table = parseAndCheckRowKeyForTableName(mutation.getRow(), UPDATE_REQUEST, user);
        aclCheck(UPDATE_REQUEST, table, user, Permission.Action.WRITE);
        // short circuit, no need to execute AccessController, it will fail otherwise
        mutation.setAttribute(ACL_CHECK_PASSED, TRUE);
      }
    } else {
      super.preBatchMutate(e, miniBatchOp);
    }
  }

  private final String AUDIT_FORMAT =
    "user: %s, action: %s, target_table: %s, access_check: %s, error_msg: %s";

  private enum MESSAGE {
    NA("n/a"),
    INVALID_TABLE("invalid table name"),
    TABLE_NOT_AVAILABLE("table is not available"),
    SYSTEM_TABLE("it is a system table"),
    READ_ACCESS_DENIED("access denied, need READ permission"),
    WRITE_ACCESS_DENIED("access denied, need WRITE permission"),
    MALFORMED_REQUEST("malformed request");

    private String msg;
    MESSAGE(String msg) {
      this.msg = msg;
    }
  }

  private void auditFailure(User user, String action, String table, MESSAGE error)
    throws AccessDeniedException {
    LOG.info(String.format(AUDIT_FORMAT, user, action, table, "fail", error.msg));
    throw new AccessDeniedException(user + " operates " + action + " with error: " + error.msg);
  }

  private void auditSuccess(User user, String action, String table, MESSAGE error) {
    LOG.info(String.format(AUDIT_FORMAT, user, action, table, "pass", error.msg));
  }

  private boolean shouldSkipCheck(User user, OperationWithAttributes op) {
      // Not normal case, return true to hand over this to access controller.
    return user == null || Superusers.isSuperUser(user)
      || op.getAttribute(ACL_CHECK_PASSED) != null;
  }

  // Extracted duplicated logic in every hook.
  // To parse the table name from given rowkey bytes.
  // When we turn on the authentication, there are three cases to throw IOE in this method.
  // 1. Target table unavailable.
  // 2. Invalid byte[] row.
  // 3. Target table is a system table.
  private TableName parseAndCheckRowKeyForTableName(byte[] row, String request, User user)
    throws IOException {
    TableName table = null;
    try {
      table = SchemaTableAccessor.parseTableNameFromRowKey(row);
    } catch (Throwable t) {
      // like invalid table name, unexpected callMESSAGE
      auditFailure(user, request, Bytes.toString(row), MESSAGE.INVALID_TABLE);
    }
    if (table == null) {
      return null;
    }
    if (table.isSystemTable()) {
      auditFailure(user, request, table.getNameAsString(), MESSAGE.SYSTEM_TABLE);
      return null;
    }
    return table;
  }

  private void aclCheck(String request, TableName table, User user, Permission.Action action)
    throws IOException {
    if (table == null) {
      // Null table name means a parse failure in the previous step.
      // We only arrive here when the authentication is off.
      // Do this check and throw a new AccessDeniedException to tell the client we cannot parse
      // the table name from the request.
      auditFailure(user, request, "Unknown table", MESSAGE.MALFORMED_REQUEST);
      throw new AccessDeniedException("Unknown table");
    }
    try {
      // in schema region, AccessChecker must not be null
      accessChecker.requireTablePermission(user, request, table, null, null, action);
    } catch (IOException ioe) {
      switch (action) {
        case READ:
          auditFailure(user, request, table.getNameAsString(), MESSAGE.READ_ACCESS_DENIED);
          break;
        case WRITE:
          auditFailure(user, request, table.getNameAsString(), MESSAGE.WRITE_ACCESS_DENIED);
          break;
        default:
          break;
      }
      throw ioe;
    }
    auditSuccess(user, request, table.getNameAsString(), MESSAGE.NA);
  }
}
