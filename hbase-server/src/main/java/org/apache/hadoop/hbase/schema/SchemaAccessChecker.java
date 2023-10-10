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

import static org.apache.hadoop.hbase.HConstants.EMPTY_BYTE_ARRAY;
import java.io.IOException;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

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
public class SchemaAccessChecker extends BaseRegionObserver {
  private static final Log LOG = LogFactory.getLog(SchemaAccessChecker.class);

  private AccessChecker aclChecker;

  private ThreadLocal<Boolean> passedACL = ThreadLocal.withInitial(() -> false);
  // for abnormal batch puts check
  private ThreadLocal<byte[]> passedTable = new ThreadLocal<>();

  private Admin admin;

  private boolean authorizationEnabled;

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    LOG.info("Starting SchemaAccessChecker");

    RegionCoprocessorEnvironment regionEnv = (RegionCoprocessorEnvironment) e;
    admin = ConnectionFactory.createConnection(regionEnv.getConfiguration()).getAdmin();
    aclChecker = new AccessChecker(regionEnv.getConfiguration(),
                                   regionEnv.getRegionServerServices().getZooKeeper());
    authorizationEnabled = AccessChecker.isAuthorizationSupported(regionEnv.getConfiguration());
  }

  @Override
  public void stop(CoprocessorEnvironment e) throws IOException {
    admin.close();
  }

  @Override
  public boolean preExists(ObserverContext<RegionCoprocessorEnvironment> e,
      Get get, boolean exists) throws IOException {
    User user = RpcServer.getRequestUser();
    if (Superusers.isSuperUser(user)) {
      return exists;
    }
    assert user != null;

    TableName table = null;
    try {
      table = TableName.valueOf(get.getRow());
    } catch (Throwable t) {
      // like invalid table name, unexpected call to preExists
      auditFailure(user, "checkTableSchemaExistence",
                   Bytes.toString(get.getRow()), MESSAGE.INVALID_TABLE);
      return exists;
    }
    if (!admin.isTableAvailable(table)) {
      auditFailure(user, "checkTableSchemaExistence",
                   table.getNameAsString(), MESSAGE.TABLE_NOT_AVAILABLE);
      return exists;
    }
    if (table.isSystemTable()) {
      auditFailure(user, "checkTableSchemaExistence",
                   table.getNameAsString(), MESSAGE.SYSTEM_TABLE);
      return exists;
    }

    try {
      // in schema region, AccessChecker must not be null
      aclChecker.requirePermission(user, "checkTableSchemaExistence", table,
          null, null, Permission.Action.READ);
    } catch (IOException ioe) {
      auditFailure(user, "checkTableSchemaExistence",
                   table.getNameAsString(), MESSAGE.READ_ACCESS_DENIED);
      throw ioe;
    }
    auditSuccess(user, "checkTableSchemaExistence",
                 table.getNameAsString(), MESSAGE.NA);
    // short circuit, no need to execute AccessController, it will fail otherwise
    passedACL.set(true);
    e.complete();
    return exists;
  }

  @Override
  public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results)
      throws IOException {
    if (passedACL.get()) {
      e.complete();
    } else {
      User user = RpcServer.getRequestUser();
      auditFailure(user, "checkTableSchemaExistence",
                   Bytes.toString(get.getRow()), MESSAGE.MALFORMED_REQUEST);
    }
  }

  @Override
  public boolean postExists(ObserverContext<RegionCoprocessorEnvironment> e, Get get,
      boolean exists) throws IOException {
    passedACL.set(false);
    return exists;
  }

  @Override
  public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan,
      RegionScanner s) throws IOException {
    User user = RpcServer.getRequestUser();
    if (Superusers.isSuperUser(user)) {
      return s;
    }
    assert user != null;

    TableName table = null;
    try {
      table = TableName.valueOf(scan.getStartRow());
    } catch (Throwable t) {
      // like invalid table name, unexpected call
      auditFailure(user, "getTableSchema",
                   Bytes.toString(scan.getStartRow()), MESSAGE.INVALID_TABLE);
      return s;
    }
    if (!admin.isTableAvailable(table)) {
      auditFailure(user, "getTableSchema",
                   table.getNameAsString(), MESSAGE.TABLE_NOT_AVAILABLE);
      return s;
    }
    if (table.isSystemTable()) {
      auditFailure(user, "getTableSchema",
                   table.getNameAsString(), MESSAGE.SYSTEM_TABLE);
      return s;
    }

    try {
      // in schema region, AccessChecker must not be null
      aclChecker.requirePermission(user, "getTableSchema", table,
          null, null, Permission.Action.READ);
    } catch (IOException ioe) {
      auditFailure(user, "getTableSchema",
                   table.getNameAsString(), MESSAGE.READ_ACCESS_DENIED);
      throw ioe;
    }
    auditSuccess(user, "getTableSchema",
                 table.getNameAsString(), MESSAGE.NA);

    // short circuit, no need to execute AccessController, it will fail otherwise
    passedACL.set(true);
    e.complete();
    return s;
  }

  @Override
  public void postScannerClose(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s)
      throws IOException {
    passedACL.set(false);
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
    User user = RpcServer.getRequestUser();
    if (Superusers.isSuperUser(user)) {
      return;
    }
    assert user != null;

    if (passedACL.get()) {
      if (Bytes.startsWith(put.getRow(), passedTable.get())) {
        // it means, put operates on a same table, should be safe
        e.complete();
        return;
      }
      // probably a problematic call
      auditFailure(user, "updateTableSchema",
                   Bytes.toString(put.getRow()), MESSAGE.MALFORMED_REQUEST);
      return;
    }

    TableName table = null;
    try {
      table = TableName.valueOf(put.getRow());
    } catch (Throwable t) {
      // like invalid table name, unexpected callMESSAGE
      auditFailure(user, "updateTableSchema",
                   Bytes.toString(put.getRow()), MESSAGE.INVALID_TABLE);
      return;
    }
    if (!admin.isTableAvailable(table)) {
      auditFailure(user, "updateTableSchema",
                   table.getNameAsString(), MESSAGE.TABLE_NOT_AVAILABLE);
      return;
    }
    if (table.isSystemTable()) {
      auditFailure(user, "updateTableSchema",
                   table.getNameAsString(), MESSAGE.SYSTEM_TABLE);
      return;
    }

    try {
      // in schema region, AccessChecker must not be null
      aclChecker.requirePermission(user, "updateTableSchema", table,
          null, null, Permission.Action.WRITE);
    } catch (IOException ioe) {
      auditFailure(user, "updateTableSchema",
                   table.getNameAsString(), MESSAGE.WRITE_ACCESS_DENIED);
      throw ioe;
    }
    auditSuccess(user, "updateTableSchema",
                 table.getNameAsString(), MESSAGE.NA);

    // short circuit, no need to execute AccessController, it will fail otherwise
    e.complete();
    passedACL.set(true);
    passedTable.set(put.getRow());
  }

  @Override
  public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    if (passedACL.get()) {
      c.complete();
    }
  }

  @Override
  public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit,
      Durability durability) throws IOException {
    if (passedACL.get()) {
      passedACL.set(false);
      passedTable.set(EMPTY_BYTE_ARRAY);
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
    if (authorizationEnabled) {
      throw new AccessDeniedException(user + " operates " + action + " with error: " + error.msg);
    }
  }

  private void auditSuccess(User user, String action, String table, MESSAGE error) {
    LOG.info(String.format(AUDIT_FORMAT, user, action, table, "pass", error.msg));
  }

}
