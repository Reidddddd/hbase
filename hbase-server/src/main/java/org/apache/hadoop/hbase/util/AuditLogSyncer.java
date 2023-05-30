/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.util;


import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class AuditLogSyncer extends AbstractAuditLogSyncer {
  private final boolean auditMore;
  private final int rowKeyLimit;
  private final int logRecordLimit;
  private final int regionNameLimit;
  private String sampleEvent = "";

  private static final String PTIME = "p_time:";
  private static final String QTIME = "q_time:";
  private static final String REQUSIZE = "req_size:";
  private static final String RESPSIZE = "resp_size:";
  private static final String TAG = "tag:";
  private static final String USERNAME = "username:";
  private static final String REALUSER = "real_user:";
  private static final String AUTHMETHOD = "auth_meth:";
  private static final String CLIENTIP = "ip:";
  private static final String METHODNAME = "action:";
  private static final String COUNT = "action_count:";
  private static final String EVENTCOUNT = "event_count:";
  private static final String DETAIL_ACTION = "d_actions:";
  private static final String REGIONS = "regions:";

  private static final String NULL_CELL = "null";

  public static final String CONF_AUDIT = "hbase.security.audit";
  public static final String CONF_AUDIT_FLUSH_INTERVAL = "hbase.security.audit.flush.interval";
  public static final String CONF_AUDIT_MORE_INFO = "hbase.security.audit.more.info";
  public static final String CONF_AUDIT_ROW_KEY_SIZE = "hbase.security.audit.row.key.size";
  public static final String CONF_AUDIT_RECORD_LIMIT = "hbase.security.audit.log.record.limit";
  public static final String CONF_AUDIT_REGION_NAME_SIZE = "hbase.security.audit.region.name.size";

  public static final boolean DEFAULT_AUDIT = true;
  public static final boolean DEFAULT_AUDIT_MORE_INFO = false;
  public static final Long DEFAULT_AUDIT_FLUSH_INTERVAL = 1000L;
  public static final int DEFAULT_AUDIT_ROW_KEY_SIZE = 100;
  public static final int DEFAULT_AUDIT_RECORD_LIMIT = 10000;
  public static final int DEFAULT_AUDIT_REGION_NAME_SIZE = -1;

  private static final ThreadLocal<StringBuilder> auditBuffer = new ThreadLocal<StringBuilder>() {
    @Override
    protected StringBuilder initialValue() {
      return new StringBuilder();
    }
    };

  private static final ThreadLocal<TreeSet<String>> actSet = new ThreadLocal<TreeSet<String>>() {
    @Override
    protected TreeSet<String> initialValue() {
      return new TreeSet<String>();
    }
    };

  private static final ThreadLocal<TreeSet<String>> regionSet = new ThreadLocal<TreeSet<String>>() {
    @Override
    protected TreeSet<String> initialValue() {
      return new TreeSet<String>();
    }
  };

  private static final ThreadLocal<TreeSet<String>> familySet = new ThreadLocal<TreeSet<String>>() {
    @Override
    protected TreeSet<String> initialValue() {
      return new TreeSet<String>();
    }
  };

  public AuditLogSyncer(Log LOG, Configuration conf) {
    super(conf.getLong(CONF_AUDIT_FLUSH_INTERVAL, DEFAULT_AUDIT_FLUSH_INTERVAL), LOG);

    this.auditMore = conf.getBoolean(CONF_AUDIT_MORE_INFO, DEFAULT_AUDIT_MORE_INFO);
    this.rowKeyLimit = conf.getInt(CONF_AUDIT_ROW_KEY_SIZE, DEFAULT_AUDIT_ROW_KEY_SIZE);
    this.logRecordLimit = conf.getInt(CONF_AUDIT_RECORD_LIMIT, DEFAULT_AUDIT_RECORD_LIMIT);
    this.regionNameLimit = conf.getInt(CONF_AUDIT_REGION_NAME_SIZE, DEFAULT_AUDIT_REGION_NAME_SIZE);

    if (!this.auditMore) {
      Threads.setDaemonThreadRunning(this.getThread(),
                Thread.currentThread().getName() + ".auditSyncer");
    }
  }

  @Override
  public void auditSync() {
    // log latest warnEvent
    if (sampleEvent.length() != 0) {
      AUDITLOG.info(sampleEvent);
      sampleEvent = "";
      samplingLock.set(false);
    }
    // log other events
    for (Map.Entry<String, AtomicInteger> e : events.entrySet()) {
      int value = events.remove(e.getKey()).get();
      AUDITLOG.info((e.getKey() + "\t" + EVENTCOUNT + value));
    }
  }

  private static final String TAG_EXCEPTION = "E";
  private static final String TAG_TOO_SLOW = "S";
  private static final String TAG_TOO_LARGE = "L";
  private static final String TAG_NORMAL = "O";
  private static final String TAG_SAMPLE = "V";

  public void logResponse(long startTime, int qTime, int warnResponseTime, int warnResponseSize,
                          long requestSize, long responseSize, Throwable exception, User user,
                          Descriptors.MethodDescriptor md, Message param,
                          MonitoredRPCHandler status) {
    int processingTime;
    boolean tooSlow;
    boolean tooLarge;
    String tag = "";

    processingTime = (int) (System.currentTimeMillis() - startTime);
    tooSlow = (processingTime > warnResponseTime && warnResponseTime > -1);
    tooLarge = (responseSize > warnResponseSize && warnResponseSize > -1);
    if (exception != null) {
      tag = tag + TAG_EXCEPTION;
    }
    if (tooSlow) {
      tag = tag + TAG_TOO_SLOW;
    }
    if (tooLarge) {
      tag = tag + TAG_TOO_LARGE;
    }
    if (tag.length() == 0) {
      tag = TAG_NORMAL;
    }
    boolean isWarnEvent = !tag.equals("O");
    if (!auditMore && isWarnEvent) {
      // for auditLess mode, we decide if we want to sample this warnEvent
      // if this warnEvent will not be sampled, mark it as normalEvent
      if (!samplingLock.getAndSet(true)) {
        tag = tag + TAG_SAMPLE;
      } else {
        isWarnEvent = false;
      }
    }
    try {
      if (user != null) {
        logResponse(md.getName(), param, tag, status.getClient(), processingTime, qTime,
                requestSize, responseSize, user.getUGI(), exception, isWarnEvent);
      } else {
        logResponse(md.getName(), param, tag, status.getClient(), processingTime, qTime,
                requestSize, responseSize, null, exception, isWarnEvent);
      }
    } catch (Exception ex) {
      LOG.error("Error when processing audit log.", ex);
    }
  }

  /**
   * Logs an RPC response to the LOG file, producing valid JSON objects for
   * client Operations.
   * <p>
   * LOG format:
   * log_tag username realuser authentication_methods ip action
   * detail_action cf_name region_encode warn_event count
   *
   * @param methodName     The name of the method invoked
   * @param param          The parameters received in the call.
   * @param tag            The tag that will be used to indicate this event in the log.
   * @param clientAddress  The address of the client who made this call.
   * @param processingTime The duration that the call took to run, in ms.
   * @param qTime          The duration that the call spent on the queue
   *                       prior to being initiated, in ms.
   * @param requestSize    The size in bytes of the request buffer.
   * @param responseSize   The size in bytes of the response buffer.
   */
  void logResponse(String methodName, Message param, String tag, String clientAddress,
                   int processingTime, int qTime, long requestSize, long responseSize,
                   UserGroupInformation user, Throwable exception, boolean warnEvent) {
    final StringBuilder sb = auditBuffer.get();
    final TreeSet<String> acts = actSet.get();
    final TreeSet<String> fams = familySet.get();
    final TreeSet<String> regions = regionSet.get();
    sb.setLength(0);
    // base information that is reported regardless of type of call
    sb.append(TAG).append(tag).append("\t");
    boolean nullUser = false;
    if (user == null) {
      nullUser = true;
    }

    if (!nullUser) {
      sb.append(USERNAME).append(user.getUserName()).append("\t");
    } else {
      sb.append(USERNAME).append(NULL_CELL).append("\t");
    }
    if (!nullUser) {
      if (user.getRealUser() != null) {
        sb.append(REALUSER).append(user.getRealUser().getUserName()).append("\t");
      }
    } else {
      sb.append(REALUSER).append(NULL_CELL + "\t");
    }
    if (!nullUser) {
      sb.append(AUTHMETHOD).append(user.getAuthenticationMethod().name().charAt(0)).append("\t");
    } else {
      sb.append(AUTHMETHOD).append(NULL_CELL).append("\t");
    }
    sb.append(CLIENTIP).append(clientAddress).append("\t");

    // append processing info
    sb.append(PTIME).append(processingTime).append("\t");
    sb.append(QTIME).append(qTime).append("\t");
    sb.append(REQUSIZE).append(requestSize).append("\t");
    sb.append(RESPSIZE).append(responseSize).append("\t");

    if (exception != null) {
      sb.append(exception.toString().trim().replaceAll("\\s+", " ")).append("\t");
    }

    sb.append(METHODNAME).append(methodName).append("\t");
    MutableInt actionCount = new MutableInt(0);
    // append parameters
    try {
      appendParameters(methodName, param, warnEvent, actionCount);
    } catch (Exception ex) {
      LOG.warn("Failed to append params for method=" + methodName, ex);
    } finally {
      int count = actionCount.intValue();
      /** If auditmore is enable, log the event immediately */
      /** If is a normal log, flush it later */
      if (auditMore) {
        AUDITLOG.info(sb.toString() + COUNT + count);
      } else if (warnEvent) {
        sampleEvent = sb.toString() + COUNT + count;
      } else {
        // buffer the log event and count its occurrence
        // syncer will flush it later
        appendSet(acts, DETAIL_ACTION);
        appendSet(fams, FAMILYS);
        appendSet(regions, REGIONS);
        String key = sb.toString() + COUNT + count;
        // this is not thread safe if syncer removes the entry
        // before increment events might be lost
        if (events.putIfAbsent(key, new AtomicInteger(1)) != null) {
          AtomicInteger value = events.get(key);
          if (value != null) {
            value.getAndIncrement();
          }
        }
      }
    }
  }

  private void appendParameters(String methodName, Message param, boolean warnEvent,
    MutableInt actionCount) {
    final StringBuilder sb = auditBuffer.get();

    if (methodName.equals("Get")) {
      ClientProtos.GetRequest request = (ClientProtos.GetRequest) param;
      ClientProtos.Get get = request.getGet();
      appendRegion(request.getRegion().getValue(), warnEvent);
      appendAction(get, warnEvent, actionCount);
    } else if (methodName.equals("Mutate")) {
      ClientProtos.MutateRequest request = (ClientProtos.MutateRequest) param;
      ClientProtos.MutationProto mutation = request.getMutation();
      appendRegion(request.getRegion().getValue(), warnEvent);
      appendAction(mutation, warnEvent, actionCount);
    } else if (methodName.equals("Scan")) {
      ClientProtos.ScanRequest request = (ClientProtos.ScanRequest) param;
      appendRegion(request.getRegion().getValue(), warnEvent);
      if (warnEvent) {
        appendScannerInfo(request.getScan());
      }
      actionCount.increment();
    } else if (methodName.equals("BulkLoadHFile")) {
      ClientProtos.BulkLoadHFileRequest request = (ClientProtos.BulkLoadHFileRequest) param;
      appendRegion(request.getRegion().getValue(), warnEvent);
    } else if (methodName.equals("ExecService")) {
      ClientProtos.CoprocessorServiceRequest request =
        (ClientProtos.CoprocessorServiceRequest) param;
      appendRegion(request.getRegion().getValue(), warnEvent);
      sb.append(request.getCall().getMethodName()).append("\t");
    } else if (methodName.equals("Multi")) {
      ClientProtos.MultiRequest request = (ClientProtos.MultiRequest) param;
      for (ClientProtos.RegionAction regionAction : request.getRegionActionList()) {
        ByteString regionName = regionAction.getRegion().getValue();
        appendRegion(regionName, warnEvent);
        for (ClientProtos.Action action : regionAction.getActionList()) {
          if (action.hasGet()) {  // get
            appendAction(action.getGet(), warnEvent, actionCount);
          } else if (action.hasMutation()) {  // mutate
            appendAction(action.getMutation(), warnEvent, actionCount);
          }
        }
      }
    }
  }

  private void appendAction(Object action, boolean warnEvent, MutableInt actionCount) {
    final StringBuilder sb = auditBuffer.get();
    actionCount.increment();
    if (sb.length() > logRecordLimit) {
      return;
    }
    /** If action is get, append a "g" */
    if (action instanceof ClientProtos.Get) {
      ClientProtos.Get get = (ClientProtos.Get) action;
      appendActionName("g", warnEvent);
      appendRow(get.getRow().toByteArray(), warnEvent);
      appendColumns(get.getColumnList(), warnEvent);
    } else if (action instanceof ClientProtos.MutationProto) {
      ClientProtos.MutationProto mutation = (ClientProtos.MutationProto) action;
      ClientProtos.MutationProto.MutationType type = mutation.getMutateType();
      appendActionName(type.toString().toLowerCase().substring(0, 1), warnEvent);
      appendRow(mutation.getRow().toByteArray(), warnEvent);
      appendColumnValues(mutation.getColumnValueList(), warnEvent);
    }
  }

  private void appendActionName(String act, boolean warnEvent) {
    final StringBuilder sb = auditBuffer.get();
    final TreeSet<String> acts = actSet.get();
    if (auditMore || warnEvent) {
      sb.append(act).append("\t");
    } else {
      acts.add(act);
    }
  }

  private static final String START_ROW = "start_row:";
  private static final String STOP_ROW = "stop_row:";
  private static final String MAX_VERSIONS = "max_vers:";
  private static final String CACHE_BLOCKS = "cache_blocks:";
  private static final String HAS_CACHING = "has_caching:";
  private static final String FILTERS = "filters:";
  private static final String START_TS = "start_ts:";
  private static final String STOP_TS = "stop_ts:";

  private void appendScannerInfo(ClientProtos.Scan scan) {
    final StringBuilder sb = auditBuffer.get();
    if (scan.hasStartRow()) {
      sb.append(START_ROW).append(
              scan.getStartRow().substring(0, Math.min(scan.getStartRow().size(), rowKeyLimit))
        .toStringUtf8()).append("\t");
    } else {
      sb.append(START_ROW).append(NULL_CELL + "\t");
    }

    if (scan.hasStopRow()) {
      sb.append(STOP_ROW).append(scan.getStopRow().substring(0,
        Math.min(scan.getStopRow().size(), rowKeyLimit)).toStringUtf8()).append("\t");
    } else {
      sb.append(STOP_ROW).append(NULL_CELL + "\t");
    }

    if (scan.hasMaxVersions()) {
      sb.append(MAX_VERSIONS).append(scan.getMaxVersions()).append("\t");
    } else {
      sb.append(MAX_VERSIONS).append(NULL_CELL + "\t");
    }

    if (scan.hasCacheBlocks()) {
      sb.append(CACHE_BLOCKS).append(scan.getCacheBlocks()).append("\t");
    } else {
      sb.append(CACHE_BLOCKS).append(NULL_CELL + "\t");
    }

    if (scan.hasCaching()) {
      sb.append(HAS_CACHING).append(scan.getCaching()).append("\t");
    } else {
      sb.append(HAS_CACHING).append(NULL_CELL + "\t");
    }

    if (scan.hasFilter()) {
      sb.append(FILTERS).append(scan.getFilter().toString()).append("\t");
    } else {
      sb.append(FILTERS).append(NULL_CELL + "\t");
    }

    if (scan.hasTimeRange()) {
      sb.append(START_TS).append(scan.getTimeRange().getFrom()).append("\t");
      sb.append(STOP_TS).append(scan.getTimeRange().getTo()).append("\t");
    } else {
      sb.append(NULL_CELL + "\t").append(NULL_CELL + "\t");
    }
  }

  private void appendRegion(ByteString regionName, boolean warnEvent) {
    final StringBuilder sb = auditBuffer.get();
    final TreeSet<String> regions = regionSet.get();
    if (regionName.size() == 0) {
      if (auditMore || warnEvent) {
        sb.append(NULL_CELL + "\t");
      }
      return;
    }
    String tableName = getTableName(regionName);
    String encodedName = getEncodedName(regionName);
    if (sb.length() > logRecordLimit || regions.size() > logRecordLimit / 32) {
      return;
    }
    if (regionNameLimit != -1) {
      encodedName = encodedName.substring(Math.max(0, encodedName.length() - regionNameLimit));
    }
    encodedName = tableName + "." + encodedName;
    // if we can't find encode name, it is a meta region
    if (encodedName.length() == 0) {
      encodedName = "meta";
    }
    if (auditMore || warnEvent) {
      sb.append(encodedName).append("\t");
    } else {
      regions.add(encodedName);
    }
  }

  private static String getTableName(ByteString regionName) {
    final int TABLE_DELTIMITER = ',';
    int i;
    for (i = 0; i < regionName.size(); i++) {
      if (regionName.byteAt(i) == TABLE_DELTIMITER) {
        break;
      }
    }
    if (i < regionName.size() - 2) {
      return regionName.substring(0, i).toStringUtf8();
    } else {
      return "";
    }
  }

  private static final int DELIMITER = '.';

  private static String getEncodedName(ByteString regionName) {
    int i = 0;
    for (i = 0; i < regionName.size(); i++) {
      if (regionName.byteAt(i) == DELIMITER) {
        break;
      }
    }
    if (i < regionName.size() - 2) {
      return regionName.substring(i + 1, regionName.size() - 1).toStringUtf8();
    } else {
      return "";
    }
  }

  private final String ROW = "row:";

  private void appendRow(byte[] row, boolean warnEvent) {
    if (auditMore || warnEvent) {
      final StringBuilder sb = auditBuffer.get();
      sb.append(ROW);
      if (row.length == 0) {
        sb.append(NULL_CELL + "\t");
        return;
      }
      sb.append(Bytes.toStringBinary(row, 0, Math.min(row.length, rowKeyLimit))).append("\t");
    }
  }

  private final String FAMILYS = "cfs:";

  private void appendColumns(List<ClientProtos.Column> columns, boolean warnEvent) {
    final StringBuilder sb = auditBuffer.get();
    final TreeSet<String> fams = familySet.get();
    if (!auditMore && !warnEvent) {
      for (ClientProtos.Column c : columns) {
        fams.add(c.getFamily().toStringUtf8());
      }
      return;
    }
    if (columns.size() == 0) {
      sb.append(NULL_CELL + "\t");
      return;
    }
    String delim = FAMILYS;
    for (ClientProtos.Column c : columns) {
      sb.append(delim).append(c.getFamily().toStringUtf8());
      delim = "/";
    }
    sb.append("\t");
  }

  private void appendColumnValues(
          List<ClientProtos.MutationProto.ColumnValue> columns, boolean warnEvent) {
    final StringBuilder sb = auditBuffer.get();
    final TreeSet<String> fams = familySet.get();
    if (!auditMore && !warnEvent) {
      for (ClientProtos.MutationProto.ColumnValue c : columns) {
        fams.add(c.getFamily().toStringUtf8());
      }
      return;
    }
    if (columns.size() == 0) {
      sb.append(NULL_CELL + "\t");
      return;
    }
    String delim = "";
    for (ClientProtos.MutationProto.ColumnValue c : columns) {
      sb.append(delim).append(c.getFamily().toStringUtf8());
      delim = "/";
    }
    sb.append("\t");
  }

  private void appendSet(TreeSet<String> set, String name) {
    final StringBuilder sb = auditBuffer.get();
    sb.append(name);
    if (set.isEmpty()) {
      sb.append(NULL_CELL + "\t");
      return;
    }
    String delim = "";
    for (String s : set) {
      sb.append(delim).append(s);
      delim = "/";
    }
    sb.append("\t");
    set.clear();
  }
}
