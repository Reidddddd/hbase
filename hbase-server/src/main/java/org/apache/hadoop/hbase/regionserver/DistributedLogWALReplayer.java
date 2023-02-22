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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.regionserver.HRegion.rowIsInRange;
import dlshade.org.apache.distributedlog.api.DistributedLogManager;
import dlshade.org.apache.distributedlog.api.namespace.Namespace;
import java.io.EOFException;
import java.io.IOException;
import java.text.ParseException;
import java.util.Map;
import java.util.NavigableSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.regionserver.wal.DistributedLogAccessor;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.Entry;
import org.apache.hadoop.hbase.wal.Reader;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALSplitterUtil;
import org.apache.hadoop.hbase.wal.WALUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class DistributedLogWALReplayer extends WALReplayer {
  private static final Log LOG = LogFactory.getLog(DistributedLogWALReplayer.class);

  private final Namespace walNamespace;

  private final int interval;
  private final int period;
  private final RegionCoprocessorHost coprocessorHost;


  protected DistributedLogWALReplayer(Configuration conf, HRegionInfo regionInfo,
    Map<byte[], Long> maxSeqIdInStores, HRegion hRegion) throws IOException {
    super(conf, regionInfo, maxSeqIdInStores, hRegion);
    try {
      walNamespace = DistributedLogAccessor.getInstance(conf).getNamespace();
    } catch (Exception e) {
      throw new IOException(e);
    }
    // How many edits seen before we check elapsed time
    interval = this.conf.getInt("hbase.hstore.report.interval.edits", 2000);
    // How often to send a progress report (default 1/2 master timeout)
    period = this.conf.getInt("hbase.hstore.report.period", 300000);
    this.coprocessorHost = hRegion.getCoprocessorHost();
  }

  @Override
  long replayRecoveredEditsIfAny(Map<byte[], Long> maxSeqIdInStores,
    CancelableProgressable reporter, MonitoredTask status) throws IOException {
    long minSeqIdForTheRegion = -1;
    for (Long maxSeqIdInStore : maxSeqIdInStores.values()) {
      if (maxSeqIdInStore < minSeqIdForTheRegion || minSeqIdForTheRegion == -1) {
        minSeqIdForTheRegion = maxSeqIdInStore;
      }
    }
    long seqId = minSeqIdForTheRegion;

    NavigableSet<Path> logs = WALSplitterUtil.getSplitEditLogsSorted(walNamespace, regionWALDir);

    LOG.info("Starting get replaying logs for region " + regionWALDir);
    for (Path log : logs) {
      LOG.info("Get logs sorted with region " + regionWALDir + " log name: " + log);
    }
    seqId = Math.max(seqId, replayRecoveredEditsForPaths(minSeqIdForTheRegion, logs, reporter));
    if (seqId > minSeqIdForTheRegion) {
      // Then we added some edits to memory. Flush and cleanup split edit files.
      hRegion.internalFlushcache(null, seqId, hRegion.getStores(), status, false);
    }

    for (Path log : logs) {
      WALUtils.deleteLogsUnderPath(walNamespace, WALUtils.pathToDistributedLogName(log),
        DistributedLogAccessor.getDistributedLogStreamName(conf), true);
    }
    return seqId;
  }

  @Override
  long writeRegionSequenceIdFile(long newSeqId, long saftyBumper) throws IOException {
    return WALSplitterUtil.writeRegionSequenceIdLog(walNamespace, regionWALDir, newSeqId,
      saftyBumper);
  }

  @Override
  void initRegionWALDir() throws IOException {
    TableName tableName = this.regionInfo.getTable();
    Path tablePath = new Path(tableName.getNamespaceAsString(), tableName.getQualifierAsString());
    this.regionWALDir = new Path(tablePath, regionInfo.getEncodedName());
  }

  private long replayRecoveredEditsForPaths(long minSeqIdForTheRegion,
      final NavigableSet<Path> files, final CancelableProgressable reporter)
    throws IOException {
    for (Path file : files) {
      LOG.info("Recover file: " + file);
    }
    long seqid = minSeqIdForTheRegion;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Found " + (files == null ? 0 : files.size()) +
        " recovered edits file(s) under " + getWALRegionDir());
    }

    if (files == null || files.isEmpty()) {
      return seqid;
    }

    for (Path edits : files) {
      if (edits == null || !walNamespace.logExists(WALUtils.pathToDistributedLogName(edits))) {
        LOG.warn("Null or non-existent edits file: " + edits);
        continue;
      }
      DistributedLogManager dlm = walNamespace.openLog(WALUtils.pathToDistributedLogName(edits));
      if (dlm.getLogRecordCount() < 1) {
        continue;
      }

      long maxSeqId;
      String fileName = edits.getName();
      maxSeqId = Math.abs(Long.parseLong(fileName));
      if (maxSeqId <= minSeqIdForTheRegion) {
        if (LOG.isDebugEnabled()) {
          String msg = "Maximum sequenceid for this wal is " + maxSeqId +
            " and minimum sequenceid for the region is " + minSeqIdForTheRegion
            + ", skipped the whole file, path=" + edits;
          LOG.debug(msg);
        }
        continue;
      }

      try {
        // replay the edits. Replay can return -1 if everything is skipped, only update
        // if seqId is greater
        seqid = Math.max(seqid, replayRecoveredEdits(edits, maxSeqIdInStores, reporter));
      } catch (IOException e) {
        boolean skipErrors = conf.getBoolean(HConstants.HREGION_EDITS_REPLAY_SKIP_ERRORS,
          conf.getBoolean("hbase.skip.errors",
            HConstants.DEFAULT_HREGION_EDITS_REPLAY_SKIP_ERRORS));
        if (conf.get("hbase.skip.errors") != null) {
          LOG.warn("The property 'hbase.skip.errors' has been deprecated. Please use " +
            HConstants.HREGION_EDITS_REPLAY_SKIP_ERRORS + " instead.");
        }
        if (skipErrors) {
          Path p = WALSplitterUtil.moveAsideBadEditsLog(walNamespace, edits);
          LOG.error(HConstants.HREGION_EDITS_REPLAY_SKIP_ERRORS +
            "=true so continuing. Renamed " + edits + " as " + p, e);
        } else {
          throw e;
        }
      }
    }
    return seqid;
  }

  private long replayRecoveredEdits(final Path edits, Map<byte[], Long> maxSeqIdInStores,
      final CancelableProgressable reporter) throws IOException {
    String msg = "Replaying edits from " + edits;
    LOG.info(msg);
    MonitoredTask status = TaskMonitor.get().createStatus(msg);
    status.setStatus("Opening recovered edits");
    try (Reader reader = WALUtils.createReader(null, edits, conf)) {
      long currentEditSeqId = -1, currentReplaySeqId = -1, firstSeqIdInLog = -1, skippedEdits = 0;
      long editsCount = 0, intervalEdits = 0;
      Entry entry;
      Store store = null;
      boolean reported_once = false;
      ServerNonceManager ng =
        this.hRegion.rsServices == null ? null : this.hRegion.rsServices.getNonceManager();

      try {
        long lastReport = EnvironmentEdgeManager.currentTime();
        preReplayWALs(this.regionInfo, edits);
        while ((entry = reader.next()) != null) {
          WALKey key = entry.getKey();
          WALEdit val = entry.getEdit();
          if (ng != null) { // some test, or nonces disabled
            ng.reportOperationFromWal(key.getNonceGroup(), key.getNonce(), key.getWriteTime());
          }
          if (reporter != null) {
            intervalEdits += val.size();
            if (intervalEdits >= interval) {
              // Number of edits interval reached
              intervalEdits = 0;
              long cur = EnvironmentEdgeManager.currentTime();
              if (lastReport + period <= cur) {
                status.setStatus(
                  "Replaying edits..." + " skipped=" + skippedEdits + " edits=" + editsCount);
                // Timeout reached
                if (!reporter.progress()) {
                  msg = "Progressable reporter failed, stopping replay";
                  LOG.warn(msg);
                  status.abort(msg);
                  throw new IOException(msg);
                }
                reported_once = true;
                lastReport = cur;
              }
            }
          }

          if (firstSeqIdInLog == -1) {
            firstSeqIdInLog = key.getLogSeqNum();
          }
          firstSeqIdInLog = firstSeqIdInLog == -1 ? key.getLogSeqNum() : firstSeqIdInLog;
          if (currentEditSeqId > key.getLogSeqNum()) {
            // when this condition is true, it means we have a serious defect because we need to
            // maintain increasing SeqId for WAL edits per region
            LOG.error(regionInfo.getEncodedName() + " : " + "Found decreasing SeqId. PreId="
              + currentEditSeqId + " key=" + key + "; edit=" + val);
          } else {
            currentEditSeqId = key.getLogSeqNum();
          }
          currentReplaySeqId =
            (key.getOrigLogSeqNum() > 0) ? key.getOrigLogSeqNum() : currentEditSeqId;
          // Start coprocessor replay here. The coprocessor is for each WALEdit
          // instead of a KeyValue.
          if (preWALRestore(regionInfo, key, val, status)) {
            // if bypass this wal entry, ignore it ...
            continue;
          }
          // Check this edit is for this region.
          boolean checkRowWithinBoundary =
            !Bytes.equals(key.getEncodedRegionName(), this.regionInfo.getEncodedNameAsBytes());
          boolean flush = false;
          for (Cell cell : val.getCells()) {
            // Check this edit is for me. Also, guard against writing the special
            // METACOLUMN info such as HBASE::CACHEFLUSH entries
            if (CellUtil.matchingFamily(cell, WALEdit.METAFAMILY)) {
              // if region names don't match, skipp replaying compaction marker
              if (!checkRowWithinBoundary) {
                //this is a special edit, we should handle it
                WALProtos.CompactionDescriptor compaction = WALEdit.getCompaction(cell);
                if (compaction != null) {
                  //replay the compaction
                  hRegion.replayWALCompactionMarker(compaction, false, true, Long.MAX_VALUE);
                }
              }
              skippedEdits++;
              continue;
            }
            // Figure which store the edit is meant for.
            if (store == null || !CellUtil.matchingFamily(cell, store.getFamily().getName())) {
              store = hRegion.getStore(cell);
            }
            if (store == null) {
              // This should never happen.  Perhaps schema was changed between
              // crash and redeploy?
              LOG.warn("No family for " + cell);
              skippedEdits++;
              continue;
            }
            if (checkRowWithinBoundary && !rowIsInRange(regionInfo, cell.getRowArray(),
              cell.getRowOffset(), cell.getRowLength())) {
              LOG.warn("Row of " + cell + " is not within region boundary");
              skippedEdits++;
              continue;
            }
            // Now, figure if we should skip this edit.
            if (key.getLogSeqNum() <= maxSeqIdInStores.get(store.getFamily().getName())) {
              skippedEdits++;
              continue;
            }
            CellUtil.setSequenceId(cell, currentReplaySeqId);
            // Once we are over the limit, restoreEdit will keep returning true to
            // flush -- but don't flush until we've played all the kvs that make up
            // the WALEdit.
            flush |= hRegion.restoreEdit(store, cell);
            editsCount++;
          }
          if (flush) {
            hRegion.internalFlushcache(null, currentEditSeqId, hRegion.getStores(), status, false);
          }
          postWALRestore(regionInfo, key, val);
        }
        LOG.info("Replay flushing with edits count: " + editsCount);
        postReplayWALs(regionInfo, edits);
      } catch (EOFException eof) {
        Path p = WALSplitterUtil.moveAsideBadEditsLog(walNamespace, edits);
        msg = "Encountered EOF. Most likely due to Master failure during wal splitting, so we have "
          + "this data in another edit. Continuing, but renaming " + edits + " as " + p;
        LOG.warn(msg, eof);
        status.abort(msg);
      } catch (IOException ioe) {
        // If the IOE resulted from bad file format,
        // then this problem is idempotent and retrying won't help
        if (ioe.getCause() instanceof ParseException) {
          Path p = WALSplitterUtil.moveAsideBadEditsLog(walNamespace, edits);
          msg = "File corruption encountered!  " + "Continuing, but renaming " + edits + " as " + p;
          LOG.warn(msg, ioe);
          status.setStatus(msg);
        } else {
          status.abort(StringUtils.stringifyException(ioe));
          // other IO errors may be transient (bad network connection,
          // checksum exception on one datanode, etc).  throw & retry
          throw ioe;
        }
      }
      if (reporter != null && !reported_once) {
        reporter.progress();
      }
      msg = "Applied " + editsCount + ", skipped " + skippedEdits + ", firstSequenceIdInLog="
        + firstSeqIdInLog + ", maxSequenceIdInLog=" + currentEditSeqId + ", path=" + edits;
      status.markComplete(msg);
      LOG.debug(msg);
      return currentEditSeqId;
    } finally {
      status.cleanup();
    }
  }

  private void preReplayWALs(final HRegionInfo info, final Path edits) throws IOException {
    if (coprocessorHost != null) {
      coprocessorHost.preReplayWALs(info, edits);
    }
  }

  public void postWALRestore(final HRegionInfo info, final WALKey logKey, final WALEdit logEdit)
    throws IOException {
    if (coprocessorHost != null) {
      coprocessorHost.postWALRestore(info, logKey, logEdit);
    }
  }

  public void postReplayWALs(final HRegionInfo info, final Path edits) throws IOException {
    if (coprocessorHost != null) {
      coprocessorHost.postReplayWALs(info, edits);
    }
  }

  private boolean preWALRestore(final HRegionInfo info, final WALKey logKey,
    final WALEdit logEdit, MonitoredTask status) throws IOException {
    if (coprocessorHost != null) {
      status.setStatus("Running pre-WAL-restore hook in coprocessors");
      return coprocessorHost.preWALRestore(info, logKey, logEdit);
    }
    return false;
  }
}
