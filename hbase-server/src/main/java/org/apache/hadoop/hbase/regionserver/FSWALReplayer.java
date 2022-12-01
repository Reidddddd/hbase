/**
 *
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
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.io.EOFException;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.Entry;
import org.apache.hadoop.hbase.wal.Reader;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALSplitterUtil;
import org.apache.hadoop.hbase.wal.WALUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class FSWALReplayer extends WALReplayer {
  private static final Log LOG = LogFactory.getLog(FSWALReplayer.class);

  private final FileSystem walFS;
  private final FileSystem rootFS;
  private final RegionCoprocessorHost coprocessorHost;
  private final int interval;
  private final int period;

  FSWALReplayer(Configuration conf, FileSystem rootFS, HRegionInfo regionInfo,
    Map<byte[], Long> maxSeqIdInStores, HRegion hRegion) throws IOException {
    super(conf, regionInfo, rootFS, maxSeqIdInStores, hRegion);
    this.walFS = FSUtils.getWALFileSystem(conf);
    this.rootFS = rootFS;
    this.coprocessorHost = hRegion.getCoprocessorHost();
    // How many edits seen before we check elapsed time
    this.interval = this.conf.getInt("hbase.hstore.report.interval.edits", 2000);
    // How often to send a progress report (default 1/2 master timeout)
    this.period = this.conf.getInt("hbase.hstore.report.period", 300000);
  }

  /**
   * Read the edits put under this region by wal splitting process.  Put
   * the recovered edits back up into this region.
   *
   * <p>We can ignore any wal message that has a sequence ID that's equal to or
   * lower than minSeqId.  (Because we know such messages are already
   * reflected in the HFiles.)
   *
   * <p>While this is running we are putting pressure on memory yet we are
   * outside of our usual accounting because we are not yet an onlined region
   * (this stuff is being run as part of Region initialization).  This means
   * that if we're up against global memory limits, we'll not be flagged to flush
   * because we are not online. We can't be flushed by usual mechanisms anyways;
   * we're not yet online so our relative sequenceids are not yet aligned with
   * WAL sequenceids -- not till we come up online, post processing of split
   * edits.
   *
   * <p>But to help relieve memory pressure, at least manage our own heap size
   * flushing if are in excess of per-region limits.  Flushing, though, we have
   * to be careful and avoid using the regionserver/wal sequenceid.  Its running
   * on a different line to whats going on in here in this region context so if we
   * crashed replaying these edits, but in the midst had a flush that used the
   * regionserver wal with a sequenceid in excess of whats going on in here
   * in this region and with its split editlogs, then we could miss edits the
   * next time we go to recover. So, we have to flush inline, using seqids that
   * make sense in a this single region context only -- until we online.
   *
   * @param maxSeqIdInStores Any edit found in split editlogs needs to be in excess of
   *                         the maxSeqId for the store to be applied, else its skipped.
   * @return the sequence id of the last edit added to this region out of the recovered
   *         edits log or <code>minSeqId</code> if nothing added from editlogs.
   */
  @Override
  protected long replayRecoveredEditsIfAny(Map<byte[], Long> maxSeqIdInStores,
      final CancelableProgressable reporter, final MonitoredTask status)
    throws IOException {
    long minSeqIdForTheRegion = -1;
    for (Long maxSeqIdInStore : maxSeqIdInStores.values()) {
      if (maxSeqIdInStore < minSeqIdForTheRegion || minSeqIdForTheRegion == -1) {
        minSeqIdForTheRegion = maxSeqIdInStore;
      }
    }
    long seqId = minSeqIdForTheRegion;

    Path regionDir = FSUtils.getRegionDirFromRootDir(FSUtils.getRootDir(conf),
      hRegion.getRegionInfo());
    Path regionWALDir = getWALRegionDir();
    Path wrongRegionWALDir = FSUtils.getWrongWALRegionDir(conf, hRegion.getRegionInfo().getTable(),
      hRegion.getRegionInfo().getEncodedName());

    // We made a mistake in HBASE-20734 so we need to do this dirty hack...
    NavigableSet<Path> filesUnderWrongRegionWALDir =
      WALSplitterUtil.getSplitEditFilesSorted(walFS, wrongRegionWALDir);
    seqId = Math.max(seqId, replayRecoveredEditsForPaths(minSeqIdForTheRegion, walFS,
      filesUnderWrongRegionWALDir, reporter, regionDir));
    // This is to ensure backwards compatability with HBASE-20723 where recovered edits can appear
    // under the root dir even if walDir is set.
    NavigableSet<Path> filesUnderRootDir = Sets.newTreeSet();
    if (!regionWALDir.equals(regionDir)) {
      filesUnderRootDir = WALSplitterUtil.getSplitEditFilesSorted(rootFS, regionDir);
      seqId = Math.max(seqId, replayRecoveredEditsForPaths(minSeqIdForTheRegion, rootFS,
        filesUnderRootDir, reporter, regionDir));
    }
    NavigableSet<Path> files = WALSplitterUtil.getSplitEditFilesSorted(walFS, regionWALDir);
    seqId = Math.max(seqId, replayRecoveredEditsForPaths(minSeqIdForTheRegion, walFS,
      files, reporter, regionWALDir));
    if (seqId > minSeqIdForTheRegion) {
      // Then we added some edits to memory. Flush and cleanup split edit files.
      hRegion.internalFlushcache(null, seqId, hRegion.getStores(), status, false);

    }
    // Now delete the content of recovered edits. We're done w/ them.
    if (conf.getBoolean("hbase.region.archive.recovered.edits", false)) {
      // For debugging data loss issues!
      // If this flag is set, make use of the hfile archiving by making recovered.edits a fake
      // column family. Have to fake out file type too by casting our recovered.edits as storefiles
      String fakeFamilyName = WALSplitterUtil.getRegionDirRecoveredEditsDir(regionWALDir).getName();
      Set<StoreFile> fakeStoreFiles = new HashSet<>();
      for (Path file: Iterables.concat(files, filesUnderWrongRegionWALDir)) {
        fakeStoreFiles.add(new StoreFile(walFS, file, conf, null, null));
      }
      for (Path file: filesUnderRootDir) {
        fakeStoreFiles.add(new StoreFile(rootFS, file, conf, null, null));
      }
      getRegionWALFileSystem().removeStoreFiles(fakeFamilyName, fakeStoreFiles);
    } else {
      for (Path file : filesUnderRootDir) {
        if (!rootFS.delete(file, false)) {
          LOG.error("Failed delete of " + file + " from under the root directory");
        } else {
          LOG.debug("Deleted recovered.edits under root directory, file=" + file);
        }
      }
      for (Path file : Iterables.concat(files, filesUnderWrongRegionWALDir)) {
        if (!walFS.delete(file, false)) {
          LOG.error("Failed delete of " + file);
        } else {
          LOG.debug("Deleted recovered.edits file=" + file);
        }
      }
    }

    // We have replayed all the recovered edits. Let's delete the wrong directories introduced
    // in HBASE-20734, see HBASE-22617 for more details.
    if (walFS.exists(wrongRegionWALDir)) {
      if (!walFS.delete(wrongRegionWALDir, true)) {
        LOG.warn("Unable to delete " + wrongRegionWALDir);
      }
    }

    return seqId;
  }

  private long replayRecoveredEditsForPaths(long minSeqIdForTheRegion, FileSystem fs,
      final NavigableSet<Path> files, final CancelableProgressable reporter, final Path regionDir)
    throws IOException {
    long seqid = minSeqIdForTheRegion;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Found " + (files == null ? 0 : files.size()) +
        " recovered edits file(s) under " + regionDir);
    }

    if (files == null || files.isEmpty()) {
      return seqid;
    }

    for (Path edits : files) {
      if (edits == null || !fs.exists(edits)) {
        LOG.warn("Null or non-existent edits file: " + edits);
        continue;
      }
      if (isZeroLengthThenDelete(fs, edits)) {
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
        seqid = Math.max(seqid, replayRecoveredEdits(edits, maxSeqIdInStores, reporter, fs));
      } catch (IOException e) {
        boolean skipErrors = conf.getBoolean(HConstants.HREGION_EDITS_REPLAY_SKIP_ERRORS,
          conf.getBoolean("hbase.skip.errors",
            HConstants.DEFAULT_HREGION_EDITS_REPLAY_SKIP_ERRORS));
        if (conf.get("hbase.skip.errors") != null) {
          LOG.warn("The property 'hbase.skip.errors' has been deprecated. Please use " +
            HConstants.HREGION_EDITS_REPLAY_SKIP_ERRORS + " instead.");
        }
        if (skipErrors) {
          Path p = WALSplitterUtil.moveAsideBadEditsFile(fs, edits);
          LOG.error(HConstants.HREGION_EDITS_REPLAY_SKIP_ERRORS +
            "=true so continuing. Renamed " + edits + " as " + p, e);
        } else {
          throw e;
        }
      }
    }
    return seqid;
  }

  /**
   * @param edits File of recovered edits.
   * @param maxSeqIdInStores Maximum sequenceid found in each store.  Edits in wal
   *                         must be larger than this to be replayed for each store.
   * @param reporter CacelableProgressable reporter
   * @return the sequence id of the last edit added to this region out of the
   *         recovered edits log or <code>minSeqId</code> if nothing added from editlogs.
   */
  private long replayRecoveredEdits(final Path edits, Map<byte[], Long> maxSeqIdInStores,
      final CancelableProgressable reporter, FileSystem fs) throws IOException {
    String msg = "Replaying edits from " + edits;
    LOG.info(msg);
    MonitoredTask status = TaskMonitor.get().createStatus(msg);
    status.setStatus("Opening recovered edits");
    try (Reader reader = WALUtils.createReader(fs, edits, conf)) {
      long currentEditSeqId = -1, currentReplaySeqId = -1, firstSeqIdInLog = -1, skippedEdits = 0;
      long editsCount = 0, intervalEdits = 0;
      Entry entry;
      Store store = null;
      boolean reported_once = false;
      ServerNonceManager ng = this.hRegion.rsServices == null ? null :
        this.hRegion.rsServices.getNonceManager();
      try {
        long lastReport = EnvironmentEdgeManager.currentTime();
        preReplayWALs(regionInfo, edits);
        while ((entry = reader.next()) != null) {
          WALKey key = entry.getKey();
          WALEdit val = entry.getEdit();
          if (ng != null) {
            // some test, or nonces disabled
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
          if (preWALRestore(status, regionInfo, key, val)) {
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
        postReplayWALs(regionInfo, edits);
      } catch (EOFException eof) {
        Path p = WALSplitterUtil.moveAsideBadEditsFile(fs, edits);
        msg = "Encountered EOF. Most likely due to Master failure during wal splitting, so we have "
          + "this data in another edit. Continuing, but renaming " + edits + " as " + p;
        LOG.warn(msg, eof);
        status.abort(msg);
      } catch (IOException ioe) {
        // If the IOE resulted from bad file format,
        // then this problem is idempotent and retrying won't help
        if (ioe.getCause() instanceof ParseException) {
          Path p = WALSplitterUtil.moveAsideBadEditsFile(fs, edits);
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

  private void preReplayWALs(HRegionInfo regionInfo, Path edits) throws IOException {
    if (coprocessorHost != null) {
      coprocessorHost.preReplayWALs(regionInfo, edits);
    }
  }

  private boolean preWALRestore(MonitoredTask status, HRegionInfo regionInfo, WALKey key,
      WALEdit val) throws IOException {
    if (coprocessorHost != null) {
      status.setStatus("Running pre-WAL-restore hook in coprocessors");
      if (coprocessorHost.preWALRestore(regionInfo, key, val)) {
        // if bypass this wal entry, ignore it ...
        return true;
      }
    }
    return false;
  }

  private void postWALRestore(HRegionInfo regionInfo, WALKey key, WALEdit val) throws IOException {
    if (coprocessorHost != null) {
      coprocessorHost.postWALRestore(regionInfo, key, val);
    }
  }

  private void postReplayWALs(HRegionInfo regionInfo, Path edits) throws IOException {
    if (coprocessorHost != null) {
      coprocessorHost.postReplayWALs(regionInfo, edits);
    }
  }

  @Override
  long writeRegionSequenceIdFile(long newSeqId, long saftyBumper)
    throws IOException {
    if (walFS.exists(regionWALDir)) {
      return WALSplitterUtil.writeRegionSequenceIdFile(walFS, regionWALDir, newSeqId, saftyBumper);
    } else {
      return -1;
    }
  }

  @Override
  void initRegionWALDir() throws IOException {
    regionWALDir = FSUtils.getWALRegionDir(conf, regionInfo);
  }

  /** @return the WAL {@link HRegionFileSystem} used by this region */
  HRegionFileSystem getRegionWALFileSystem() throws IOException {
    return new HRegionFileSystem(conf, walFS,
      FSUtils.getWALTableDir(conf, regionInfo.getTable()), regionInfo);
  }

  /*
   * @param fs
   * @param p File to check.
   * @return True if file was zero-length (and if so, we'll delete it in here).
   * @throws IOException
   */
  private static boolean isZeroLengthThenDelete(final FileSystem fs, final Path p)
    throws IOException {
    FileStatus stat = fs.getFileStatus(p);
    if (stat.getLen() > 0) {
      return false;
    }
    LOG.warn("File " + p + " is zero-length, deleting.");
    fs.delete(p, false);
    return true;
  }
}
