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
package org.apache.hadoop.hbase.wal;

import com.google.common.collect.Lists;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.coordination.BaseCoordinatedStateManager;
import org.apache.hadoop.hbase.coordination.ZKSplitLogManagerCoordination;
import org.apache.hadoop.hbase.exceptions.RegionOpeningException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Class that manages to replay edits from WAL files directly to assigned fail over region servers
 */
@InterfaceAudience.Private
abstract class AbstractLogReplayOutputSink extends OutputSink {
  private static final Log LOG = LogFactory.getLog(AbstractLogReplayOutputSink.class);
  private static final double BUFFER_THRESHOLD = 0.35;
  private static final String KEY_DELIMITER = "#";

  private final long waitRegionOnlineTimeOut;
  private final Set<String> recoveredRegions = Collections.synchronizedSet(new HashSet<String>());
  private final Map<String, RegionServerWriter> rsWriters =
    new ConcurrentHashMap<String, RegionServerWriter>();
  // online encoded region name -> region location map
  private final Map<String, HRegionLocation> onlineRegions =
    new ConcurrentHashMap<String, HRegionLocation>();
  private final Map<TableName, HConnection> tableNameToHConnectionMap = Collections
    .synchronizedMap(new TreeMap<TableName, HConnection>());

  /**
   * Map key -> value layout
   * servername:table name -> Queue(Row)
   */
  private final Map<String, List<Pair<HRegionLocation, Entry>>> serverToBufferQueueMap =
    new ConcurrentHashMap<String, List<Pair<HRegionLocation, Entry>>>();
  private final Set<TableName> disablingOrDisabledTables;
  private final Map<String, Long> lastFlushedSequenceIds;
  private final List<Throwable> thrown = new ArrayList<Throwable>();
  // Min batch size when replay WAL edits
  private final int minBatchSize;
  // The following sink is used in distrubitedLogReplay mode for entries of regions in a disabling
  // table. It's a limitation of distributedLogReplay. Because log replay needs a region is
  // assigned and online before it can replay wal edits while regions of disabling/disabled table
  // won't be assigned by AM. We can retire this code after HBASE-8234.
  private final OutputSink legacyOutputSink;
  private final BaseCoordinatedStateManager csm;

  protected final Configuration conf;
  protected final Map<String, Map<byte[], Long>> regionMaxSeqIdInStores;

  private boolean hasEditsInDisablingOrDisabledTables = false;

  // Failed region server that the wal file being split belongs to
  protected String failedServerName;

  public AbstractLogReplayOutputSink(PipelineController controller, EntryBuffers entryBuffers,
      int numWriters, Configuration conf, Set<TableName> disablingOrDisabledTables,
      Map<String, Long> lastFlushedSequenceIds, BaseCoordinatedStateManager csm,
      Map<String, Map<byte[], Long>> regionMaxSeqIdInStores, String failedServerName) {
    super(controller, entryBuffers, numWriters);
    this.conf = conf;
    this.waitRegionOnlineTimeOut = conf.getInt(HConstants.HBASE_SPLITLOG_MANAGER_TIMEOUT,
      ZKSplitLogManagerCoordination.DEFAULT_TIMEOUT);
    // a larger minBatchSize may slow down recovery because replay writer has to wait for
    // enough edits before replaying them
    this.minBatchSize = conf.getInt("hbase.regionserver.wal.logreplay.batch.size", 64);
    this.legacyOutputSink = createLegacyOutputSink(controller, entryBuffers, numWriters);
    this.legacyOutputSink.setReporter(reporter);
    this.disablingOrDisabledTables = disablingOrDisabledTables;
    this.lastFlushedSequenceIds = lastFlushedSequenceIds;
    this.regionMaxSeqIdInStores = regionMaxSeqIdInStores;
    this.csm = csm;
    this.failedServerName = failedServerName;
  }

  protected abstract OutputSink createLegacyOutputSink(PipelineController controller,
    EntryBuffers entryBuffers, int numWriters);

  @Override
  public void append(RegionEntryBuffer buffer) throws IOException {
    List<Entry> entries = buffer.entryBuffer;
    if (entries.isEmpty()) {
      LOG.warn("got an empty buffer, skipping");
      return;
    }

    // check if current region in a disabling or disabled table
    if (disablingOrDisabledTables.contains(buffer.tableName)) {
      // need fall back to old way
      legacyOutputSink.append(buffer);
      hasEditsInDisablingOrDisabledTables = true;
      // store regions we have recovered so far
      addToRecoveredRegions(Bytes.toString(buffer.encodedRegionName));
      return;
    }

    // group entries by region servers
    groupEditsByServer(entries);

    // process workitems
    String maxLocKey = null;
    int maxSize = 0;
    List<Pair<HRegionLocation, Entry>> maxQueue = null;
    synchronized (this.serverToBufferQueueMap) {
      for (Map.Entry<String, List<Pair<HRegionLocation, Entry>>> entry:
        serverToBufferQueueMap.entrySet()) {
        List<Pair<HRegionLocation, Entry>> curQueue = entry.getValue();
        if (curQueue.size() > maxSize) {
          maxSize = curQueue.size();
          maxQueue = curQueue;
          maxLocKey = entry.getKey();
        }
      }
      if (maxSize < minBatchSize
        && entryBuffers.totalBuffered < BUFFER_THRESHOLD * entryBuffers.maxHeapUsage) {
        // buffer more to process
        return;
      } else if (maxSize > 0) {
        this.serverToBufferQueueMap.remove(maxLocKey);
      }
    }

    if (maxSize > 0) {
      processWorkItems(maxLocKey, maxQueue);
    }
  }

  private void addToRecoveredRegions(String encodedRegionName) {
    if (!recoveredRegions.contains(encodedRegionName)) {
      recoveredRegions.add(encodedRegionName);
    }
  }

  /**
   * Helper function to group WALEntries to individual region servers
   */
  private void groupEditsByServer(List<Entry> entries) throws IOException {
    Set<TableName> nonExistentTables = null;
    Long cachedLastFlushedSequenceId = -1L;
    for (Entry entry : entries) {
      WALEdit edit = entry.getEdit();
      TableName table = entry.getKey().getTablename();
      // clear scopes which isn't needed for recovery
      entry.getKey().setScopes(null);
      String encodeRegionNameStr = Bytes.toString(entry.getKey().getEncodedRegionName());
      // skip edits of non-existent tables
      if (nonExistentTables != null && nonExistentTables.contains(table)) {
        this.skippedEdits.incrementAndGet();
        continue;
      }

      Map<byte[], Long> maxStoreSequenceIds = null;
      boolean needSkip = false;
      HRegionLocation loc = null;
      String locKey = null;
      List<Cell> cells = edit.getCells();
      List<Cell> skippedCells = new ArrayList<Cell>();
      HConnection hconn = this.getConnectionByTableName(table);

      for (Cell cell : cells) {
        byte[] row = CellUtil.cloneRow(cell);
        byte[] family = CellUtil.cloneFamily(cell);
        boolean isCompactionEntry = false;
        if (CellUtil.matchingFamily(cell, WALEdit.METAFAMILY)) {
          WALProtos.CompactionDescriptor compaction = WALEdit.getCompaction(cell);
          if (compaction != null && compaction.hasRegionName()) {
            try {
              byte[][] regionName = HRegionInfo.parseRegionName(compaction.getRegionName()
                .toByteArray());
              row = regionName[1]; // startKey of the region
              family = compaction.getFamilyName().toByteArray();
              isCompactionEntry = true;
            } catch (Exception ex) {
              LOG.warn("Unexpected exception received, ignoring " + ex);
              skippedCells.add(cell);
              continue;
            }
          } else {
            skippedCells.add(cell);
            continue;
          }
        }

        try {
          loc =
            locateRegionAndRefreshLastFlushedSequenceId(hconn, table, row,
              encodeRegionNameStr);
          // skip replaying the compaction if the region is gone
          if (isCompactionEntry && !encodeRegionNameStr.equalsIgnoreCase(
            loc.getRegionInfo().getEncodedName())) {
            LOG.info("Not replaying a compaction marker for an older region: "
              + encodeRegionNameStr);
            needSkip = true;
          }
        } catch (TableNotFoundException ex) {
          // table has been deleted so skip edits of the table
          LOG.info("Table " + table + " doesn't exist. Skip log replay for region "
            + encodeRegionNameStr);
          lastFlushedSequenceIds.put(encodeRegionNameStr, Long.MAX_VALUE);
          if (nonExistentTables == null) {
            nonExistentTables = new TreeSet<TableName>();
          }
          nonExistentTables.add(table);
          this.skippedEdits.incrementAndGet();
          needSkip = true;
          break;
        }

        cachedLastFlushedSequenceId =
          lastFlushedSequenceIds.get(loc.getRegionInfo().getEncodedName());
        if (cachedLastFlushedSequenceId != null
          && cachedLastFlushedSequenceId >= entry.getKey().getLogSeqNum()) {
          // skip the whole WAL entry
          this.skippedEdits.incrementAndGet();
          needSkip = true;
          break;
        } else {
          if (maxStoreSequenceIds == null) {
            maxStoreSequenceIds = regionMaxSeqIdInStores.get(loc.getRegionInfo().getEncodedName());
          }
          if (maxStoreSequenceIds != null) {
            Long maxStoreSeqId = maxStoreSequenceIds.get(family);
            if (maxStoreSeqId == null || maxStoreSeqId >= entry.getKey().getLogSeqNum()) {
              // skip current kv if column family doesn't exist anymore or already flushed
              skippedCells.add(cell);
              continue;
            }
          }
        }
      }

      // skip the edit
      if (loc == null || needSkip) {
        continue;
      }

      if (!skippedCells.isEmpty()) {
        cells.removeAll(skippedCells);
      }

      synchronized (serverToBufferQueueMap) {
        locKey = loc.getHostnamePort() + KEY_DELIMITER + table;
        List<Pair<HRegionLocation, Entry>> queue = serverToBufferQueueMap.get(locKey);
        if (queue == null) {
          queue =
            Collections.synchronizedList(new ArrayList<Pair<HRegionLocation, Entry>>());
          serverToBufferQueueMap.put(locKey, queue);
        }
        queue.add(new Pair<HRegionLocation, Entry>(loc, entry));
      }
      // store regions we have recovered so far
      addToRecoveredRegions(loc.getRegionInfo().getEncodedName());
    }
  }

  /**
   * Locate destination region based on table name & row. This function also makes sure the
   * destination region is online for replay.
   */
  private HRegionLocation locateRegionAndRefreshLastFlushedSequenceId(HConnection hconn,
    TableName table, byte[] row, String originalEncodedRegionName) throws IOException {
    // fetch location from cache
    HRegionLocation loc = onlineRegions.get(originalEncodedRegionName);
    if(loc != null) {
      return loc;
    }
    // fetch location from hbase:meta directly without using cache to avoid hit old dead server
    loc = hconn.getRegionLocation(table, row, true);
    if (loc == null) {
      throw new IOException("Can't locate location for row:" + Bytes.toString(row)
        + " of table:" + table);
    }
    // check if current row moves to a different region due to region merge/split
    if (!originalEncodedRegionName.equalsIgnoreCase(loc.getRegionInfo().getEncodedName())) {
      // originalEncodedRegionName should have already flushed
      lastFlushedSequenceIds.put(originalEncodedRegionName, Long.MAX_VALUE);
      HRegionLocation tmpLoc = onlineRegions.get(loc.getRegionInfo().getEncodedName());
      if (tmpLoc != null) {
        return tmpLoc;
      }
    }

    long lastFlushedSequenceId = -1L;
    AtomicBoolean isRecovering = new AtomicBoolean(true);
    loc = waitUntilRegionOnline(loc, row, this.waitRegionOnlineTimeOut, isRecovering);
    if (!isRecovering.get()) {
      // region isn't in recovering at all because WAL file may contain a region that has
      // been moved to somewhere before hosting RS fails
      lastFlushedSequenceIds.put(loc.getRegionInfo().getEncodedName(), Long.MAX_VALUE);
      LOG.info("logReplay skip region: " + loc.getRegionInfo().getEncodedName()
        + " because it's not in recovering.");
    } else {
      Long cachedLastFlushedSequenceId =
        lastFlushedSequenceIds.get(loc.getRegionInfo().getEncodedName());

      // retrieve last flushed sequence Id from ZK. Because region postOpenDeployTasks will
      // update the value for the region
      ClusterStatusProtos.RegionStoreSequenceIds ids =
        csm.getSplitLogWorkerCoordination().getRegionFlushedSequenceId(failedServerName,
          loc.getRegionInfo().getEncodedName());
      if (ids != null) {
        lastFlushedSequenceId = ids.getLastFlushedSequenceId();
        Map<byte[], Long> storeIds = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
        List<ClusterStatusProtos.StoreSequenceId> maxSeqIdInStores = ids.getStoreSequenceIdList();
        for (ClusterStatusProtos.StoreSequenceId id : maxSeqIdInStores) {
          storeIds.put(id.getFamilyName().toByteArray(), id.getSequenceId());
        }
        regionMaxSeqIdInStores.put(loc.getRegionInfo().getEncodedName(), storeIds);
      }

      if (cachedLastFlushedSequenceId == null
        || lastFlushedSequenceId > cachedLastFlushedSequenceId) {
        lastFlushedSequenceIds.put(loc.getRegionInfo().getEncodedName(), lastFlushedSequenceId);
      }
    }

    onlineRegions.put(loc.getRegionInfo().getEncodedName(), loc);
    return loc;
  }

  private void processWorkItems(String key, List<Pair<HRegionLocation, Entry>> actions)
    throws IOException {
    RegionServerWriter rsw = null;

    long startTime = System.nanoTime();
    try {
      rsw = getRegionServerWriter(key);
      rsw.sink.replayEntries(actions);

      // Pass along summary statistics
      rsw.incrementEdits(actions.size());
      rsw.incrementNanoTime(System.nanoTime() - startTime);
    } catch (IOException e) {
      e = RemoteExceptionHandler.checkIOException(e);
      LOG.fatal(" Got while writing log entry to log", e);
      throw e;
    }
  }

  /**
   * Wait until region is online on the destination region server
   * @param timeout How long to wait
   * @param isRecovering Recovering state of the region interested on destination region server.
   * @return True when region is online on the destination region server
   */
  private HRegionLocation waitUntilRegionOnline(HRegionLocation loc, byte[] row,
    final long timeout, AtomicBoolean isRecovering)
    throws IOException {
    final long endTime = EnvironmentEdgeManager.currentTime() + timeout;
    final long pause = conf.getLong(HConstants.HBASE_CLIENT_PAUSE,
      HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    boolean reloadLocation = false;
    TableName tableName = loc.getRegionInfo().getTable();
    int tries = 0;
    Throwable cause = null;
    while (endTime > EnvironmentEdgeManager.currentTime()) {
      try {
        // Try and get regioninfo from the hosting server.
        HConnection hconn = getConnectionByTableName(tableName);
        if(reloadLocation) {
          loc = hconn.getRegionLocation(tableName, row, true);
        }
        AdminProtos.AdminService.BlockingInterface remoteSvr =
          hconn.getAdmin(loc.getServerName());
        HRegionInfo region = loc.getRegionInfo();
        try {
          AdminProtos.GetRegionInfoRequest request =
            RequestConverter.buildGetRegionInfoRequest(region.getRegionName());
          AdminProtos.GetRegionInfoResponse response = remoteSvr.getRegionInfo(null, request);
          if (HRegionInfo.convert(response.getRegionInfo()) != null) {
            isRecovering.set((response.hasIsRecovering()) ? response.getIsRecovering() : true);
            return loc;
          }
        } catch (ServiceException se) {
          throw ProtobufUtil.getRemoteException(se);
        }
      } catch (IOException e) {
        cause = e.getCause();
        if(!(cause instanceof RegionOpeningException)) {
          reloadLocation = true;
        }
      }
      long expectedSleep = ConnectionUtils.getPauseTime(pause, tries);
      try {
        Thread.sleep(expectedSleep);
      } catch (InterruptedException e) {
        throw new IOException("Interrupted when waiting region " +
          loc.getRegionInfo().getEncodedName() + " online.", e);
      }
      tries++;
    }

    throw new IOException("Timeout when waiting region " + loc.getRegionInfo().getEncodedName() +
      " online for " + timeout + " milliseconds.", cause);
  }

  @Override
  public boolean flush() throws IOException {
    String curLoc = null;
    int curSize = 0;
    List<Pair<HRegionLocation, Entry>> curQueue = null;
    synchronized (this.serverToBufferQueueMap) {
      for (Map.Entry<String, List<Pair<HRegionLocation, Entry>>> entry :
        serverToBufferQueueMap.entrySet()) {
        String locationKey = entry.getKey();
        curQueue = entry.getValue();
        if (!curQueue.isEmpty()) {
          curSize = curQueue.size();
          curLoc = locationKey;
          break;
        }
      }
      if (curSize > 0) {
        this.serverToBufferQueueMap.remove(curLoc);
      }
    }

    if (curSize > 0) {
      this.processWorkItems(curLoc, curQueue);
      // We should already have control of the monitor; ensure this is the case.
      synchronized(controller.dataAvailable) {
        controller.dataAvailable.notifyAll();
      }
      return true;
    }
    return false;
  }

  @Override
  public boolean keepRegionEvent(Entry entry) {
    return true;
  }

  void addWriterError(Throwable t) {
    thrown.add(t);
  }

  @Override
  public List<Path> finishWritingAndClose() throws IOException {
    try {
      if (!finishWriting(false)) {
        return null;
      }
      if (hasEditsInDisablingOrDisabledTables) {
        splits = legacyOutputSink.finishWritingAndClose();
      } else {
        splits = new ArrayList<Path>();
      }
      // returns an empty array in order to keep interface same as old way
      return splits;
    } finally {
      List<IOException> thrown = closeRegionServerWriters();
      if (thrown != null && !thrown.isEmpty()) {
        throw MultipleIOException.createIOException(thrown);
      }
    }
  }

  @Override
  int getNumOpenWriters() {
    return this.rsWriters.size() + this.legacyOutputSink.getNumOpenWriters();
  }

  private List<IOException> closeRegionServerWriters() throws IOException {
    List<IOException> result = null;
    if (!writersClosed) {
      result = Lists.newArrayList();
      try {
        for (WriterThread t : writerThreads) {
          while (t.isAlive()) {
            t.shouldStop = true;
            t.interrupt();
            try {
              t.join(10);
            } catch (InterruptedException e) {
              IOException iie = new InterruptedIOException();
              iie.initCause(e);
              throw iie;
            }
          }
        }
      } finally {
        synchronized (rsWriters) {
          for (Map.Entry<String, RegionServerWriter> entry : rsWriters.entrySet()) {
            String locationKey = entry.getKey();
            RegionServerWriter tmpW = entry.getValue();
            try {
              tmpW.close();
            } catch (IOException ioe) {
              LOG.error("Couldn't close writer for region server:" + locationKey, ioe);
              result.add(ioe);
            }
          }
        }

        // close connections
        synchronized (this.tableNameToHConnectionMap) {
          for (Map.Entry<TableName, HConnection> entry :
            tableNameToHConnectionMap.entrySet()) {
            TableName tableName = entry.getKey();
            HConnection hconn = entry.getValue();
            try {
              hconn.clearRegionCache();
              hconn.close();
            } catch (IOException ioe) {
              result.add(ioe);
            }
          }
        }
        writersClosed = true;
      }
    }
    return result;
  }

  @Override
  public Map<byte[], Long> getOutputCounts() {
    TreeMap<byte[], Long> ret = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
    synchronized (rsWriters) {
      for (Map.Entry<String, RegionServerWriter> entry : rsWriters.entrySet()) {
        ret.put(Bytes.toBytes(entry.getKey()), entry.getValue().editsWritten);
      }
    }
    return ret;
  }

  @Override
  public int getNumberOfRecoveredRegions() {
    return this.recoveredRegions.size();
  }

  /**
   * Get a writer and path for a log starting at the given entry. This function is threadsafe so
   * long as multiple threads are always acting on different regions.
   * @return null if this region shouldn't output any logs
   */
  private RegionServerWriter getRegionServerWriter(String loc) throws IOException {
    RegionServerWriter ret = rsWriters.get(loc);
    if (ret != null) {
      return ret;
    }

    TableName tableName = getTableFromLocationStr(loc);
    if(tableName == null){
      throw new IOException("Invalid location string:" + loc + " found. Replay aborted.");
    }

    HConnection hconn = getConnectionByTableName(tableName);
    synchronized (rsWriters) {
      ret = rsWriters.get(loc);
      if (ret == null) {
        ret = new RegionServerWriter(conf, tableName, hconn);
        rsWriters.put(loc, ret);
      }
    }
    return ret;
  }

  private HConnection getConnectionByTableName(final TableName tableName) throws IOException {
    HConnection hconn = this.tableNameToHConnectionMap.get(tableName);
    if (hconn == null) {
      synchronized (this.tableNameToHConnectionMap) {
        hconn = this.tableNameToHConnectionMap.get(tableName);
        if (hconn == null) {
          hconn = HConnectionManager.getConnection(conf);
          this.tableNameToHConnectionMap.put(tableName, hconn);
        }
      }
    }
    return hconn;
  }

  private TableName getTableFromLocationStr(String loc) {
    /**
     * location key is in format <server name:port>#<table name>
     */
    String[] splits = loc.split(KEY_DELIMITER);
    if (splits.length != 2) {
      return null;
    }
    return TableName.valueOf(splits[1]);
  }
}
