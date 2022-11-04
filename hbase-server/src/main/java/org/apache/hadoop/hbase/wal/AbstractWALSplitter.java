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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coordination.BaseCoordinatedStateManager;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.regionserver.LastSequenceId;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALCellCodec;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public abstract class AbstractWALSplitter {
  private static final Log LOG = LogFactory.getLog(AbstractWALSplitter.class);

  /** By default we retry errors in splitting, rather than skipping. */
  public static final boolean SPLIT_SKIP_ERRORS_DEFAULT = false;
  public final static String SPLIT_WRITER_CREATION_BOUNDED = "hbase.split.writer.creation.bounded";

  protected final Configuration conf;
  protected final WALFactory walFactory;
  // For checking the latest flushed sequence id
  protected final LastSequenceId sequenceIdChecker;
  protected final boolean splitWriterCreationBounded;

  protected Set<TableName> disablingOrDisabledTables = new HashSet<TableName>();
  protected BaseCoordinatedStateManager csm;
  protected MonitoredTask status;

  // Major subcomponents of the split process.
  // These are separated into inner classes to make testing easier.
  PipelineController controller;
  OutputSink outputSink;
  EntryBuffers entryBuffers;

  protected boolean distributedLogReplay;

  // Map encodedRegionName -> lastFlushedSequenceId
  protected Map<String, Long> lastFlushedSequenceIds = new ConcurrentHashMap<String, Long>();

  // Map encodedRegionName -> maxSeqIdInStores
  protected Map<String, Map<byte[], Long>> regionMaxSeqIdInStores =
    new ConcurrentHashMap<String, Map<byte[], Long>>();

  // Failed region server that the wal file being split belongs to
  protected String failedServerName = "";

  // Number of writer threads
  protected final int numWriterThreads;

  protected AbstractWALSplitter(Configuration conf, WALFactory walFactory,
      LastSequenceId sequenceIdChecker, CoordinatedStateManager csm, RecoveryMode mode) {
    this.conf = HBaseConfiguration.create(conf);
    String codecClassName = conf
      .get(WALCellCodec.WAL_CELL_CODEC_CLASS_KEY, WALCellCodec.class.getName());
    this.conf.set(HConstants.RPC_CODEC_CONF_KEY, codecClassName);
    this.walFactory = walFactory;
    this.sequenceIdChecker = sequenceIdChecker;
    this.splitWriterCreationBounded = conf.getBoolean(SPLIT_WRITER_CREATION_BOUNDED, false);
    this.distributedLogReplay = (ZooKeeperProtos.SplitLogTask.RecoveryMode.LOG_REPLAY == mode);
    this.csm = (BaseCoordinatedStateManager)csm;
    this.controller = new PipelineController();
    this.numWriterThreads = this.conf.getInt("hbase.regionserver.hlog.splitlog.writer.threads", 3);
    entryBuffers = new EntryBuffers(controller,
      this.conf.getInt("hbase.regionserver.hlog.splitlog.buffersize",
        128*1024*1024), splitWriterCreationBounded);
  }

  /**
   * As the OutputSink init is dependent on the different basal storage, we need to invoke this
   * in each subclass's constructor.
   */
  protected abstract void initOutputSink();

  /**
   * Get current open writers
   */
  protected int getNumOpenWriters() {
    int result = 0;
    if (this.outputSink != null) {
      result += this.outputSink.getNumOpenWriters();
    }
    return result;
  }

  /**
   * Create a new {@link Writer} for writing log splits.
   * @return a new Writer instance, caller should close
   */
  protected Writer createWriter(Path logfile) throws IOException {
    return WALUtils.createRecoveredEditsWriter(null, logfile, conf);
  }

  /**
   * Create a new {@link Reader} for reading logs to split.
   * @return new Reader instance, caller should close
   */
  protected Reader getReader(Path curLogFile, CancelableProgressable reporter) throws IOException {
    return WALUtils.createReader(null, curLogFile, reporter, conf);
  }

  /**
   * This function is used to construct mutations from a WALEntry. It also
   * reconstructs WALKey &amp; WALEdit from the passed in WALEntry
   * @param logEntry pair of WALKey and WALEdit instance stores WALKey and WALEdit instances
   *          extracted from the passed in WALEntry.
   * @return list of Pair&lt;MutationType, Mutation&gt; to be replayed
   */
  public static List<MutationReplay> getMutationsFromWALEntry(AdminProtos.WALEntry entry,
      CellScanner cells, Pair<WALKey, WALEdit> logEntry, Durability durability)
    throws IOException {
    if (entry == null) {
      // return an empty array
      return new ArrayList<MutationReplay>();
    }

    long replaySeqId = (entry.getKey().hasOrigSequenceNumber()) ?
      entry.getKey().getOrigSequenceNumber() : entry.getKey().getLogSequenceNumber();
    int count = entry.getAssociatedCellCount();
    List<MutationReplay> mutations = new ArrayList<MutationReplay>();
    Cell previousCell = null;
    Mutation m = null;
    WALKey key = null;
    WALEdit val = null;
    if (logEntry != null) {
      val = new WALEdit();
    }

    for (int i = 0; i < count; i++) {
      // Throw index out of bounds if our cell count is off
      if (!cells.advance()) {
        throw new ArrayIndexOutOfBoundsException("Expected=" + count + ", index=" + i);
      }
      Cell cell = cells.current();
      if (val != null) {
        val.add(cell);
      }

      boolean isNewRowOrType =
        previousCell == null || previousCell.getTypeByte() != cell.getTypeByte()
          || !CellUtil.matchingRow(previousCell, cell);
      if (isNewRowOrType) {
        // Create new mutation
        if (CellUtil.isDelete(cell)) {
          m = new Delete(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
          // Deletes don't have nonces.
          mutations.add(new MutationReplay(ClientProtos.MutationProto.MutationType.DELETE, m,
            HConstants.NO_NONCE, HConstants.NO_NONCE));
        } else {
          m = new Put(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
          // Puts might come from increment or append, thus we need nonces.
          long nonceGroup = entry.getKey().hasNonceGroup()
            ? entry.getKey().getNonceGroup() : HConstants.NO_NONCE;
          long nonce = entry.getKey().hasNonce() ? entry.getKey().getNonce() : HConstants.NO_NONCE;
          mutations.add(new MutationReplay(ClientProtos.MutationProto.MutationType.PUT, m,
            nonceGroup, nonce));
        }
      }
      if (CellUtil.isDelete(cell)) {
        ((Delete) m).addDeleteMarker(cell);
      } else {
        ((Put) m).add(cell);
      }
      m.setDurability(durability);
      previousCell = cell;
    }

    // reconstruct WALKey
    if (logEntry != null) {
      org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALKey walKeyProto = entry.getKey();
      List<UUID> clusterIds = new ArrayList<UUID>(walKeyProto.getClusterIdsCount());
      for (HBaseProtos.UUID uuid : entry.getKey().getClusterIdsList()) {
        clusterIds.add(new UUID(uuid.getMostSigBits(), uuid.getLeastSigBits()));
      }
      // we use HLogKey here instead of WALKey directly to support legacy coprocessors.
      key = new HLogKey(walKeyProto.getEncodedRegionName().toByteArray(), TableName.valueOf(
        walKeyProto.getTableName().toByteArray()), replaySeqId, walKeyProto.getWriteTime(),
        clusterIds, walKeyProto.getNonceGroup(), walKeyProto.getNonce(), null);
      logEntry.setFirst(key);
      logEntry.setSecond(val);
    }

    return mutations;
  }
}
