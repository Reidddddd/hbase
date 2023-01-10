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
import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.distributedlog.shaded.api.DistributedLogManager;
import org.apache.distributedlog.shaded.api.namespace.Namespace;
import org.apache.distributedlog.shaded.exceptions.DLException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.wal.DistributedLogAccessor;
import org.apache.hadoop.hbase.regionserver.wal.DistributedLogWriter;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A {@link OutputSink}
 */
@InterfaceAudience.Private
public class DistributedLogRecoveredEditsOutputSink extends AbstractLogRecoveredEditsOutputSink {
  private static final Log LOG = LogFactory.getLog(DistributedLogRecoveredEditsOutputSink.class);

  private String logInSplitting;

  public DistributedLogRecoveredEditsOutputSink(PipelineController controller,
      EntryBuffers entryBuffers, int numWriters, Configuration conf,
      Map<String, Map<byte[], Long>> regionMaxSeqIdInStores, AbstractWALSplitter walSplitter) {
    super(controller, entryBuffers, numWriters, conf, regionMaxSeqIdInStores, walSplitter);
  }

  @Override
  Path closeWriter(String encodedRegionName, WriterAndPath wap, List<IOException> thrown)
    throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Closing " + wap.p);
    }

    try {
      wap.w.close();
    } catch (IOException ioe) {
      LOG.error("Couldn't close log at " + wap.p, ioe);
      thrown.add(ioe);
      return null;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Closed wap " + wap.p + " (wrote " + wap.editsWritten
        + " edits, skipped " + wap.editsSkipped + " edits in "
        + (wap.nanosSpent / 1000 / 1000) + "ms");
    }
    Namespace namespace;
    try {
      namespace = DistributedLogAccessor.getInstance(conf).getNamespace();
    } catch (Exception e) {
      throw new IOException("Failed accessing distributedlog with exception: \n", e);
    }
    if (wap.editsWritten == 0) {
      // just remove the empty recovered.edits file
      String logName = wap.p.toString();
      if (namespace.logExists(logName)) {
        try {
          namespace.deleteLog(logName);
        } catch (DLException e){
          LOG.warn("Failed deleting empty " + wap.p);
          throw new IOException("Failed deleting empty " + wap.p + " with exception: \n", e);
        }
      }
      return null;
    }

    Path dst = WALSplitterUtil.getCompletedRecoveredEditsFilePath(wap.p,
      regionMaximumEditLogSeqNum.get(encodedRegionName));
    String oldLogName = WALUtils.pathToDistributedLogName(wap.p);
    String newLogName = WALUtils.pathToDistributedLogName(dst);
    try {
      if (!dst.equals(wap.p) && namespace.logExists(newLogName)) {
        deleteOneWithFewerEntries(wap, dst);
      }
      // Skip the unit tests which create a splitter that reads and
      // writes the data without touching disk.
      // TestHLogSplit#testThreading is an example.
      if (namespace.logExists(oldLogName)) {
        WALUtils.checkEndOfStream(namespace, oldLogName);
        namespace.renameLog(oldLogName, newLogName).get();
        LOG.info("Rename " + wap.p + " to " + dst);
      }
    } catch (IOException | ExecutionException | InterruptedException e) {
      LOG.error("Couldn't rename " + wap.p + " to " + dst, e);
      if (e instanceof IOException) {
        thrown.add((IOException) e);
      } else {
        thrown.add(new IOException(e));
      }
      return null;
    }
    return dst;
  }

  void deleteOneWithFewerEntries(WriterAndPath wap, Path dst) throws IOException {
    long dstMinLogSeqNum = -1L;
    try (Reader reader = WALUtils.createReader(null, dst, conf)) {
      Entry entry = reader.next();
      if (entry != null) {
        dstMinLogSeqNum = entry.getKey().getLogSeqNum();
      }
    } catch (EOFException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got EOF when reading first WAL entry from " + dst +
            ", an empty or broken WAL file?", e);
      }
    }
    Namespace namespace;
    try {
      namespace = DistributedLogAccessor.getInstance(conf).getNamespace();
    } catch (Exception e) {
      throw new IOException("Failed to access distributedlog with exception: \n", e);
    }
    if (wap.minLogSeqNum < dstMinLogSeqNum) {
      String dstStr = WALUtils.pathToDistributedLogName(dst);
      DistributedLogManager dlm = namespace.openLog(dstStr);
      LOG.warn("Found existing old edits file. It could be the result of a previous failed"
        + " split attempt or we have duplicated wal entries. Deleting " + dst + ", length="
        + dlm.getLastTxId());
      try {
        namespace.deleteLog(dstStr);
      } catch (DLException e) {
        LOG.warn("Failed deleting of old " + dst);
        throw new IOException("Failed deleting of old " + dst + " with exception: \n", e);
      } finally {
        dlm.close();
      }
    } else {
      String pathStr = WALUtils.pathToDistributedLogName(wap.p);
      DistributedLogManager dlm = namespace.openLog(pathStr);
      LOG.warn("Found existing old edits file and we have less entries. Deleting " + wap.p
        + ", length=" + dlm.getLastTxId());
      try {
        namespace.deleteLog(wap.p.toString());
      } catch (DLException e) {
        LOG.warn("Failed deleting of " + wap.p);
        throw new IOException("Failed deleting of old " + wap.p + " with exception: \n", e);
      } finally {
        dlm.close();
      }
    }
  }

  List<IOException> closeLogWriters(List<IOException> thrown) throws IOException {
    if (writersClosed) {
      return thrown;
    }

    if (thrown == null) {
      thrown = Lists.newArrayList();
    }
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
      WriterAndPath wap = null;
      for (SinkWriter tmpWAP : writers.values()) {
        try {
          wap = (WriterAndPath) tmpWAP;
          wap.w.close();
        } catch (IOException ioe) {
          LOG.error("Couldn't close log at " + wap.p, ioe);
          thrown.add(ioe);
          continue;
        }
        LOG.info(
          "Closed log " + wap.p + " (wrote " + wap.editsWritten + " edits in " + (wap.nanosSpent
            / 1000 / 1000) + "ms)");
      }
      writersClosed = true;
    }

    return thrown;
  }

  @Override
  WriterAndPath createWAP(byte[] region, Entry entry) throws IOException {
    Namespace namespace;
    try {
      namespace = DistributedLogAccessor.getInstance(conf).getNamespace();
    } catch (Exception e) {
      throw new IOException("Failed access distributedlog with exception: \n", e);
    }
    Path regionEdits =
      WALSplitterUtil.getRegionSplitEditsPath4DistributedLog(entry, logInSplitting);
    String regionEditsStr = WALUtils.pathToDistributedLogName(regionEdits);

    if (namespace.logExists(regionEditsStr)) {
      DistributedLogManager dlm = namespace.openLog(regionEditsStr);
      LOG.warn("Found old edits log. It could be the "
        + "result of a previous failed split attempt. Deleting " + regionEditsStr + ", length="
        + dlm.getLastTxId());
      try {
        LOG.info("Delete existing log: " + regionEditsStr);
        WALUtils.checkEndOfStream(dlm);
        namespace.deleteLog(regionEditsStr);
      } catch (DLException e) {
        LOG.warn("Failed delete of old " + regionEditsStr);
      } finally {
        dlm.close();
      }
    }
    Writer w = this.walSplitter.createWriter(regionEdits);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating writer path=" + regionEditsStr);
    }
    LOG.info("Created writer for edits log: " + regionEdits);
    return new WriterAndPath(regionEdits, w, entry.getKey().getLogSeqNum());
  }

  public void setLogInSplitting(String logInSplitting) {
    this.logInSplitting = logInSplitting;
  }

  @Override
  WriterAndPath appendBuffer(RegionEntryBuffer buffer, boolean reusable) throws IOException {
    WriterAndPath wap = super.appendBuffer(buffer, reusable);
    if (wap.w instanceof DistributedLogWriter) {
      ((DistributedLogWriter) wap.w).forceWriter();
    }
    return wap;
  }
}
