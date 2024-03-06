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

import java.io.EOFException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Class that manages the output streams from the log splitting process.
 */
@InterfaceAudience.Private
public class LogRecoveredEditsOutputSink extends AbstractLogRecoveredEditsOutputSink {
  private static final Log LOG = LogFactory.getLog(LogRecoveredEditsOutputSink.class);

  private final FileSystem walFS;

  private String fileNameBeingSplit;

  public LogRecoveredEditsOutputSink(PipelineController controller, EntryBuffers entryBuffers,
      int numWriters, FileSystem walFS, Configuration conf, AbstractWALSplitter walSplitter,
      Map<String, Map<byte[], Long>> regionMaxSeqIdInStores) {
    // More threads could potentially write faster at the expense
    // of causing more disk seeks as the logs are split.
    // 3. After a certain setting (probably around 3) the
    // process will be bound on the reader in the current
    // implementation anyway.
    super(controller, entryBuffers, numWriters, conf, regionMaxSeqIdInStores, walSplitter);
    this.walFS = walFS;
  }

  /**
   * @return null if failed to report progress
   */
  @Override
  public List<Path> finishWritingAndClose() throws IOException {
    boolean isSuccessful = false;
    List<Path> result = null;
    try {
      isSuccessful = finishWriting(false);
    } finally {
      result = close();
      List<IOException> thrown = closeLogWriters(null);
      if (thrown != null && !thrown.isEmpty()) {
        throw MultipleIOException.createIOException(thrown);
      }
    }
    if (isSuccessful) {
      splits = result;
    }
    return splits;
  }

  // delete the one with fewer wal entries
  void deleteOneWithFewerEntries(WriterAndPath wap, Path dst)
    throws IOException {
    long dstMinLogSeqNum = -1L;
    try (Reader reader = WALUtils.createReader(walFS, dst, conf)) {
      Entry entry = reader.next();
      if (entry != null) {
        dstMinLogSeqNum = entry.getKey().getLogSeqNum();
      }
    } catch (EOFException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
          "Got EOF when reading first WAL entry from " + dst + ", an empty or broken WAL file?",
          e);
      }
    }
    if (wap.minLogSeqNum < dstMinLogSeqNum) {
      LOG.warn("Found existing old edits file. It could be the result of a previous failed"
        + " split attempt or we have duplicated wal entries. Deleting " + dst + ", length="
        + walFS.getFileStatus(dst).getLen());
      if (!walFS.delete(dst, false)) {
        LOG.warn("Failed deleting of old " + dst);
        throw new IOException("Failed deleting of old " + dst);
      }
    } else {
      LOG.warn("Found existing old edits file and we have less entries. Deleting " + wap.p
        + ", length=" + walFS.getFileStatus(wap.p).getLen());
      if (!walFS.delete(wap.p, false)) {
        LOG.warn("Failed deleting of " + wap.p);
        throw new IOException("Failed deleting of " + wap.p);
      }
    }
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
    if (wap.editsWritten == 0) {
      // just remove the empty recovered.edits file
      if (walFS.exists(wap.p) && !walFS.delete(wap.p, false)) {
        LOG.warn("Failed deleting empty " + wap.p);
        throw new IOException("Failed deleting empty  " + wap.p);
      }
      return null;
    }

    Path dst = WALSplitterUtil.getCompletedRecoveredEditsFilePath(wap.p,
      regionMaximumEditLogSeqNum.get(encodedRegionName));
    try {
      if (!dst.equals(wap.p) && walFS.exists(dst)) {
        deleteOneWithFewerEntries(wap, dst);
      }
      // Skip the unit tests which create a splitter that reads and
      // writes the data without touching disk.
      // TestHLogSplit#testThreading is an example.
      if (walFS.exists(wap.p)) {
        if (!walFS.rename(wap.p, dst)) {
          throw new IOException("Failed renaming " + wap.p + " to " + dst);
        }
        LOG.info("Rename " + wap.p + " to " + dst);
      }
    } catch (IOException ioe) {
      LOG.error("Couldn't rename " + wap.p + " to " + dst, ioe);
      thrown.add(ioe);
      return null;
    }
    return dst;
  }

  /**
   * @return a path with a write for that path. caller should close.
   */
  @Override
  WriterAndPath createWAP(byte[] region, Entry entry) throws IOException {
    String tmpDirName = conf.get(HConstants.TEMPORARY_FS_DIRECTORY_KEY,
      HConstants.DEFAULT_TEMPORARY_HDFS_DIRECTORY);
    Path regionEdits = WALSplitterUtil.getRegionSplitEditsPath(entry, fileNameBeingSplit,
      tmpDirName, conf);
    if (walFS.exists(regionEdits)) {
      LOG.warn("Found old edits file. It could be the "
        + "result of a previous failed split attempt. Deleting " + regionEdits + ", length="
        + walFS.getFileStatus(regionEdits).getLen());
      if (!walFS.delete(regionEdits, false)) {
        LOG.warn("Failed delete of old " + regionEdits);
      }
    }
    Writer w = this.walSplitter.createWriter(regionEdits);
    LOG.debug("Creating writer path=" + regionEdits);
    return new WriterAndPath(regionEdits, w, entry.getKey().getLogSeqNum());
  }

  public void setFileNameBeingSplit(String fileNameBeingSplit) {
    this.fileNameBeingSplit = fileNameBeingSplit;
  }
}
