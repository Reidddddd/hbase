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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public abstract class AbstractLogRecoveredEditsOutputSink extends OutputSink {
  private static final Log LOG = LogFactory.getLog(AbstractLogRecoveredEditsOutputSink.class);

  protected final Configuration conf;
  protected final Map<String, Map<byte[], Long>> regionMaxSeqIdInStores;
  protected final AbstractWALSplitter walSplitter;

  public AbstractLogRecoveredEditsOutputSink(PipelineController controller,
    EntryBuffers entryBuffers, int numWriters, Configuration conf,
    Map<String, Map<byte[], Long>> regionMaxSeqIdInStores, AbstractWALSplitter walSplitter) {
    super(controller, entryBuffers, numWriters);
    this.conf = conf;
    this.regionMaxSeqIdInStores = regionMaxSeqIdInStores;
    this.walSplitter = walSplitter;
  }

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

  /**
   * Close all of the output streams.
   * @return the list of paths written.
   */
  List<Path> close() throws IOException {
    Preconditions.checkState(!closeAndCleanCompleted);

    final List<Path> paths = new ArrayList<Path>();
    final List<IOException> thrown = Lists.newArrayList();
    ThreadPoolExecutor closeThreadPool = Threads.getBoundedCachedThreadPool(numThreads, 30L,
      TimeUnit.SECONDS, new ThreadFactory() {
        private int count = 1;

        @Override
        public Thread newThread(Runnable r) {
          Thread t = new Thread(r, "split-log-closeStream-" + count++);
          return t;
        }
      });
    CompletionService<Void> completionService =
      new ExecutorCompletionService<Void>(closeThreadPool);
    boolean progress_failed;
    try{
      progress_failed = executeCloseTask(completionService, thrown, paths);
    } catch (InterruptedException e) {
      IOException iie = new InterruptedIOException();
      iie.initCause(e);
      throw iie;
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    } finally {
      closeThreadPool.shutdownNow();
    }

    if (!thrown.isEmpty()) {
      throw MultipleIOException.createIOException(thrown);
    }
    writersClosed = true;
    closeAndCleanCompleted = true;
    if (progress_failed) {
      return null;
    }
    return paths;
  }

  boolean executeCloseTask(CompletionService<Void> completionService,
    final List<IOException> thrown, final List<Path> paths)
    throws InterruptedException, ExecutionException {
    for (final Map.Entry<String, SinkWriter> writersEntry : writers.entrySet()) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Submitting close of " + ((WriterAndPath) writersEntry.getValue()).p);
      }
      completionService.submit(new Callable<Void>() {
        @Override public Void call() throws Exception {
          WriterAndPath wap = (WriterAndPath) writersEntry.getValue();
          Path dst = closeWriter(writersEntry.getKey(), wap, thrown);
          paths.add(dst);
          return null;
        }
      });
    }
    boolean progress_failed = false;
    for (int i = 0, n = this.writers.size(); i < n; i++) {
      Future<Void> future = completionService.take();
      future.get();
      if (!progress_failed && reporter != null && !reporter.progress()) {
        progress_failed = true;
      }
    }
    return progress_failed;
  }

  abstract Path closeWriter(String encodedRegionName, WriterAndPath wap, List<IOException> thrown)
    throws IOException;

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

  /**
   * Get a writer and path for a log starting at the given entry. This function is threadsafe so
   * long as multiple threads are always acting on different regions.
   * @return null if this region shouldn't output any logs
   */
  WriterAndPath getWriterAndPath(Entry entry, boolean reusable) throws IOException {
    byte[] region = entry.getKey().getEncodedRegionName();
    String regionName = Bytes.toString(region);
    WriterAndPath ret = (WriterAndPath) writers.get(regionName);
    if (ret != null) {
      return ret;
    }
    // If we already decided that this region doesn't get any output
    // we don't need to check again.
    if (blacklistedRegions.contains(region)) {
      return null;
    }
    ret = createWAP(region, entry);
    if (ret == null) {
      blacklistedRegions.add(region);
      return null;
    }

    if(reusable) {
      writers.put(regionName, ret);
    }
    return ret;
  }

  abstract WriterAndPath createWAP(byte[] region, Entry entry) throws IOException;

  void filterCellByStore(Entry logEntry) {
    Map<byte[], Long> maxSeqIdInStores =
      regionMaxSeqIdInStores.get(Bytes.toString(logEntry.getKey().getEncodedRegionName()));
    if (maxSeqIdInStores == null || maxSeqIdInStores.isEmpty()) {
      return;
    }
    // Create the array list for the cells that aren't filtered.
    // We make the assumption that most cells will be kept.
    ArrayList<Cell> keptCells = new ArrayList<Cell>(logEntry.getEdit().getCells().size());
    for (Cell cell : logEntry.getEdit().getCells()) {
      if (CellUtil.matchingFamily(cell, WALEdit.METAFAMILY)) {
        keptCells.add(cell);
      } else {
        byte[] family = CellUtil.cloneFamily(cell);
        Long maxSeqId = maxSeqIdInStores.get(family);
        // Do not skip cell even if maxSeqId is null. Maybe we are in a rolling upgrade,
        // or the master was crashed before and we can not get the information.
        if (maxSeqId == null || maxSeqId.longValue() < logEntry.getKey().getLogSeqNum()) {
          keptCells.add(cell);
        }
      }
    }

    // Anything in the keptCells array list is still live.
    // So rather than removing the cells from the array list
    // which would be an O(n^2) operation, we just replace the list
    logEntry.getEdit().setCells(keptCells);
  }

  /**
   * @return a map from encoded region ID to the number of edits written out for that region.
   */
  @Override
  public Map<byte[], Long> getOutputCounts() {
    TreeMap<byte[], Long> ret = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
    synchronized (writers) {
      for (Map.Entry<String, SinkWriter> entry : writers.entrySet()) {
        ret.put(Bytes.toBytes(entry.getKey()), entry.getValue().editsWritten);
      }
    }
    return ret;
  }

  @Override
  public int getNumberOfRecoveredRegions() {
    return writers.size();
  }

  @Override
  public void append(RegionEntryBuffer buffer) throws IOException {
    appendBuffer(buffer, true);
  }

  WriterAndPath appendBuffer(RegionEntryBuffer buffer, boolean reusable) throws IOException {
    List<Entry> entries = buffer.entryBuffer;
    if (entries.isEmpty()) {
      LOG.warn("got an empty buffer, skipping");
      return null;
    }
    WriterAndPath wap = null;

    long startTime = System.nanoTime();
    try {
      int editsCount = 0;

      for (Entry logEntry : entries) {
        if (wap == null) {
          wap = getWriterAndPath(logEntry, reusable);
          if (wap == null) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("getWriterAndPath decided we don't need to write edits for " + logEntry);
            }
            return null;
          }
        }
        filterCellByStore(logEntry);
        if (!logEntry.getEdit().isEmpty()) {
          wap.w.append(logEntry);
          this.updateRegionMaximumEditLogSeqNum(logEntry);
          editsCount++;
        } else {
          wap.incrementSkippedEdits(1);
        }
      }
      // Pass along summary statistics
      wap.incrementEdits(editsCount);
      wap.incrementNanoTime(System.nanoTime() - startTime);
    } catch (IOException e) {
      e = RemoteExceptionHandler.checkIOException(e);
      LOG.fatal(" Got while writing log entry to log", e);
      throw e;
    }
    return wap;
  }

  @Override
  public boolean keepRegionEvent(Entry entry) {
    ArrayList<Cell> cells = entry.getEdit().getCells();
    for (int i = 0; i < cells.size(); i++) {
      if (WALEdit.isCompactionMarker(cells.get(i))) {
        return true;
      }
    }
    return false;
  }
}
