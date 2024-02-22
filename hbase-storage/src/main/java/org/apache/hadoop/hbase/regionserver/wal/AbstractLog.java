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
package org.apache.hadoop.hbase.regionserver.wal;

import com.google.common.annotations.VisibleForTesting;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.management.MemoryUsage;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.io.util.HeapMemorySizeUtil;
import org.apache.hadoop.hbase.mvcc.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.Writer;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public abstract class AbstractLog extends WALBase {
  private static final Log LOG = LogFactory.getLog(AbstractLog.class);

  /**
   * The nexus at which all incoming handlers meet.  Does appends and sync with an ordering.
   * Appends and syncs are each put on the ring which means handlers need to
   * smash up against the ring twice (can we make it once only? ... maybe not since time to append
   * is so different from time to sync and sometimes we don't want to sync or we want to async
   * the sync).  The ring is where we make sure of our ordering and it is also where we do
   * batching up of handler sync calls.
   */
  protected Disruptor<RingBufferTruck> disruptor;

  /**
   * An executorservice that runs the disruptor AppendEventHandler append executor.
   */
  protected ExecutorService appendExecutor;

  /**
   * This fellow is run by the above appendExecutor service but it is all about batching up appends
   * and syncs; it may shutdown without cleaning out the last few appends or syncs.  To guard
   * against this, keep a reference to this handler and do explicit close on way out to make sure
   * all flushed out before we exit.
   */
  protected RingBufferEventHandler ringBufferEventHandler;

  protected SyncFutureCache syncFutureCache;

  // If live datanode count is lower than the default replicas value,
  // RollWriter will be triggered in each sync(So the RollWriter will be
  // triggered one by one in a short time). Using it as a workaround to slow
  // down the roll frequency triggered by checkLowReplication().
  protected final AtomicInteger consecutiveLogRolls = new AtomicInteger(0);

  // If consecutiveLogRolls is larger than lowReplicationRollLimit,
  // then disable the rolling in checkLowReplication().
  // Enable it if the replications recover.
  protected volatile boolean lowReplicationRollEnabled = true;

  public AbstractLog(Configuration conf, final List<WALActionsListener> listeners,
      final String prefix, final String suffix) throws IOException {
    super(conf, listeners, prefix, suffix);
  }

  private int calculateMaxLogFiles(float memstoreSizeRatio, long logRollSize) {
    long max = -1L;
    final MemoryUsage usage = HeapMemorySizeUtil.safeGetHeapMemoryUsage();
    if (usage != null) {
      max = usage.getMax();
    }
    return Math.round(max * memstoreSizeRatio * 2 / logRollSize);
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NP_NULL_ON_SOME_PATH_EXCEPTION",
    justification="Will never be null")
  @Override
  public long append(final HTableDescriptor htd, final HRegionInfo hri, final WALKey key,
    final WALEdit edits, final boolean inMemstore) throws IOException {
    if (this.closed) {
      throw new IOException("Cannot append; log is closed");
    }
    // Make a trace scope for the append.  It is closed on other side of the ring buffer by the
    // single consuming thread.  Don't have to worry about it.
    final MutableLong txidHolder = new MutableLong();
    final RingBuffer<RingBufferTruck> ringBuffer = disruptor.getRingBuffer();
    MultiVersionConcurrencyControl.WriteEntry we = key.getMvcc().begin(new Runnable() {
      @Override public void run() {
        txidHolder.setValue(ringBuffer.next());
      }
    });
    long txid = txidHolder.longValue();
    try {
      GenericWALEntry entry = new GenericWALEntry(txid, key, edits, htd, hri, inMemstore, we);
      ringBuffer.get(txid).loadPayload(entry);
    } finally {
      ringBuffer.publish(txid);
    }
    return txid;
  }

  /**
   * To init and start disruptor.
   * The purpose to extract to this independent function is to make the initialisation sequence
   * customisable in subclass.
   * Must guarantee this function is invoked and only once in the subclass construction.
   * Additionally, this init should be invoked after the first time rolling writer.
   */
  protected void initDisruptor() {
    // This is the 'writer' -- a single threaded executor.  This single thread 'consumes' what is
    // put on the ring buffer.
    String hostingThreadName = Thread.currentThread().getName();
    this.appendExecutor = Executors.
      newSingleThreadExecutor(Threads.getNamedThreadFactory(hostingThreadName + ".append"));
    // Preallocate objects to use on the ring buffer.  The way that appends and syncs work, we will
    // be stuck and make no progress if the buffer is filled with appends only and there is no
    // sync. If no sync, then the handlers will be outstanding just waiting on sync completion
    // before they return.
    final int preallocatedEventCount =
      this.conf.getInt("hbase.regionserver.wal.disruptor.event.count", 1024 * 16);
    // Using BlockingWaitStrategy.  Stuff that is going on here takes so long it makes no sense
    // spinning as other strategies do.
    this.disruptor =
      new Disruptor<RingBufferTruck>(RingBufferTruck.EVENT_FACTORY, preallocatedEventCount,
        this.appendExecutor, ProducerType.MULTI, new BlockingWaitStrategy());
    // Advance the ring buffer sequence so that it starts from 1 instead of 0,
    // because SyncFuture.NOT_DONE = 0.
    this.disruptor.getRingBuffer().next();
    int syncerCount = conf.getInt("hbase.regionserver.hlog.syncer.count", 5);
    int maxBatchCount = conf.getInt("hbase.regionserver.wal.sync.batch.count",
      conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT, 200));

    this.ringBufferEventHandler = new RingBufferEventHandler(syncerCount, maxBatchCount);
    this.syncFutureCache = new SyncFutureCache(conf);
    // Start disruptor at the last step.
    this.disruptor.handleExceptionsWith(new RingBufferExceptionHandler());
    this.disruptor.handleEventsWith(new RingBufferEventHandler [] {this.ringBufferEventHandler});
    // Starting up threads in constructor is a no no; Interface should have an init call.
    this.disruptor.start();
  }

  @Override
  public void sync() throws IOException {
    publishSyncThenBlockOnCompletion();
  }

  @Override
  public void sync(long txid) throws IOException {
    if (this.highestSyncedSequence.get() >= txid){
      // Already sync'd.
      return;
    }
    publishSyncThenBlockOnCompletion();
  }

  // Sync all known transactions
  protected void publishSyncThenBlockOnCompletion() throws IOException {
    blockOnSync(publishSyncOnRingBuffer());
  }

  protected void blockOnSync(final SyncFuture syncFuture) throws IOException {
    // Now we have published the ringbuffer, halt the current thread until we get an answer back.
    try {
      syncFuture.get(walSyncTimeout);
    } catch (TimeoutIOException tioe) {
      throw tioe;
    } catch (InterruptedException ie) {
      LOG.warn("Interrupted", ie);
      throw convertInterruptedExceptionToIOException(ie);
    } catch (ExecutionException e) {
      throw ensureIOException(e.getCause());
    }
  }

  @InterfaceAudience.Private
  public SyncFuture publishSyncOnRingBuffer() {
    long sequence = this.disruptor.getRingBuffer().next();
    return publishSyncOnRingBuffer(sequence);
  }

  private SyncFuture publishSyncOnRingBuffer(long sequence) {
    SyncFuture syncFuture = getSyncFuture(sequence);
    try {
      RingBufferTruck truck = this.disruptor.getRingBuffer().get(sequence);
      truck.loadPayload(syncFuture);
    } finally {
      this.disruptor.getRingBuffer().publish(sequence);
    }
    return syncFuture;
  }

  private SyncFuture getSyncFuture(final long sequence) {
    return syncFutureCache.getIfPresentOrNew().reset(sequence);
  }

  private IOException convertInterruptedExceptionToIOException(final InterruptedException ie) {
    Thread.currentThread().interrupt();
    IOException ioe = new InterruptedIOException();
    ioe.initCause(ie);
    return ioe;
  }

  private static IOException ensureIOException(final Throwable t) {
    return (t instanceof IOException)? (IOException)t: new IOException(t);
  }

  @Override
  public void shutdown() throws IOException {
    if (shutdown.compareAndSet(false, true)) {
      try {
        // Prevent all further flushing and rolling.
        closeBarrier.stopAndDrainOps();
      } catch (InterruptedException e) {
        LOG.error("Exception while waiting for cache flushes and log rolls", e);
        Thread.currentThread().interrupt();
      }

      // Shutdown the disruptor.  Will stop after all entries have been processed.  Make sure we
      // have stopped incoming appends before calling this else it will not shutdown.  We are
      // conservative below waiting a long time and if not elapsed, then halting.
      if (this.disruptor != null) {
        long timeoutms = conf.getLong("hbase.wal.disruptor.shutdown.timeout.ms", 60000);
        try {
          this.disruptor.shutdown(timeoutms, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
          LOG.warn("Timed out bringing down disruptor after " + timeoutms + "ms; forcing halt " +
            "(It is a problem if this is NOT an ABORT! -- DATALOSS!!!!)");
          this.disruptor.halt();
          this.disruptor.shutdown();
        }
      }
      // With disruptor down, this is safe to let go.
      if (this.appendExecutor !=  null) {
        this.appendExecutor.shutdown();
      }

      if (syncFutureCache != null) {
        syncFutureCache.clear();
      }

      // Tell our listeners that the log is closing
      if (!this.listeners.isEmpty()) {
        for (WALActionsListener i : this.listeners) {
          i.logCloseRequested();
        }
      }
      this.closed = true;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Closing WAL writer in " + getLogFullName());

      }
      if (this.writer != null) {
        this.writer.close();
        this.writer = null;
      }
    }
  }

  /**
   * Cleans up current writer closing it and then puts in place the passed in
   * <code>nextWriter</code>.
   *
   * In the case of creating a new WAL, oldPath will be null.
   *
   * In the case of rolling over from one file to the next, none of the params will be null.
   *
   * In the case of closing out this FSHLog with no further use newPath, nextWriter, and
   * nextHdfsOut will be null.
   *
   * @param oldPathStr may be null
   * @param newPathStr may be null
   * @param nextWriter may be null
   * @return the passed in <code>newPath</code>
   * @throws IOException if there is a problem flushing or closing the underlying FS
   */
  @Override
  protected String replaceWriter(final String oldPathStr, final String newPathStr,
    Writer nextWriter)
    throws IOException {
    // Ask the ring buffer writer to pause at a safe point.  Once we do this, the writer
    // thread will eventually pause. An error hereafter needs to release the writer thread
    // regardless -- hence the finally block below.  Note, this method is called from the FSHLog
    // constructor BEFORE the ring buffer is set running so it is null on first time through
    // here; allow for that.
    SyncFuture syncFuture = null;
    SafePointZigZagLatch zigzagLatch = null;
    long sequence = -1L;
    if (this.ringBufferEventHandler != null) {
      // Get sequence first to avoid dead lock when ring buffer is full
      // Considering below sequence
      // 1. replaceWriter is called and zigzagLatch is initialized
      // 2. ringBufferEventHandler#onEvent is called and arrives at #attainSafePoint(long) then wait
      // on safePointReleasedLatch
      // 3. Since ring buffer is full, if we get sequence when publish sync, the replaceWriter
      // thread will wait for the ring buffer to be consumed, but the only consumer is waiting
      // replaceWriter thread to release safePointReleasedLatch, which causes a deadlock
      sequence = getSequenceOnRingBuffer();
      zigzagLatch = this.ringBufferEventHandler.attainSafePoint();
    }
    afterCreatingZigZagLatch();
    CompletableFuture<Void> closeFuture = null;
    try {
      // Wait on the safe point to be achieved.  Send in a sync in case nothing has hit the
      // ring buffer between the above notification of writer that we want it to go to
      // 'safe point' and then here where we are waiting on it to attain safe point.  Use
      // 'sendSync' instead of 'sync' because we do not want this thread to block waiting on it
      // to come back.  Cleanup this syncFuture down below after we are ready to run again.
      try {
        if (zigzagLatch != null) {
          // use assert to make sure no change breaks the logic that
          // sequence and zigzagLatch will be set together
          assert sequence > 0L : "Failed to get sequence from ring buffer";
          syncFuture = zigzagLatch.waitSafePoint(publishSyncOnRingBuffer(sequence));
        }
      } catch (FailedSyncBeforeLogCloseException e) {
        // If unflushed/unsynced entries on close, it is reason to abort.
        if (isUnflushedEntries()) {
          throw e;
        }
        LOG.warn("Failed sync-before-close but no outstanding appends; closing WAL: " +
          e.getMessage());
      }

      // It is at the safe point.  Swap out writer from under the blocked writer thread.
      Writer localWriter = this.writer;
      this.writer = nextWriter;
      closeFuture = asyncCloseWriter(localWriter, oldPathStr, newPathStr, nextWriter);
    } catch (InterruptedException ie) {
      // Perpetuate the interrupt
      Thread.currentThread().interrupt();
    } catch (IOException e) {
      long count = getUnflushedEntriesCount();
      LOG.error("Failed close of WAL writer " + oldPathStr + ", unflushedEntries=" + count, e);
      throw new FailedLogCloseException(oldPathStr + ", unflushedEntries=" + count, e);
    } finally {
      try {
        // Let the writer thread go regardless, whether error or not.
        if (zigzagLatch != null) {
          zigzagLatch.releaseSafePoint();
          // syncFuture will be null if we failed our wait on safe point above. Otherwise, if
          // latch was obtained successfully, the sync we threw in either trigger the latch or it
          // got stamped with an exception because the WAL was damaged and we could not sync. Now
          // the write pipeline has been opened up again by releasing the safe point, process the
          // syncFuture we got above. This is probably a noop but it may be stale exception from
          // when old WAL was in place. Catch it if so.
          if (syncFuture != null) {
            try {
              blockOnSync(syncFuture);
            } catch (IOException ioe) {
              if (LOG.isTraceEnabled()) {
                LOG.trace("Stale sync exception", ioe);
              }
            }
          }
        }
      } finally {
        if (closeFuture != null) {
          try {
            closeFuture.join();
          } catch (CompletionException e) {
            if (e.getCause() instanceof IOException) {
              throw (IOException) e.getCause();
            }
            throw e;
          }
        }

      }
    }
    return newPathStr;
  }

  private long getSequenceOnRingBuffer() {
    return this.disruptor.getRingBuffer().next();
  }

  @VisibleForTesting
  protected boolean isLowReplicationRollEnabled() {
    return lowReplicationRollEnabled;
  }

  /**
   * Handler that is run by the disruptor ringbuffer consumer. Consumer is a SINGLE
   * 'writer/appender' thread.  Appends edits and starts up sync runs.  Tries its best to batch up
   * syncs.  There is no discernible benefit batching appends so we just append as they come in
   * because it simplifies the below implementation.  See metrics for batching effectiveness
   * (In measurement, at 100 concurrent handlers writing 1k, we are batching > 10 appends and 10
   * handler sync invocations for every actual dfsclient sync call; at 10 concurrent handlers,
   * YMMV).
   * <p>Herein, we have an array into which we store the sync futures as they come in.  When we
   * have a 'batch', we'll then pass what we have collected to a SyncRunner thread to do the
   * filesystem sync.  When it completes, it will then call
   * {@link SyncFuture#done(long, Throwable)} on each of SyncFutures in the batch to release
   * blocked Handler threads.
   * <p>I've tried various effects to try and make latencies low while keeping throughput high.
   * I've tried keeping a single Queue of SyncFutures in this class appending to its tail as the
   * syncs coming and having sync runner threads poll off the head to 'finish' completed
   * SyncFutures.  I've tried linkedlist, and various from concurrent utils whether
   * LinkedBlockingQueue or ArrayBlockingQueue, etc.  The more points of synchronization, the
   * more 'work' (according to 'perf stats') that has to be done; small increases in stall
   * percentages seem to have a big impact on throughput/latencies.  The below model where we have
   * an array into which we stash the syncs and then hand them off to the sync thread seemed like
   * a decent compromise.  See HBASE-8755 for more detail.
   */
  public class RingBufferEventHandler implements EventHandler<RingBufferTruck>, LifecycleAware {
    private final SyncRunner[] syncRunners;
    private final SyncFuture [] syncFutures;
    // Had 'interesting' issues when this was non-volatile.  On occasion, we'd not pass all
    // syncFutures to the next sync'ing thread.
    private AtomicInteger syncFuturesCount = new AtomicInteger();
    private volatile SafePointZigZagLatch zigzagLatch;
    /**
     * Set if we get an exception appending or syncing so that all subsequence appends and syncs
     * on this WAL fail until WAL is replaced.
     */
    private Exception exception = null;
    /**
     * Object to block on while waiting on safe point.
     */
    private final Object safePointWaiter = new Object();
    private volatile boolean shutdown = false;

    /**
     * Which syncrunner to use next.
     */
    private int syncRunnerIndex;

    RingBufferEventHandler(final int syncRunnerCount, final int maxBatchCount) {
      this.syncFutures = new SyncFuture[maxBatchCount];
      this.syncRunners = new SyncRunner[syncRunnerCount];
      for (int i = 0; i < syncRunnerCount; i++) {
        this.syncRunners[i] = new SyncRunner("sync." + i, maxBatchCount);
      }
    }

    private void cleanupOutstandingSyncsOnException(final long sequence, final Exception e) {
      // There could be handler-count syncFutures outstanding.
      for (int i = 0; i < this.syncFuturesCount.get(); i++) {
        this.syncFutures[i].done(sequence, e);
      }
      offerDoneSyncsBackToCache();
    }

    /**
     * @return True if outstanding sync futures still
     */
    private boolean isOutstandingSyncs() {
      // Look at SyncFutures in the EventHandler
      for (int i = 0; i < this.syncFuturesCount.get(); i++) {
        if (!this.syncFutures[i].isDone()) {
          return true;
        }
      }

      return false;
    }

    private boolean isOutstandingSyncsFromRunners() {
      // Look at SyncFutures in the SyncRunners
      for (SyncRunner syncRunner: syncRunners) {
        if(syncRunner.isAlive() && !syncRunner.areSyncFuturesReleased()) {
          return true;
        }
      }
      return false;
    }

    @Override
    // We can set endOfBatch in the below method if at end of our this.syncFutures array
    public void onEvent(final RingBufferTruck truck, final long sequence, boolean endOfBatch)
      throws Exception {
      // Appends and syncs are coming in order off the ringbuffer.  We depend on this fact.  We'll
      // add appends to dfsclient as they come in.  Batching appends doesn't give any significant
      // benefit on measurement.  Handler sync calls we will batch up. If we get an exception
      // appending an edit, we fail all subsequent appends and syncs with the same exception until
      // the WAL is reset. It is important that we not short-circuit and exit early this method.
      // It is important that we always go through the attainSafePoint on the end. Another thread,
      // the log roller may be waiting on a signal from us here and will just hang without it.

      try {
        if (truck.hasSyncFuturePayload()) {
          this.syncFutures[this.syncFuturesCount.getAndIncrement()] =
            truck.unloadSyncFuturePayload();
          // Force flush of syncs if we are carrying a full complement of syncFutures.
          if (this.syncFuturesCount.get() == this.syncFutures.length) {
            endOfBatch = true;
          }
        } else if (truck.hasFSWALEntryPayload()) {
          try {
            GenericWALEntry entry = truck.unloadFSWALEntryPayload();
            if (this.exception != null) {
              // Return to keep processing events coming off the ringbuffer
              return;
            }
            append(entry);
          } catch (Exception e) {
            // Failed append. Record the exception.
            this.exception = e;
            // invoking cleanupOutstandingSyncsOnException when append failed with exception,
            // it will cleanup existing sync requests recorded in syncFutures but not offered to
            // SyncRunner yet,so there won't be any sync future left over if no further
            // truck published to disruptor.
            cleanupOutstandingSyncsOnException(sequence,
              this.exception instanceof DamagedWALException ? this.exception
                : new DamagedWALException("On sync", this.exception));
            // Return to keep processing events coming off the ringbuffer
            return;
          }
        } else {
          // What is this if not an append or sync. Fail all up to this!!!
          cleanupOutstandingSyncsOnException(sequence,
            new IllegalStateException("Neither append nor sync"));
          // Return to keep processing.
          return;
        }

        // TODO: Check size and if big go ahead and call a sync if we have enough data.
        // This is a sync. If existing exception, fall through. Else look to see if batch.
        if (this.exception == null) {
          // If not a batch, return to consume more events from the ring buffer before proceeding;
          // we want to get up a batch of syncs and appends before we go do a filesystem sync.
          if (!endOfBatch || this.syncFuturesCount.get() <= 0) {
            return;
          }
          // syncRunnerIndex is bound to the range [0, Integer.MAX_INT - 1] as follows:
          //   * The maximum value possible for syncRunners.length is Integer.MAX_INT
          //   * syncRunnerIndex starts at 0 and is incremented only here
          //   * after the increment, the value is bounded by the '%' operator to
          //     [0, syncRunners.length),
          //     presuming the value was positive prior to the '%' operator.
          //   * after being bound to [0, Integer.MAX_INT - 1], the new value is stored in
          //     syncRunnerIndex ensuring that it can't grow without bound and overflow.
          //   * note that the value after the increment must be positive, because the most it
          //     could have been prior was Integer.MAX_INT - 1, and we only increment by 1.
          this.syncRunnerIndex = (this.syncRunnerIndex + 1) % this.syncRunners.length;
          try {
            // Below expects that the offer 'transfers' responsibility for the outstanding syncs to
            // the syncRunner. We should never get an exception in here.
            this.syncRunners[this.syncRunnerIndex].offer(sequence, this.syncFutures,
              this.syncFuturesCount.get());
          } catch (Exception e) {
            // Should NEVER get here.
            requestLogRoll();
            this.exception = new DamagedWALException("Failed offering sync", e);
          }
        }
        // We may have picked up an exception above trying to offer sync
        if (this.exception != null) {
          cleanupOutstandingSyncsOnException(sequence,
            this.exception instanceof DamagedWALException?
              this.exception:
              new DamagedWALException("On sync", this.exception));
        }
        attainSafePoint(sequence);
        // It is critical that we offer the futures back to the cache for reuse here after the
        // safe point is attained and all the clean up has been done. There have been
        // issues with reusing sync futures early causing WAL lockups, see HBASE-25984.
        offerDoneSyncsBackToCache();
      } catch (Throwable t) {
        LOG.error("UNEXPECTED!!! syncFutures.length=" + this.syncFutures.length, t);
      }
    }

    /**
     * Offers the finished syncs back to the cache for reuse.
     */
    private void offerDoneSyncsBackToCache() {
      for (int i = 0; i < this.syncFuturesCount.get(); i++) {
        syncFutureCache.offer(syncFutures[i]);
      }
      this.syncFuturesCount.set(0);
    }

    SafePointZigZagLatch attainSafePoint() {
      this.zigzagLatch = new SafePointZigZagLatch();
      return this.zigzagLatch;
    }

    /**
     * Check if we should attain safe point.  If so, go there and then wait till signalled before
     * we proceeding.
     */
    private void attainSafePoint(final long currentSequence) {
      if (this.zigzagLatch == null || !this.zigzagLatch.isCocked()) {
        return;
      }
      // If here, another thread is waiting on us to get to safe point.  Don't leave it hanging.
      beforeWaitOnSafePoint();
      try {
        // Wait on outstanding syncers; wait for them to finish syncing (unless we've been
        // shutdown or unless our latch has been thrown because we have been aborted or unless
        // this WAL is broken and we can't get a sync/append to complete).
        while ((!this.shutdown && this.zigzagLatch.isCocked()
          && highestSyncedSequence.get() < currentSequence &&
          // We could be in here and all syncs are failing or failed. Check for this. Otherwise
          // we'll just be stuck here for ever. In other words, ensure there syncs running.
          isOutstandingSyncs())
          // Wait for all SyncRunners to finish their work so that we can replace the writer
          || isOutstandingSyncsFromRunners()) {
          synchronized (this.safePointWaiter) {
            this.safePointWaiter.wait(0, 1);
          }
        }
        // Tell waiting thread we've attained safe point. Can clear this.throwable if set here
        // because we know that next event through the ringbuffer will be going to a new WAL
        // after we do the zigzaglatch dance.
        this.exception = null;
        this.zigzagLatch.safePointAttained();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted ", e);
        Thread.currentThread().interrupt();
      }
    }

    /**
     * Append to the WAL.  Does all CP and WAL listener calls.
     */
    void append(final GenericWALEntry entry) throws Exception {
      // TODO: WORK ON MAKING THIS APPEND FASTER. DOING WAY TOO MUCH WORK WITH CPs, PBing, etc.
      atHeadOfRingBufferEventHandlerAppend();

      long start = EnvironmentEdgeManager.currentTime();
      byte [] encodedRegionName = entry.getKey().getEncodedRegionName();
      long regionSequenceId = WALKey.NO_SEQUENCE_ID;
      try {

        regionSequenceId = entry.getKey().getSequenceId();
        // Edits are empty, there is nothing to append.  Maybe empty when we are looking for a
        // region sequence id only, a region edit/sequence id that is not associated with an actual
        // edit. It has to go through all the rigmarole to be sure we have the right ordering.
        if (entry.getEdit().isEmpty()) {
          return;
        }
        coprocessorPreWALWrite(entry.getHRegionInfo(), entry.getKey(), entry.getEdit());
        walListenerBeforeWrite(entry.getHTableDescriptor(), entry.getKey(), entry.getEdit());

        writer.append(entry);
        assert highestUnsyncedSequence < entry.getSequence();
        highestUnsyncedSequence = entry.getSequence();
        sequenceIdAccounting.update(encodedRegionName, entry.getFamilyNames(), regionSequenceId,
          entry.isInMemstore());
        coprocessorHost.postWALWrite(entry.getHRegionInfo(), entry.getKey(), entry.getEdit());
        // Update metrics.
        postAppend(entry, EnvironmentEdgeManager.currentTime() - start);
      } catch (Exception e) {
        String msg = "Append sequenceId=" + regionSequenceId + ", requesting roll of WAL";
        LOG.warn(msg, e);
        requestLogRoll();
        throw new DamagedWALException(msg, e);
      }
      numEntries.incrementAndGet();
    }

    @Override
    public void onStart() {
      for (SyncRunner syncRunner: this.syncRunners) {
        syncRunner.start();
      }
    }

    @Override
    public void onShutdown() {
      for (SyncRunner syncRunner: this.syncRunners) {
        syncRunner.interrupt();
      }
    }
  }

  // RingBuffer related methods
  /**
   * Used to manufacture race condition reliably. For testing only.
   * @see #beforeWaitOnSafePoint()
   */
  @VisibleForTesting
  protected void afterCreatingZigZagLatch() {}

  /**
   * @see #afterCreatingZigZagLatch()
   */
  @VisibleForTesting
  protected void beforeWaitOnSafePoint() {}

  /**
   * Exposed for testing only.  Use to tricks like halt the ring buffer appending.
   */
  @VisibleForTesting
  protected void atHeadOfRingBufferEventHandlerAppend() {
    // Noop
  }

  /**
   * Thread to runs the hdfs sync call. This call takes a while to complete.  This is the longest
   * pole adding edits to the WAL and this must complete to be sure all edits persisted.  We run
   * multiple threads sync'ng rather than one that just syncs in series so we have better
   * latencies; otherwise, an edit that arrived just after a sync started, might have to wait
   * almost the length of two sync invocations before it is marked done.
   * <p>When the sync completes, it marks all the passed in futures done.  On the other end of the
   * sync future is a blocked thread, usually a regionserver Handler.  There may be more than one
   * future passed in the case where a few threads arrive at about the same time and all invoke
   * 'sync'.  In this case we'll batch up the invocations and run one filesystem sync only for a
   * batch of Handler sync invocations.  Do not confuse these Handler SyncFutures with the futures
   * an ExecutorService returns when you call submit. We have no use for these in this model. These
   * SyncFutures are 'artificial', something to hold the Handler until the filesystem sync
   * completes.
   */
  private class SyncRunner extends HasThread {
    private volatile long sequence;
    // Keep around last exception thrown. Clear on successful sync.
    private final BlockingQueue<SyncFuture> syncFutures;
    private volatile SyncFuture takeSyncFuture = null;

    /**
     * UPDATE!
     * @param syncs the batch of calls to sync that arrived as this thread was starting; when done,
     *              we will put the result of the actual hdfs sync call as the result.
     * @param sequence The sequence number on the ring buffer when this thread was set running.
     *                 If this actual writer sync completes then all appends up this point have
     *                 been flushed/synced/pushed to datanodes.  If we fail, then the passed in
     *                 <code>syncs</code> futures will return the exception to their clients;
     *                 some of the edits may have made it out to data nodes but we will report all
     *                 that were part of this session as failed.
     */
    SyncRunner(final String name, final int maxHandlersCount) {
      super(name);
      // LinkedBlockingQueue because of
      // http://www.javacodegeeks.com/2010/09/java-best-practices-queue-battle-and.html
      // Could use other blockingqueues here or concurrent queues.
      //
      // We could let the capacity be 'open' but bound it so we get alerted in pathological case
      // where we cannot sync and we have a bunch of threads all backed up waiting on their syncs
      // to come in.  LinkedBlockingQueue actually shrinks when you remove elements so Q should
      // stay neat and tidy in usual case.  Let the max size be three times the maximum handlers.
      // The passed in maxHandlerCount is the user-level handlers which is what we put up most of
      // but HBase has other handlers running too -- opening region handlers which want to write
      // the meta table when succesful (i.e. sync), closing handlers -- etc.  These are usually
      // much fewer in number than the user-space handlers so Q-size should be user handlers plus
      // some space for these other handlers.  Lets multiply by 3 for good-measure.
      this.syncFutures = new LinkedBlockingQueue<SyncFuture>(maxHandlersCount * 3);
    }

    void offer(final long sequence, final SyncFuture [] syncFutures, final int syncFutureCount) {
      // Set sequence first because the add to the queue will wake the thread if sleeping.
      this.sequence = sequence;
      for (int i = 0; i < syncFutureCount; ++i) {
        this.syncFutures.add(syncFutures[i]);
      }
    }

    /**
     * Release the passed <code>syncFuture</code>
     * @return Returns 1.
     */
    private int releaseSyncFuture(final SyncFuture syncFuture, final long currentSequence,
      final Throwable t) {
      if (!syncFuture.done(currentSequence, t)) {
        throw new IllegalStateException();
      }
      // This function releases one sync future only.
      return 1;
    }

    /**
     * Release all SyncFutures whose sequence is <= <code>currentSequence</code>.
     * @param t May be non-null if we are processing SyncFutures because an exception was thrown.
     * @return Count of SyncFutures we let go.
     */
    private int releaseSyncFutures(final long currentSequence, final Throwable t) {
      int syncCount = 0;
      for (SyncFuture syncFuture; (syncFuture = this.syncFutures.peek()) != null;) {
        if (syncFuture.getRingBufferSequence() > currentSequence) {
          break;
        }
        releaseSyncFuture(syncFuture, currentSequence, t);
        if (!this.syncFutures.remove(syncFuture)) {
          throw new IllegalStateException(syncFuture.toString());
        }
        syncCount++;
      }
      return syncCount;
    }

    boolean areSyncFuturesReleased() {
      // check whether there is no sync futures offered, and no in-flight sync futures that is being
      // processed.
      return syncFutures.size() <= 0
        && takeSyncFuture == null;
    }

    @Override
    public void run() {
      long currentSequence;
      while (!isInterrupted()) {
        int syncCount = 0;

        try {
          // Make a local copy of takeSyncFuture after we get it.  We've been running into NPEs
          // 2020-03-22 16:54:32,180 WARN  [sync.1] wal.FSHLog$SyncRunner(589): UNEXPECTED
          // java.lang.NullPointerException
          // at org.apache.hadoop.hbase.regionserver.wal.filesystem.FSHLog$SyncRunner.run
          // (FSHLog.java:582)
          // at java.lang.Thread.run(Thread.java:748)
          SyncFuture sf;
          while (true) {
            takeSyncFuture = null;
            // We have to process what we 'take' from the queue
            takeSyncFuture = this.syncFutures.take();
            // Make local copy.
            sf = takeSyncFuture;
            currentSequence = this.sequence;
            long syncFutureSequence = sf.getRingBufferSequence();
            if (syncFutureSequence > currentSequence) {
              throw new IllegalStateException("currentSequence=" + currentSequence +
                ", syncFutureSequence=" + syncFutureSequence);
            }
            // See if we can process any syncfutures BEFORE we go sync.
            long currentHighestSyncedSequence = highestSyncedSequence.get();
            if (currentSequence < currentHighestSyncedSequence) {
              syncCount += releaseSyncFuture(sf, currentHighestSyncedSequence, null);
              // Done with the 'take'.  Go around again and do a new 'take'.
              continue;
            }
            break;
          }
          // I got something.  Lets run.  Save off current sequence number in case it changes
          // while we run.
          long start = System.nanoTime();
          Throwable lastException = null;
          try {
            long unSyncedFlushSeq = highestUnsyncedSequence;
            writer.sync();
            if (unSyncedFlushSeq > currentSequence) {
              currentSequence = unSyncedFlushSeq;
            }
            currentSequence = updateHighestSyncedSequence(currentSequence);
          } catch (IOException e) {
            LOG.error("Error syncing, request close of WAL", e);
            lastException = e;
          } catch (Exception e) {
            LOG.warn("UNEXPECTED", e);
            lastException = e;
          } finally {
            // First release what we 'took' from the queue.
            syncCount += releaseSyncFuture(sf, currentSequence, lastException);
            // Can we release other syncs?
            syncCount += releaseSyncFutures(currentSequence, lastException);
            if (lastException != null) {
              requestLogRoll();
            } else {
              checkLogRoll();
            }
          }
          postSync(System.nanoTime() - start, syncCount);
        } catch (InterruptedException e) {
          // Presume legit interrupt.
          Thread.currentThread().interrupt();
        } catch (Throwable t) {
          LOG.warn("UNEXPECTED, continuing", t);
        }
      }
    }
  }

  /**
   * Exception handler to pass the disruptor ringbuffer.  Same as native implementation only it
   * logs using our logger instead of java native logger.
   */
  static class RingBufferExceptionHandler implements ExceptionHandler {
    @Override
    public void handleEventException(Throwable ex, long sequence, Object event) {
      LOG.error("Sequence=" + sequence + ", event=" + event, ex);
      throw new RuntimeException(ex);
    }

    @Override
    public void handleOnStartException(Throwable ex) {
      LOG.error(ex);
      throw new RuntimeException(ex);
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
      LOG.error(ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * This class is used coordinating two threads holding one thread at a
   * 'safe point' while the orchestrating thread does some work that requires the first thread
   * paused: e.g. holding the WAL writer while its WAL is swapped out from under it by another
   * thread.
   *
   * <p>Thread A signals Thread B to hold when it gets to a 'safe point'.  Thread A wait until
   * Thread B gets there. When the 'safe point' has been attained, Thread B signals Thread A.
   * Thread B then holds at the 'safe point'.  Thread A on notification that Thread B is paused,
   * goes ahead and does the work it needs to do while Thread B is holding.  When Thread A is done,
   * it flags B and then Thread A and Thread B continue along on their merry way.  Pause and
   * signalling 'zigzags' between the two participating threads.  We use two latches -- one the
   * inverse of the other -- pausing and signaling when states are achieved.
   *
   * <p>To start up the drama, Thread A creates an instance of this class each time it would do
   * this zigzag dance and passes it to Thread B (these classes use Latches so it is one shot
   * only). Thread B notices the new instance (via reading a volatile reference or how ever) and it
   * starts to work toward the 'safe point'.  Thread A calls {@link #waitSafePoint()} when it
   * cannot proceed until the Thread B 'safe point' is attained. Thread A will be held inside in
   * {@link #waitSafePoint()} until Thread B reaches the 'safe point'.  Once there, Thread B
   * frees Thread A by calling {@link #safePointAttained()}.  Thread A now knows Thread B
   * is at the 'safe point' and that it is holding there (When Thread B calls
   * {@link #safePointAttained()} it blocks here until Thread A calls {@link #releaseSafePoint()}).
   * Thread A proceeds to do what it needs to do while Thread B is paused.  When finished,
   * it lets Thread B lose by calling {@link #releaseSafePoint()} and away go both Threads again.
   */
  static class SafePointZigZagLatch {
    /**
     * Count down this latch when safe point attained.
     */
    private volatile CountDownLatch safePointAttainedLatch = new CountDownLatch(1);
    /**
     * Latch to wait on.  Will be released when we can proceed.
     */
    private volatile CountDownLatch safePointReleasedLatch = new CountDownLatch(1);

    private void checkIfSyncFailed(SyncFuture syncFuture)
      throws FailedSyncBeforeLogCloseException {
      if (syncFuture.isThrowable()) {
        throw new FailedSyncBeforeLogCloseException(syncFuture.getThrowable());
      }
    }

    /**
     * For Thread A to call when it is ready to wait on the 'safe point' to be attained.
     * Thread A will be held in here until Thread B calls {@link #safePointAttained()}
     * @param syncFuture We need this as barometer on outstanding syncs.  If it comes home with
     *        an exception, then something is up w/ our syncing.
     * @return The passed <code>syncFuture</code>
     */
    SyncFuture waitSafePoint(SyncFuture syncFuture) throws InterruptedException,
      FailedSyncBeforeLogCloseException {
      while (!this.safePointAttainedLatch.await(1, TimeUnit.MILLISECONDS)) {
        checkIfSyncFailed(syncFuture);
      }
      checkIfSyncFailed(syncFuture);
      return syncFuture;
    }

    boolean isSafePointAttained() {
      return safePointAttainedLatch.getCount() == 0;
    }

    /**
     * Called by Thread B when it attains the 'safe point'.  In this method, Thread B signals
     * Thread A it can proceed. Thread B will be held in here until {@link #releaseSafePoint()}
     * is called by Thread A.
     */
    void safePointAttained() throws InterruptedException {
      this.safePointAttainedLatch.countDown();
      this.safePointReleasedLatch.await();
    }

    /**
     * Called by Thread A when it is done with the work it needs to do while Thread B is
     * halted.  This will release the Thread B held in a call to {@link #safePointAttained()}
     */
    void releaseSafePoint() {
      this.safePointReleasedLatch.countDown();
    }

    /**
     * @return True is this is a 'cocked', fresh instance, and not one that has already fired.
     */
    boolean isCocked() {
      return this.safePointAttainedLatch.getCount() > 0 &&
        this.safePointReleasedLatch.getCount() > 0;
    }
  }

  @VisibleForTesting
  Writer getWriter() {
    return this.writer;
  }

  @VisibleForTesting
  protected void setWriter(Writer writer) {
    this.writer = writer;
  }
}
