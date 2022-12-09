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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.distributedlog.shaded.exceptions.DLException;
import org.apache.distributedlog.shaded.exceptions.LogNotFoundException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.wal.DistributedLogWALSplitter;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Subclass of {@link SplitLogWorker} implemented based on DistributedLog
 */
@InterfaceAudience.Private
public class DistributedLogSplitWorker extends SplitLogWorker {
  private static final Log LOG = LogFactory.getLog(DistributedLogSplitWorker.class);

  public DistributedLogSplitWorker(Server hserver, Configuration conf,
    RegionServerServices server, TaskExecutor splitTaskExecutor) {
    super(hserver, conf, server, splitTaskExecutor);
  }

  public DistributedLogSplitWorker(Server hserver, Configuration conf,
    RegionServerServices rsServices, LastSequenceId sequenceIdChecker, WALFactory factory) {
    super(hserver, conf, rsServices, new TaskExecutor() {
      @Override
      public Status exec(String logName, ZooKeeperProtos.SplitLogTask.RecoveryMode mode,
        CancelableProgressable p) {
        try {
          if (!DistributedLogWALSplitter.splitLog(logName, conf, p, sequenceIdChecker,
              rsServices.getCoordinatedStateManager(), mode, factory)) {
            return Status.PREEMPTED;
          }
        } catch (InterruptedIOException iioe) {
          LOG.warn("Log splitting of " + logName + " interrupted, resigning", iioe);
          return Status.RESIGNED;
        } catch (IOException e) {
          if (e instanceof LogNotFoundException) {
            // A wal file may not exist anymore. Nothing can be recovered so move on
            LOG.warn("WAL " + logName + " does not exist anymore \n", e);
            return Status.DONE;
          }
          Throwable cause = e.getCause();
          if (e instanceof RetriesExhaustedException && (cause instanceof NotServingRegionException
            || cause instanceof ConnectException || cause instanceof SocketTimeoutException)) {
            LOG.warn("Log replaying of " + logName + " can't connect to the target regionserver, "
              + "resigning \n", e);
            return Status.RESIGNED;
          } else if (cause instanceof InterruptedException) {
            LOG.warn("Log splitting of " + logName + " interrupted, resigning \n", e);
            return Status.RESIGNED;
          } else if (cause instanceof DLException) {
            LOG.warn("Failed access DistributedLog with exception: \n", e);
            return Status.RESIGNED;
          }
          LOG.warn("Log splitting of " + logName + " failed, returning error", e);
          return Status.ERR;
        }
        return Status.DONE;
      }
    });
  }
}
