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

import static org.apache.hadoop.hbase.wal.WALUtils.WAL_FILE_NAME_DELIMITER;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.wal.AbstractLog;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class AbstractWALProvider implements WALProvider {
  private static final Log LOG = LogFactory.getLog(AbstractWALProvider.class);

  protected volatile AbstractLog log = null;
  protected WALFactory factory = null;
  protected Configuration conf = null;
  protected List<WALActionsListener> listeners = null;
  protected String providerId = null;
  protected AtomicBoolean initialized = new AtomicBoolean(false);
  // for default wal provider, logPrefix won't change
  protected String logPrefix = null;

  /**
   * we synchronized on walCreateLock to prevent wal recreation in different threads
   */
  private final Object walCreateLock = new Object();

  /**
   * @param factory factory that made us, identity used for FS layout. may not be null
   * @param conf may not be null
   * @param listeners may be null
   * @param providerId differentiate between providers from one facotry, used for FS layout. may be
   *                   null
   */
  @Override
  public void init(final WALFactory factory, final Configuration conf,
    final List<WALActionsListener> listeners, String providerId) throws IOException {
    if (!initialized.compareAndSet(false, true)) {
      throw new IllegalStateException("WALProvider.init should only be called once.");
    }
    this.factory = factory;
    this.conf = conf;
    this.listeners = listeners;
    this.providerId = providerId;
    // get log prefix
    StringBuilder sb = new StringBuilder().append(factory.factoryId);
    if (providerId != null) {
      if (providerId.startsWith(WAL_FILE_NAME_DELIMITER)) {
        sb.append(providerId);
      } else {
        sb.append(WAL_FILE_NAME_DELIMITER).append(providerId);
      }
    }
    logPrefix = sb.toString();
  }

  @Override
  public List<WAL> getWALs() throws IOException {
    if (log == null) {
      return Collections.emptyList();
    }
    List<WAL> wals = new ArrayList<WAL>();
    wals.add(log);
    return wals;
  }

  @Override
  public WAL getWAL(final byte[] identifier, byte[] namespace) throws IOException {
    if (log == null) {
      // only lock when need to create wal, and need to lock since
      // creating hlog on fs is time consuming
      synchronized (walCreateLock) {
        if (log == null) {
          log = createWAL();
        }
      }
    }
    return log;
  }

  protected abstract AbstractLog createWAL() throws IOException;

  @Override
  public void close() throws IOException {
    if (log != null) {
      log.close();
    }
  }

  @Override
  public void shutdown() throws IOException {
    if (log != null) {
      log.shutdown();
    }
  }

  /**
   * iff the given WALFactory is using the DefaultWALProvider for meta and/or non-meta,
   * count the number of files (rolled and active). if either of them aren't, count 0
   * for that provider.
   */
  @Override
  public long getNumLogFiles() {
    return log == null ? 0 : this.log.getNumLogFiles();
  }

  /**
   * iff the given WALFactory is using the DefaultWALProvider for meta and/or non-meta,
   * count the size of files (rolled and active). if either of them aren't, count 0
   * for that provider.
   */
  @Override
  public long getLogFileSize() {
    return log == null ? 0 : this.log.getLogFileSize();
  }
}
