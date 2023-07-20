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

package org.apache.hadoop.hbase.regionserver.wal;

import com.google.common.annotations.VisibleForTesting;
import dlshade.org.apache.distributedlog.AppendOnlyStreamWriter;
import dlshade.org.apache.distributedlog.api.DistributedLogManager;
import dlshade.org.apache.distributedlog.api.namespace.Namespace;
import dlshade.org.apache.distributedlog.exceptions.LogExistsException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.wal.Entry;
import org.apache.hadoop.hbase.wal.ServiceBasedWriter;
import org.apache.hadoop.hbase.wal.WALUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Writer for DistributedLog based WAL.
 * Not Support WAL compression.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class DistributedLogWriter extends AbstractProtobufLogWriter implements ServiceBasedWriter {
  private static final Log LOG = LogFactory.getLog(DistributedLogWriter.class);
  // LogRecord limit - meta overhead - (2 Long + 1 Integer) overhead
  // Comes from BookKeeper, org.apache.distributedlog.LogRecord#MAX_LOGRECORD_SIZE
  // The last 20 bytes is 2 Long and 1 Integer for LogRecord serialisation.
  // Refer to org.apache.distributedlog.LogRecord#getPersistentSize()
  private static final int LOG_RECORD_SIZE_LIMIT = 1024 * 1024 - 1024 * 8 - 20;

  private Namespace walNamespace;
  private DistributedLogManager distributedLogManager;
  private AppendOnlyStreamWriter appendOnlyStreamWriter;
  private String logName;

  @Override
  public void init(Configuration conf, String logName) throws URISyntaxException, IOException {
    try {
      init(conf, logName, DistributedLogAccessor.getInstance(conf).getNamespace());
    } catch (Exception e) {
      LOG.error("Failed to init writer for log: " + logName);
      throw new IOException(e);
    }
  }

  // We need to spy the namespace object in UTs.
  @VisibleForTesting
  public void init(Configuration conf, String logName, Namespace walNamespace) throws IOException {
    LOG.info("Init DistributedLog writer " + logName);
    this.logName = logName;
    this.walNamespace = walNamespace;
    super.init(conf);
    // Force the wal header.
    this.appendOnlyStreamWriter.force(false);
  }

  @Override
  public void sync() throws IOException {
    // Do nothing.
    try {
      appendOnlyStreamWriter.flush(false);
    } catch (Exception e) {
      throw new IOException("Failed sync with exception: ", e);
    }
  }

  @Override
  protected void initOutput() throws IOException {
    // Check if a log with same name is in splitting.
    if (walNamespace.logExists(WALUtils.getSplittingName(logName))) {
      throw new IOException("A log with name " + logName + " is under splitting, cannot create "
        + "a new log with the same name: " + logName);
    }
    // Check if a log with same name is archived.
    if (walNamespace.logExists(WALUtils.getArchivedLogName(logName))) {
      throw new IOException("A log with name " + logName + " is already archived, cannot create "
        + "a new log with name: " + logName);
    }

    if (walNamespace.logExists(logName)) {
      throw new LogExistsException("The log :" + logName + " already exists");
    }

    try {
      walNamespace.createLog(logName);
    } catch (IOException e) {
      LOG.error("Failed create log writer", e);
      throw e;
    }
    distributedLogManager = walNamespace.openLog(logName);
    if (distributedLogManager == null) {
      throw new IllegalStateException("Failed to access DistributedLog. ");
    }
    appendOnlyStreamWriter = distributedLogManager.getAppendOnlyStreamWriter();
    output = new ByteArrayOutputStream(LOG_RECORD_SIZE_LIMIT);
  }


  @Override
  public void append(Entry entry) throws IOException {
    ByteArrayOutputStream buffer = (ByteArrayOutputStream) this.output;
    // Clear the buffer first.
    // The data of WALHeader and codec should be already written.
    buffer.reset();

    super.append(entry);
    byte[] recordArray = buffer.toByteArray();
    if (recordArray.length > LOG_RECORD_SIZE_LIMIT) {
      int numOfRecord = recordArray.length / LOG_RECORD_SIZE_LIMIT;
      if (recordArray.length % LOG_RECORD_SIZE_LIMIT != 0) {
        numOfRecord += 1;
      }

      for (int i = 0; i < numOfRecord; i++) {
        // Here we must use the min here to prevent the padded 0.
        appendOnlyStreamWriter.write(Arrays.copyOfRange(recordArray, i * LOG_RECORD_SIZE_LIMIT,
          Math.min((i + 1) * LOG_RECORD_SIZE_LIMIT, recordArray.length)));
      }
    } else {
      appendOnlyStreamWriter.write(recordArray);
    }
    buffer.reset();
  }

  @VisibleForTesting
  public void forceWriter() throws IOException {
    this.appendOnlyStreamWriter.force(false);
  }

  @Override
  protected boolean checkRecoveredEdits() {
    return logName.contains(HConstants.RECOVERED_EDITS_DIR);
  }

  @Override
  protected String getURL() {
    return this.logName;
  }

  @Override
  public long getLength() throws IOException {
    return appendOnlyStreamWriter.position();
  }

  @Override
  public void close() throws IOException {
    super.close();
    try {
      if (appendOnlyStreamWriter != null) {
        forceWriter();
        appendOnlyStreamWriter.markEndOfStream();
      }
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      if (appendOnlyStreamWriter != null) {
        appendOnlyStreamWriter.close();
        appendOnlyStreamWriter = null;
      }
      if (distributedLogManager != null) {
        distributedLogManager.close();
        distributedLogManager = null;
      }
    }
  }

  @Override
  protected void initAfterHeader(boolean doCompress) throws IOException {
    super.initAfterHeader(doCompress);
    // Need to transmit the bytes of WALHeader.
    ByteArrayOutputStream buffer = (ByteArrayOutputStream) this.output;
    appendOnlyStreamWriter.write(buffer.toByteArray());
    buffer.reset();
  }

  @Override
  protected void writeWALTrailer() throws IOException {
    ByteArrayOutputStream buffer = (ByteArrayOutputStream) this.output;
    buffer.reset();
    super.writeWALTrailer();
    appendOnlyStreamWriter.write(buffer.toByteArray());
    buffer.reset();
  }

  @VisibleForTesting
  public void force() throws IOException {
    this.appendOnlyStreamWriter.force(false);
  }
}
