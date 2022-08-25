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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.distributedlog.shaded.AppendOnlyStreamWriter;
import org.apache.distributedlog.shaded.api.DistributedLogManager;
import org.apache.distributedlog.shaded.api.namespace.Namespace;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.wal.Entry;
import org.apache.hadoop.hbase.wal.ServiceBasedWriter;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Writer for DistributedLog based WAL.
 * Not Support WAL compression.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class DistributedLogWriter extends AbstractProtobufLogWriter implements ServiceBasedWriter {
  private static final Log LOG = LogFactory.getLog(DistributedLogWriter.class);

  private DistributedLogManager distributedLogManager;
  private AppendOnlyStreamWriter appendOnlyStreamWriter;
  private String logName;

  @Override
  public void init(Configuration conf, String logName) throws URISyntaxException, IOException {
    this.logName = logName;
    super.init(conf);
  }

  @Override
  public void sync() throws IOException {
    appendOnlyStreamWriter.force(false);
  }

  @Override
  public void append(Entry entry) throws IOException {
    // Not support WAL compression.
    entry.setCompressionContext(null);
    byte[] keyBytes = entry.getKey().getBuilder(null).
      setFollowingKvCount(entry.getEdit().size()).build().toByteArray();
    appendOnlyStreamWriter.write(keyBytes);
    for (Cell cell : entry.getEdit().getCells()) {
      // cellEncoder must assume little about the stream, since we write PB and cells in turn.
      cellEncoder.write(cell);
    }
  }

  @Override
  protected void initOutput() throws IOException {
    Namespace namespace;
    try {
      namespace = DistributedLogAccessor.getInstance(this.conf).getNamespace();
    } catch (Exception e) {
      LOG.error("Failed to get DistributedLog namespace.");
      throw new IOException(e);
    }
    if (namespace.logExists(logName)) {
      throw new IOException("Should not create multi writers to an existing DistributedLog. ");
    }

    distributedLogManager = namespace.openLog(logName);
    if (distributedLogManager == null) {
      throw new IllegalStateException("Failed to access DistributedLog. ");
    }
    appendOnlyStreamWriter = distributedLogManager.getAppendOnlyStreamWriter();
    output = new DataOutputStream(new DistributedLogOutputStream(appendOnlyStreamWriter));
  }

  @Override
  protected boolean checkRecoveredEdits() {
    return logName.contains(HConstants.RECOVERED_EDITS_DIR);
  }

  @Override
  protected String getURL() {
    return this.logName;
  }

  protected WALCellCodec getCodec(Configuration conf, CompressionContext compressionContext)
      throws IOException {
    return WALCellCodec.create(conf, null, compressionContext);
  }

  @Override
  public long getLength() throws IOException {
    return appendOnlyStreamWriter.position();
  }

  @Override
  public void close() throws IOException {
    if (appendOnlyStreamWriter != null) {
      appendOnlyStreamWriter.markEndOfStream();
      appendOnlyStreamWriter.close();
      appendOnlyStreamWriter = null;
    }
    super.close();
  }

  WALProtos.WALTrailer buildWALTrailer(WALProtos.WALTrailer.Builder builder) {
    return builder.build();
  }

  static class DistributedLogOutputStream extends OutputStream {
    private final AppendOnlyStreamWriter writer;

    DistributedLogOutputStream(AppendOnlyStreamWriter writer) {
      this.writer = writer;
    }

    @Override
    public void write(int b) throws IOException {
      try {
        writer.write(new byte[] { (byte) b });
      } catch (Exception e) {
        throw new IOException("Failed to write into DistributedLog output stream with "
          + "exception: \n", e);
      }
    }
  }
}
