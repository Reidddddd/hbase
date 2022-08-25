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

import static org.apache.hadoop.hbase.wal.WALUtils.DEFAULT_WAL_TRAILER_WARN_SIZE;
import static org.apache.hadoop.hbase.wal.WALUtils.PB_WAL_COMPLETE_MAGIC;
import static org.apache.hadoop.hbase.wal.WALUtils.PB_WAL_MAGIC;
import static org.apache.hadoop.hbase.wal.WALUtils.WAL_TRAILER_WARN_SIZE;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALHeader;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALTrailer;
import org.apache.hadoop.hbase.wal.Entry;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Writer for protobuf-based WAL.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public abstract class AbstractProtobufLogWriter extends WriterBase {
  private static final Log LOG = LogFactory.getLog(AbstractProtobufLogWriter.class);

  protected DataOutputStream output;
  protected Codec.Encoder cellEncoder;
  protected WALCellCodec.ByteStringCompressor compressor;
  protected boolean trailerWritten;
  protected WALTrailer trailer;
  // maximum size of the wal Trailer in bytes. If a user writes/reads a trailer with size larger
  // than this size, it is written/read respectively, with a WARN message in the log.
  protected int trailerWarnSize;

  public AbstractProtobufLogWriter() {
    super();
  }

  @Override
  public void init(Configuration conf) throws IOException {
    super.init(conf);
    assert this.output == null;
    isRecoveredEdits = checkRecoveredEdits();
    boolean doCompress = initializeCompressionContext(conf);
    this.trailerWarnSize = conf.getInt(WAL_TRAILER_WARN_SIZE, DEFAULT_WAL_TRAILER_WARN_SIZE);
    initOutput();
    output.write(PB_WAL_MAGIC);
    boolean doTagCompress = doCompress
      && conf.getBoolean(CompressionContext.ENABLE_WAL_TAGS_COMPRESSION, true);
    buildWALHeader(conf,
      WALProtos.WALHeader.newBuilder().setHasCompression(doCompress).setHasTagCompression(doTagCompress))
      .writeDelimitedTo(output);

    initAfterHeader(doCompress);

    // instantiate trailer to default value.
    trailer = WALProtos.WALTrailer.newBuilder().build();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Initialized protobuf WAL=" + getURL() + ", compression=" + doCompress);
    }
  }

  protected abstract void initOutput() throws IOException;

  protected abstract boolean checkRecoveredEdits();

  protected abstract String getURL();

  protected WALCellCodec getCodec(Configuration conf, CompressionContext compressionContext)
      throws IOException {
    return WALCellCodec.create(conf, null, compressionContext);
  }

  protected WALHeader buildWALHeader(Configuration conf, WALHeader.Builder builder)
      throws IOException {
    if (!builder.hasWriterClsName()) {
      builder.setWriterClsName(ProtobufLogWriter.class.getSimpleName());
    }
    if (!builder.hasCellCodecClsName()) {
      builder.setCellCodecClsName(WALCellCodec.getWALCellCodecClass(conf).getName());
    }
    return builder.build();
  }

  protected void initAfterHeader(boolean doCompress) throws IOException {
    WALCellCodec codec = getCodec(conf, this.compressionContext);
    this.cellEncoder = codec.getEncoder(this.output);
    if (doCompress) {
      this.compressor = codec.getByteStringCompressor();
    }
  }

  @Override
  public void append(Entry entry) throws IOException {
    entry.setCompressionContext(compressionContext);
    entry.getKey().getBuilder(compressor).
      setFollowingKvCount(entry.getEdit().size()).build().writeDelimitedTo(output);
    for (Cell cell : entry.getEdit().getCells()) {
      // cellEncoder must assume little about the stream, since we write PB and cells in turn.
      cellEncoder.write(cell);
    }
  }

  @Override
  public void close() throws IOException {
    if (this.output != null) {
      try {
        if (!trailerWritten) {
          writeWALTrailer();
        }
        this.output.close();
      } catch (NullPointerException npe) {
        // Can get a NPE coming up from down in DFSClient$DFSOutputStream#close
        LOG.warn(npe);
      }
      this.output = null;
    }
  }

  WALTrailer buildWALTrailer(WALTrailer.Builder builder) {
    return builder.build();
  }

  private void writeWALTrailer() {
    try {
      int trailerSize = 0;
      if (this.trailer == null) {
        // use default trailer.
        LOG.warn("WALTrailer is null. Continuing with default.");
        this.trailer = buildWALTrailer(WALTrailer.newBuilder());
        trailerSize = this.trailer.getSerializedSize();
      } else if (this.trailer.getSerializedSize() > this.trailerWarnSize) {
        trailerSize = this.trailer.getSerializedSize();
        // continue writing after warning the user.
        LOG.warn("Please investigate WALTrailer usage. Trailer size > maximum size : " +
          trailerSize + " > " + this.trailerWarnSize);
      }
      this.trailer.writeTo(output);
      output.writeInt(trailerSize);
      output.write(PB_WAL_COMPLETE_MAGIC);
      this.trailerWritten = true;
    } catch (IOException ioe) {
      LOG.warn("Failed to write trailer, non-fatal, continuing...", ioe);
    }
  }

  void setWALTrailer(WALTrailer walTrailer) {
    this.trailer = walTrailer;
  }
}
