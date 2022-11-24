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

import static org.apache.hadoop.hbase.wal.WALUtils.PB_WAL_MAGIC;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Arrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.distributedlog.shaded.AppendOnlyStreamReader;
import org.apache.distributedlog.shaded.api.DistributedLogManager;
import org.apache.distributedlog.shaded.exceptions.EndOfStreamException;
import org.apache.distributedlog.shaded.exceptions.LogEmptyException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.util.WALInputStream;
import org.apache.hadoop.hbase.wal.ServiceBasedReader;
import org.apache.hadoop.hbase.wal.WALUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Reader for DistributedLog based WAL.
 * Not Support WAL compression.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class DistributedLogReader extends AbstractProtobufLogReader implements ServiceBasedReader {
  private static final Log LOG = LogFactory.getLog(DistributedLogReader.class);

  private AppendOnlyStreamReader appendOnlyStreamReader;
  private DistributedLogManager distributedLogManager;
  private String logName;

  static {
    writerClsNames.add(DistributedLogWriter.class.getSimpleName());
  }

  @Override
  protected boolean checkRecoveredEdits() {
    return WALUtils.isRecoveredEdits(logName);
  }

  @Override
  protected String initReader(InputStream stream) throws IOException {
    this.inputStream = new DistributedLogInputStream(stream);
    long expectedPos = PB_WAL_MAGIC.length;
    this.inputStream.seek(expectedPos);

    if (this.inputStream.getPos() != expectedPos) {
      throw new IOException("The stream is at invalid position: " + this.inputStream.getPos());
    }
    // Initialize metadata or, when we reset, just skip the header.
    WALProtos.WALHeader.Builder builder = WALProtos.WALHeader.newBuilder();
    WALHdrContext hdrCtxt = readHeader(builder, stream);
    ProtobufLogReader.WALHdrResult walHdrRes = hdrCtxt.getResult();
    if (walHdrRes == ProtobufLogReader.WALHdrResult.EOF) {
      throw new EOFException("Couldn't read WAL PB header");
    }
    if (walHdrRes == ProtobufLogReader.WALHdrResult.UNKNOWN_WRITER_CLS) {
      throw new IOException("Got unknown writer class: " + builder.getWriterClsName());
    }
    WALProtos.WALHeader header = builder.build();
    this.hasCompression = header.hasHasCompression() && header.getHasCompression();
    this.hasTagCompression = header.hasHasTagCompression() && header.getHasTagCompression();
    this.walEditsStopOffset = this.distributedLogManager.getLastTxId();
    long currentPosition = this.inputStream.getPos();
    trailerPresent = setTrailerIfPresent();
    this.inputStream.seek(currentPosition);
    if (LOG.isTraceEnabled()) {
      LOG.trace("After reading the trailer: walEditsStopOffset: " + this.walEditsStopOffset
        + ", logLength: " + this.logLength + ", " + "trailerPresent: "
        + (trailerPresent ? "true, size: " + trailer.getSerializedSize() : "false")
        + ", currentPosition: " + currentPosition);
    }

    codecClsName = hdrCtxt.getCellCodecClsName();

    return codecClsName;
  }

  @Override
  protected IOException extractHiddenEof(Exception ex) {
    // There are two problems we are dealing with here. AppendOnlyLogReader throws generic exception
    // for EndOfStream, not EndOfStream; and scanner further hides it inside RuntimeException.
    IOException ioEx = null;
    if (ex instanceof EndOfStreamException) {
      return new EOFException(ex.getMessage());
    } else if (ex instanceof IOException) {
      ioEx = (IOException)ex;
    } else if (ex instanceof RuntimeException
      && ex.getCause() != null && ex.getCause() instanceof IOException) {
      ioEx = (IOException)ex.getCause();
    }
    if (ioEx != null) {
      if (ioEx.getMessage().contains("EndOfStream")) {
        return ioEx;
      }
      return null;
    }
    return null;
  }

  @Override
  protected String getLogName() {
    return this.logName;
  }

  @Override
  public void seek(long pos) throws IOException {
    inputStream.seek(pos);
  }

  @Override
  public long getPosition() throws IOException {
    return inputStream.getPos();
  }

  @Override
  public void reset() throws IOException {
    if (distributedLogManager != null) {
      this.appendOnlyStreamReader = distributedLogManager.getAppendOnlyStreamReader();
    }
  }

  @Override
  public void init(Configuration conf, String logName) throws URISyntaxException, IOException {
    this.logName = logName;
    try {
      this.distributedLogManager =
        DistributedLogAccessor.getInstance(conf).getNamespace().openLog(logName);
    } catch (Exception e) {
      LOG.warn("Failed to init distributed log reader. ");
      throw new IOException(e);
    }
    reset();
    byte[] magic = new byte[PB_WAL_MAGIC.length];
    boolean isPbWal = (appendOnlyStreamReader.read(magic) == magic.length)
      && Arrays.equals(magic, PB_WAL_MAGIC);
    if (!isPbWal) {
      throw new IOException(logName + " is not a protobuf wal log.");
    }
    this.logLength = distributedLogManager.getLastTxId();
    super.init(conf, appendOnlyStreamReader);
  }

  static class DistributedLogInputStream extends WALInputStream {
    private final AppendOnlyStreamReader reader;

    /**
     * Creates a DataInputStream that uses the specified
     * underlying InputStream.
     *
     * @param in the specified input stream
     */
    public DistributedLogInputStream(InputStream in) {
      super(in);
      if (! (in instanceof AppendOnlyStreamReader)) {
        throw new IllegalArgumentException("DistributedLogInputStream should be initialized with: "
          + AppendOnlyStreamReader.class + " but got: " + in.getClass());
      }
      this.reader = (AppendOnlyStreamReader) in;
    }

    @Override
    public long getPos() throws IOException {
      return reader.position();
    }

    @Override
    public void seek(long pos) throws IOException {
      reader.skipTo(pos);
    }

    @Override
    public int read() throws IOException {
      try {
        return super.read();
      } catch (IOException e) {
        if (e instanceof EndOfStreamException || e instanceof LogEmptyException) {
          return -1;
        } else {
          throw e;
        }
      }
    }
  }
}
